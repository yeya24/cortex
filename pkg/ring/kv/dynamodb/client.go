package dynamodb

import (
	"context"
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/cortexproject/cortex/pkg/ring/kv/codec"
	"github.com/cortexproject/cortex/pkg/util/backoff"
)

const (
	maxCasRetries = 10 // max retries in CAS operation
)

// Config to create a ConsulClient
type Config struct {
	Region         string        `yaml:"region"`
	TableName      string        `yaml:"table_name"`
	TTL            time.Duration `yaml:"ttl"`
	PullerSyncTime time.Duration `yaml:"puller_sync_time"`
	MaxCasRetries  int           `yaml:"max_cas_retries"`
	Timeout        time.Duration `yaml:"timeout"`
}

type Client struct {
	kv             dynamoDbClient
	codec          codec.Codec
	ddbMetrics     *dynamodbMetrics
	logger         log.Logger
	pullerSyncTime time.Duration
	backoffConfig  backoff.Config

	staleDataLock sync.RWMutex
	staleData     map[staleDataKey]staleData
}

type staleDataKey struct {
	primaryKey   string
	secondaryKey string
}

type staleData struct {
	data      codec.MultiKey
	timestamp time.Time
}

// RegisterFlags adds the flags required to config this to the given FlagSet
// If prefix is not an empty string it should end with a period.
func (cfg *Config) RegisterFlags(f *flag.FlagSet, prefix string) {
	f.StringVar(&cfg.Region, prefix+"dynamodb.region", "", "Region to access dynamodb.")
	f.StringVar(&cfg.TableName, prefix+"dynamodb.table-name", "", "Table name to use on dynamodb.")
	f.DurationVar(&cfg.TTL, prefix+"dynamodb.ttl-time", 0, "Time to expire items on dynamodb.")
	f.DurationVar(&cfg.PullerSyncTime, prefix+"dynamodb.puller-sync-time", 60*time.Second, "Time to refresh local ring with information on dynamodb.")
	f.IntVar(&cfg.MaxCasRetries, prefix+"dynamodb.max-cas-retries", maxCasRetries, "Maximum number of retries for DDB KV CAS.")
	f.DurationVar(&cfg.Timeout, prefix+"dynamodb.timeout", 2*time.Minute, "Timeout of dynamoDbClient requests. Default is 2m.")
}

func NewClient(cfg Config, cc codec.Codec, logger log.Logger, registerer prometheus.Registerer) (*Client, error) {
	dynamoDB, err := newDynamodbKV(cfg, logger)
	if err != nil {
		return nil, err
	}

	ddbMetrics := newDynamoDbMetrics(registerer)

	backoffConfig := backoff.Config{
		MinBackoff: 1 * time.Second,
		MaxBackoff: cfg.PullerSyncTime,
		MaxRetries: cfg.MaxCasRetries,
	}

	var kv dynamoDbClient
	kv = dynamodbInstrumentation{kv: dynamoDB, ddbMetrics: ddbMetrics}
	if cfg.Timeout > 0 {
		kv = newDynamodbKVWithTimeout(kv, cfg.Timeout)
	}
	c := &Client{
		kv:             kv,
		codec:          cc,
		logger:         ddbLog(logger),
		ddbMetrics:     ddbMetrics,
		pullerSyncTime: cfg.PullerSyncTime,
		staleData:      make(map[staleDataKey]staleData),
		backoffConfig:  backoffConfig,
	}
	level.Info(c.logger).Log("msg", "dynamodb kv initialized")
	return c, nil
}

func (c *Client) List(ctx context.Context, key string) ([]string, error) {
	resp, _, err := c.kv.List(ctx, dynamodbKey{primaryKey: key})
	if err != nil {
		level.Warn(c.logger).Log("msg", "error List", "key", key, "err", err)
		return nil, err
	}
	return resp, err
}

// Get reads the key. When hint.SecondaryKey is set, only that item (sort key) is read; otherwise
// the full key (all items under the partition key) is read.
func (c *Client) Get(ctx context.Context, key string, hint *codec.CASHint) (any, error) {
	ddbKey := dynamodbKey{primaryKey: key}
	if hint != nil && hint.SecondaryKey != "" {
		ddbKey.sortKey = hint.SecondaryKey
	}
	resp, _, err := c.kv.Query(ctx, ddbKey, false)
	if err != nil {
		level.Warn(c.logger).Log("msg", "error Get", "key", key, "err", err)
		return nil, err
	}
	res, err := c.decodeMultikey(resp)
	if err != nil {
		return nil, err
	}
	secondaryKey := ""
	if hint != nil && hint.SecondaryKey != "" {
		secondaryKey = hint.SecondaryKey
	}
	c.updateStaleData(key, secondaryKey, res, time.Now().UTC())

	return res, nil
}

func (c *Client) Delete(ctx context.Context, key string) error {
	resp, _, err := c.kv.Query(ctx, dynamodbKey{primaryKey: key}, false)
	if err != nil {
		level.Warn(c.logger).Log("msg", "error Delete", "key", key, "err", err)
		return err
	}

	for innerKey := range resp {
		err := c.kv.Delete(ctx, dynamodbKey{
			primaryKey: key,
			sortKey:    innerKey,
		})
		if err != nil {
			level.Warn(c.logger).Log("msg", "error Delete", "key", key, "innerKey", innerKey, "err", err)
			return err
		}
	}
	c.deleteStaleData(key)

	return err
}

func (c *Client) CAS(ctx context.Context, key string, f func(in any) (out any, retry bool, err error), hint *codec.CASHint) error {
	// When hint.SecondaryKey is set, the caller indicates they are fine with a partial value: we
	// query only that instance's item (sort key) and pass it to the callback. No cache is used.
	// Whether to pass a hint is the caller's choice; the storage layer may return only that sub-key.
	bo := backoff.New(ctx, c.backoffConfig)
	for bo.Ongoing() {
		c.ddbMetrics.dynamodbCasAttempts.Inc()

		var resp map[string]dynamodbItem
		var current codec.MultiKey
		usedHint := false

		if hint != nil && hint.SecondaryKey != "" {
			// Query only this instance's item (sort key); callback receives partial (0 or 1 instance).
			var err error
			resp, _, err = c.kv.Query(ctx, dynamodbKey{primaryKey: key, sortKey: hint.SecondaryKey}, false)
			if err != nil {
				level.Error(c.logger).Log("msg", "error cas query (hint)", "key", key, "sortKey", hint.SecondaryKey, "err", err)
				bo.Wait()
				continue
			}
			current, err = c.decodeMultikey(resp)
			if err != nil {
				level.Error(c.logger).Log("msg", "error decoding key (hint)", "key", key, "err", err)
				continue
			}
			usedHint = true
		} else {
			// Full ring query.
			var err error
			resp, _, err = c.kv.Query(ctx, dynamodbKey{primaryKey: key}, false)
			if err != nil {
				level.Error(c.logger).Log("msg", "error cas query", "key", key, "err", err)
				bo.Wait()
				continue
			}
			current, err = c.decodeMultikey(resp)
			if err != nil {
				level.Error(c.logger).Log("msg", "error decoding key", "key", key, "err", err)
				continue
			}
		}

		out, retry, err := f(current.Clone())
		if err != nil {
			if !retry {
				return err
			}
			bo.Wait()
			continue
		}

		// Callback returning nil means it doesn't want to CAS anymore.
		if out == nil {
			return nil
		}
		// Don't even try

		r, ok := out.(codec.MultiKey)
		if !ok || r == nil {
			return fmt.Errorf("invalid type: %T, expected MultiKey", out)
		}

		toUpdate, toDelete, err := current.FindDifference(r)

		if err != nil {
			level.Error(c.logger).Log("msg", "error getting key", "key", key, "err", err)
			continue
		}

		buf, err := c.codec.EncodeMultiKey(toUpdate)
		if err != nil {
			level.Error(c.logger).Log("msg", "error serialising value", "key", key, "err", err)
			continue
		}

		putRequests := map[dynamodbKey]dynamodbItem{}
		for childKey, bytes := range buf {
			if usedHint && childKey != hint.SecondaryKey {
				continue
			}
			version := int64(0)
			if ddbItem, ok := resp[childKey]; ok {
				version = ddbItem.version
			}
			putRequests[dynamodbKey{primaryKey: key, sortKey: childKey}] = dynamodbItem{
				data:    bytes,
				version: version,
			}
		}

		deleteRequests := make([]dynamodbKey, 0, len(toDelete))
		for _, childKey := range toDelete {
			if usedHint && childKey != hint.SecondaryKey {
				continue
			}
			deleteRequests = append(deleteRequests, dynamodbKey{primaryKey: key, sortKey: childKey})
		}

		if len(putRequests) > 0 || len(deleteRequests) > 0 {
			retry, err := c.kv.Batch(ctx, putRequests, deleteRequests)
			if err != nil {
				if !retry {
					return err
				}
				bo.Wait()
				continue
			}
			secondaryKey := ""
			if usedHint && hint != nil {
				secondaryKey = hint.SecondaryKey
			}
			c.updateStaleData(key, secondaryKey, r, time.Now().UTC())
			return nil
		}

		if len(putRequests) == 0 && len(deleteRequests) == 0 {
			// no change detected, retry. FindDifference only adds an instance to toUpdate when the
			// desired timestamp is greater than stored, or same timestamp with LEFT/structural change.
			// So this can happen if: (1) desired timestamp <= stored (e.g. clock skew, or pre-write
			// used a timestamp that is not older than ingester's time.Now()), or (2) descriptors are equal.
			level.Warn(c.logger).Log("msg", "no change detected in ring, retry CAS",
				"hint", "If scaling from STAGING: ensure pre-written entry timestamp is strictly older than ingester startup (e.g. use past epoch or synced clocks).")
			bo.Wait()
			continue
		}

		return nil
	}
	err := fmt.Errorf("failed to CAS %s", key)
	level.Error(c.logger).Log("msg", "failed to CAS after retries", "key", key)
	return err
}

// WatchKey polls the full key every pullerSyncTime. Each poll is a full Query (all items under the
// partition key), so total read capacity is dominated by (number of Ring clients × full ring size / pullerSyncTime).
func (c *Client) WatchKey(ctx context.Context, key string, f func(any) bool) {
	watchBackoffConfig := c.backoffConfig
	watchBackoffConfig.MaxRetries = 0
	bo := backoff.New(ctx, watchBackoffConfig)

	for bo.Ongoing() {
		out, _, err := c.kv.Query(ctx, dynamodbKey{
			primaryKey: key,
		}, false)
		if err != nil {
			level.Error(c.logger).Log("msg", "error WatchKey", "key", key, "err", err)

			if bo.NumRetries() >= 10 {
				level.Error(c.logger).Log("msg", "failed to WatchKey after retries", "key", key, "err", err)
				// WatchKey is always full-key; use secondaryKey "" for stale fallback.
				if stale := c.getStaleData(key, ""); stale != nil {
					if !f(stale) {
						return
					}
				}
			}
			bo.Wait()
			continue
		}

		decoded, err := c.decodeMultikey(out)
		if err != nil {
			level.Error(c.logger).Log("msg", "error decoding key", "key", key, "err", err)
			continue
		}
		// WatchKey always reads full key; cache under secondaryKey "".
		c.updateStaleData(key, "", decoded, time.Now().UTC())

		if !f(decoded) {
			return
		}

		bo.Reset()
		select {
		case <-ctx.Done():
			return
		case <-time.After(c.pullerSyncTime):
		}
	}
}

func (c *Client) WatchPrefix(ctx context.Context, prefix string, f func(string, any) bool) {
	watchBackoffConfig := c.backoffConfig
	watchBackoffConfig.MaxRetries = 0
	bo := backoff.New(ctx, watchBackoffConfig)

	for bo.Ongoing() {
		out, _, err := c.kv.Query(ctx, dynamodbKey{
			primaryKey: prefix,
		}, true)
		if err != nil {
			level.Error(c.logger).Log("msg", "WatchPrefix", "prefix", prefix, "err", err)
			bo.Wait()
			continue
		}

		for key, ddbItem := range out {
			decoded, err := c.codec.Decode(ddbItem.data)
			if err != nil {
				level.Error(c.logger).Log("msg", "error decoding key", "key", key, "err", err)
				continue
			}
			if !f(key, decoded) {
				return
			}
		}

		bo.Reset()
		select {
		case <-ctx.Done():
			return
		case <-time.After(c.pullerSyncTime):
		}
	}
}

func (c *Client) decodeMultikey(data map[string]dynamodbItem) (codec.MultiKey, error) {
	multiKeyData := make(map[string][]byte, len(data))
	for key, ddbItem := range data {
		multiKeyData[key] = ddbItem.data
	}
	res, err := c.codec.DecodeMultiKey(multiKeyData)
	if err != nil {
		return nil, err
	}
	out, ok := res.(codec.MultiKey)
	if !ok || out == nil {
		return nil, fmt.Errorf("invalid type: %T, expected MultiKey", out)
	}

	return out, nil
}

func (c *Client) LastUpdateTime(key string) time.Time {
	// Ring uses this for the full key; look up full-key cache entry (secondaryKey "").
	return c.lastUpdateTime(key, "")
}

func (c *Client) lastUpdateTime(primaryKey, secondaryKey string) time.Time {
	c.staleDataLock.RLock()
	defer c.staleDataLock.RUnlock()

	data, ok := c.staleData[staleDataKey{primaryKey: primaryKey, secondaryKey: secondaryKey}]
	if !ok {
		return time.Time{}
	}

	return data.timestamp
}

func (c *Client) updateStaleData(primaryKey, secondaryKey string, data codec.MultiKey, timestamp time.Time) {
	c.staleDataLock.Lock()
	defer c.staleDataLock.Unlock()

	c.staleData[staleDataKey{primaryKey: primaryKey, secondaryKey: secondaryKey}] = staleData{
		data:      data,
		timestamp: timestamp,
	}
}

func (c *Client) getStaleData(primaryKey, secondaryKey string) codec.MultiKey {
	c.staleDataLock.RLock()
	defer c.staleDataLock.RUnlock()

	data, ok := c.staleData[staleDataKey{primaryKey: primaryKey, secondaryKey: secondaryKey}]
	if !ok {
		return nil
	}

	newD := data.data.Clone().(codec.MultiKey)

	return newD
}

// deleteStaleData removes all cached stale data for the given primary key (full and any secondaryKey).
func (c *Client) deleteStaleData(primaryKey string) {
	c.staleDataLock.Lock()
	defer c.staleDataLock.Unlock()

	for k := range c.staleData {
		if k.primaryKey == primaryKey {
			delete(c.staleData, k)
		}
	}
}

func ddbLog(logger log.Logger) log.Logger {
	return log.WithPrefix(logger, "class", "DynamodbKvClient")
}
