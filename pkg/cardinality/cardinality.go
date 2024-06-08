package cardinalit

import (
	"context"
	"encoding/json"
	"golang.org/x/sync/errgroup"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"
)

const (
	workers = 8
)

type CardinalityExplorer struct {
	services.Service

	cfg               Config
	bucketClient      objstore.InstrumentedBucket
	queryBucketClient objstore.InstrumentedBucket
	logger            log.Logger
	reg               prometheus.Registerer
	cfgProvider       bucket.TenantConfigProvider

	lastSyncedFiles         []string
	cardinalityOverallMtx   sync.Mutex
	metricCardinalityMtx    sync.Mutex
	cardinalityOverall      []*TSDBStatusWithKey
	metricNameCardinalities []*MetricNameCardinalitiesWithKey
}

type TSDBStatusWithKey struct {
	*TSDBStatus
	key string
}

type MetricNameCardinalitiesWithKey struct {
	cardinalities *MetricNameCardinalities
	key           string
}

type Config struct {
	EnabledTenant string        `yaml:"enabled_tenant"`
	SyncInterval  time.Duration `yaml:"sync_interval"`
}

func NewCardinalityExplorer(cfg Config, bucketCfg bucket.Config, cfgProvider bucket.TenantConfigProvider, log log.Logger, reg prometheus.Registerer) (*CardinalityExplorer, error) {
	c := &CardinalityExplorer{
		cfg:                     cfg,
		logger:                  log,
		reg:                     reg,
		cfgProvider:             cfgProvider,
		cardinalityOverall:      make([]*TSDBStatusWithKey, 0),
		metricNameCardinalities: make([]*MetricNameCardinalitiesWithKey, 0),
	}
	var err error
	c.bucketClient, err = bucket.NewClient(context.Background(), bucketCfg, "cardinality", c.logger, c.reg)
	if err != nil {
		return nil, err
	}
	c.bucketClient = bucket.NewPrefixedBucketClient(c.bucketClient, "cardinality")

	c.Service = services.NewBasicService(c.starting, c.running, c.stopping)
	return c, nil
}

func (c *CardinalityExplorer) starting(ctx context.Context) error {
	return c.syncCardinalityFiles(ctx)
}

func (c *CardinalityExplorer) running(ctx context.Context) error {
	// Apply a jitter to the sync frequency in order to increase the probability
	// of hitting the shared cache (if any).
	syncTicker := time.NewTicker(util.DurationWithJitter(c.cfg.SyncInterval, 0.2))
	defer syncTicker.Stop()

	for {
		select {
		case <-syncTicker.C:
			if err := c.syncCardinalityFiles(ctx); err != nil {
				return err
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func (c *CardinalityExplorer) stopping(_ error) error {
	return nil
}

func (c *CardinalityExplorer) syncCardinalityFiles(ctx context.Context) error {
	bkt := bucket.NewUserBucketClient(c.cfg.EnabledTenant, c.bucketClient, c.cfgProvider)
	lastSyncedSet := make(map[string]struct{})
	for _, f := range c.lastSyncedFiles {
		lastSyncedSet[f] = struct{}{}
	}

	synced := make([]string, 0)
	type key struct {
		path    string
		dateStr string
		file    string
	}
	var eg errgroup.Group
	fileChan := make(chan *key)

	for i := 0; i < workers; i++ {
		eg.Go(func() error {
			for p := range fileChan {
				reader, err := bkt.Get(ctx, p.path)
				if err != nil {
					return err
				}

				obj, err := io.ReadAll(reader)
				if err != nil {
					return err
				}

				if strings.Contains(p.file, "metric-cardinalities") {
					var m MetricNameCardinalities
					if err = json.Unmarshal(obj, &m); err != nil {
						return err
					}

					c.metricCardinalityMtx.Lock()
					idx := -1
					for j, x := range c.metricNameCardinalities {
						if x.key == p.dateStr {
							idx = j
							break
						}
					}
					if idx == -1 {
						c.metricNameCardinalities = append(c.metricNameCardinalities, &MetricNameCardinalitiesWithKey{
							key:           p.dateStr,
							cardinalities: &m,
						})
					} else {
						c.metricNameCardinalities[idx].cardinalities.merge(m)
					}
					c.metricCardinalityMtx.Unlock()

				} else if strings.Contains(p.file, "overall") {
					var m TSDBStatus
					if err = json.Unmarshal(obj, &m); err != nil {
						return err
					}

					c.cardinalityOverallMtx.Lock()
					idx := -1
					for j, x := range c.cardinalityOverall {
						if x.key == p.dateStr {
							idx = j
							break
						}
					}
					if idx == -1 {
						c.cardinalityOverall = append(c.cardinalityOverall, &TSDBStatusWithKey{
							key:        p.dateStr,
							TSDBStatus: &m,
						})
					} else {
						c.cardinalityOverall[idx].TSDBStatus.merge(m)
					}
					c.cardinalityOverallMtx.Unlock()
				}
			}
			return nil
		})
	}

	if err := bkt.Iter(ctx, "", func(s string) error {
		if _, ok := lastSyncedSet[s]; ok {
			return nil
		}

		// Check date str

		synced = append(synced, s)
		parts := strings.Split(s, "/")
		dateStr, file := parts[0], parts[len(parts)-1]

		select {
		case <-ctx.Done():
		case fileChan <- &key{path: s, file: file, dateStr: dateStr}:
		}
		return nil
	}); err != nil {
		return err
	}

	close(fileChan)
	if err := eg.Wait(); err != nil {
		return err
	}

	c.lastSyncedFiles = synced
	return nil
}
