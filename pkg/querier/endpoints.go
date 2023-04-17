package querier

import (
	"context"
	"github.com/cortexproject/cortex/pkg/cortexpb"
	"io"
	"time"

	"github.com/cortexproject/cortex/pkg/ring/client"
	"github.com/cortexproject/cortex/pkg/storegateway/storegatewaypb"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/util/stats"
	"github.com/thanos-community/promql-engine/api"
)

type TenantStoreGatewayEngines struct {
	finder      BlocksFinder
	stores      BlocksStoreSet
	clientsPool *client.Pool
}

func NewTenantStoreGatewayEngines(finder BlocksFinder, stores BlocksStoreSet, clientsPool *client.Pool) *TenantStoreGatewayEngines {
	return &TenantStoreGatewayEngines{
		finder:      finder,
		stores:      stores,
		clientsPool: clientsPool,
	}
}

func (m *TenantStoreGatewayEngines) Engines(userID string, _ string, start, end time.Time) []api.RemoteEngine {
	// TODO: add time check.
	knownBlocks, _, err := m.finder.GetBlocks(context.Background(), userID, start.UnixMilli(), end.UnixMilli())
	if err != nil {

	}
	requiredLen := len(knownBlocks.GetULIDs())
	res := make([]api.RemoteEngine, 0)
	blockLocations, err := m.stores.GetBlocksLocations(userID, knownBlocks.GetULIDs())
	for addr, blocks := range blockLocations {
		if len(blocks) == requiredLen {
			c, err := m.clientsPool.GetClientFor(addr)
			if err != nil {
				continue
			}
			res = append(res, &StoreGatewayRemoteEngine{
				Addr:     addr,
				BlockIDs: convertULIDToString(blocks),
				client:   c.(storegatewaypb.StoreGatewayClient),
			})
		}
	}
	return res
}

func convertULIDToString(ulids []ulid.ULID) []string {
	output := make([]string, 0, len(ulids))
	for _, u := range ulids {
		output = append(output, u.String())
	}
	return output
}

type StoreGatewayRemoteEngine struct {
	Addr     string
	BlockIDs []string
	client   storegatewaypb.StoreGatewayClient
}

func (e *StoreGatewayRemoteEngine) MaxT() int64 { return 0 }

func (e *StoreGatewayRemoteEngine) MinT() int64 { return 0 }

func (e *StoreGatewayRemoteEngine) LabelSets() []labels.Labels { return nil }

func (e *StoreGatewayRemoteEngine) NewRangeQuery(opts *promql.QueryOpts, qs string, start, end time.Time, step time.Duration) (promql.Query, error) {
	return &remoteQuery{
		query:         qs,
		start:         start,
		end:           end,
		step:          step,
		blockIDs:      e.BlockIDs,
		lookBackDelta: opts.LookbackDelta,
		client:        e.client,
	}, nil
}

type Opts struct {
	Timeout time.Duration
}

type remoteQuery struct {
	logger log.Logger
	client storegatewaypb.StoreGatewayClient
	opts   Opts

	query         string
	start         time.Time
	end           time.Time
	step          time.Duration
	lookBackDelta time.Duration

	blockIDs []string
	cancel   context.CancelFunc
}

func (r *remoteQuery) Exec(ctx context.Context) *promql.Result {
	start := time.Now()

	qctx, cancel := context.WithCancel(ctx)
	r.cancel = cancel
	defer cancel()

	request := &storegatewaypb.QueryRangeRequest{
		Query:                r.query,
		StartTimeSeconds:     r.start.Unix(),
		EndTimeSeconds:       r.end.Unix(),
		IntervalSeconds:      int64(r.step.Seconds()),
		TimeoutSeconds:       int64(r.opts.Timeout.Seconds()),
		LookbackDeltaSeconds: int64(r.lookBackDelta.Seconds()),
		BlockIDs:             r.blockIDs,
	}

	qry, err := r.client.QueryRange(qctx, request)
	if err != nil {
		return &promql.Result{Err: err}
	}

	result := make(promql.Matrix, 0)
	for {
		msg, err := qry.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return &promql.Result{Err: err}
		}

		if warn := msg.GetWarnings(); warn != "" {
			return &promql.Result{Err: errors.New(warn)}
		}

		ts := msg.GetTimeseries()
		if ts == nil {
			continue
		}
		series := promql.Series{
			Metric: cortexpb.FromLabelAdaptersToLabels(ts.Labels),
			Points: make([]promql.Point, 0, len(ts.Samples)),
		}
		for _, s := range ts.Samples {
			series.Points = append(series.Points, promql.Point{
				T: s.TimestampMs,
				V: s.Value,
			})
		}
		result = append(result, series)
	}
	level.Debug(r.logger).Log("Executed query", "query", r.query, "time", time.Since(start))

	return &promql.Result{Value: result}
}

func (r *remoteQuery) Close() {
	r.Cancel()
}

func (r *remoteQuery) Statement() parser.Statement {
	return nil
}

func (r *remoteQuery) Stats() *stats.Statistics {
	return nil
}

func (r *remoteQuery) Cancel() {
	if r.cancel != nil {
		r.cancel()
	}
}

func (r *remoteQuery) String() string {
	return r.query
}
