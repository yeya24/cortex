package querier

import (
	"context"
	"github.com/cortexproject/cortex/pkg/storegateway/storegatewaypb"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/thanos-community/promql-engine/api"
	"time"
)

type TenantStoreGatewayEngines struct {
	userID string
	finder BlocksFinder
	stores BlocksStoreSet
}

func (m TenantStoreGatewayEngines) Engines(_ string, start, end time.Time) []api.RemoteEngine {
	knownBlocks, _, err := m.finder.GetBlocks(context.Background(), m.userID, start.UnixMilli(), end.UnixMilli())
	if err != nil {

	}
	requiredLen := len(knownBlocks.GetULIDs())
	res := make([]api.RemoteEngine, 0)
	blockLocations, err := m.stores.GetBlocksLocations(m.userID, knownBlocks.GetULIDs())
	for addr, blocks := range blockLocations {
		if len(blocks) == requiredLen {
			res = append(res, &StoreGatewayRemoteEngine{
				Addr:     addr,
				BlockIDs: blocks,
			})
		}
	}
	return res
}

type StoreGatewayRemoteEngine struct {
	Addr     string
	BlockIDs []string
}

func (e *StoreGatewayRemoteEngine) MaxT() int64 { return 0 }

func (e *StoreGatewayRemoteEngine) MinT() int64 { return 0 }

func (e *StoreGatewayRemoteEngine) LabelSets() []labels.Labels { return nil }

func (e *StoreGatewayRemoteEngine) NewRangeQuery(opts *promql.QueryOpts, qs string, start, end time.Time, interval time.Duration) (promql.Query, error) {
	c := storegatewaypb.NewStoreGatewayClient()
	c.QueryRange(context.Background(), &storegatewaypb.QueryRangeRequest{
		Query:                qs,
		StartTimeSeconds:     start.Unix(),
		EndTimeSeconds:       end.Unix(),
		IntervalSeconds:      int64(interval) / int64(time.Second),
		LookbackDeltaSeconds: int64(opts.LookbackDelta) / int64(time.Second),
		BlockIDs:             []string{},
	})
}
