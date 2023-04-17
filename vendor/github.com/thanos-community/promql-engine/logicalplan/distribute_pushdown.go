package logicalplan

import (
	"github.com/thanos-community/promql-engine/api"
	"github.com/thanos-community/promql-engine/internal/prometheus/parser"
)

type DistributedPushDownOptimizer struct {
	Endpoints api.RemoteEndpoints
}

func (m DistributedPushDownOptimizer) Optimize(plan parser.Expr, opts *Opts) parser.Expr {
	engines := m.Endpoints.Engines(opts.UserID, opts.Query, opts.Start, opts.End, opts.LookbackDelta)
	if len(engines) == 1 {
		return RemoteExecution{
			Engine:          engines[0],
			Query:           plan.String(),
			QueryRangeStart: opts.Start,
		}
	}
	return plan
}
