package querier

import (
	"github.com/thanos-community/promql-engine/api"
	"github.com/thanos-community/promql-engine/internal/prometheus/parser"
)

type DistributedExecutionOptimizer struct {
	Endpoints api.RemoteEndpoints
}

func (m DistributedExecutionOptimizer) Optimize(plan parser.Expr, opts *Opts) parser.Expr {

}
