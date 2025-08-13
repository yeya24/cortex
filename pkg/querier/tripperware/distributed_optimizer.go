package tripperware

import (
	"github.com/cortexproject/cortex/pkg/engine/distributed_execution"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"
)

// This is a simplified implementation.
// Future versions of the distributed optimizer are expected to:
// - Support more complex query patterns.
// - Incorporate diverse optimization strategies.
// - Extend support to node types beyond binary operations.

type DistributedOptimizer struct{}

func (d *DistributedOptimizer) Optimize(root logicalplan.Node, opts *query.Options) (logicalplan.Node, annotations.Annotations) {
	warns := annotations.New()

	logicalplan.TraverseBottomUp(nil, &root, func(parent, current *logicalplan.Node) bool {

		if (*current).Type() == logicalplan.BinaryNode {
			ch := (*current).Children()

			// TODO: add more checks so it's not 1+1
			// meed to be vector for lhs and rhs

			for _, child := range ch {
				temp := (*child).Clone()
				*child = distributed_execution.NewRemoteNode()
				*(*child).Children()[0] = temp
			}
		}

		return false
	})
	return root, *warns
}
