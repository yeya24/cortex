package fragmenter

import (
	"github.com/cortexproject/cortex/pkg/querier/tripperware"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSchedulerCoordination(t *testing.T) {
	table := NewFragmentTable()
	table.AddMapping(uint64(0), uint64(1), "localhost:8000")
	table.AddMapping(uint64(0), uint64(2), "localhost:8001")

	result, exist := table.GetMapping(uint64(0), []uint64{1, 2})
	require.True(t, exist)
	require.Equal(t, []string{"localhost:8000", "localhost:8001"}, result)

	result, exist = table.GetMapping(uint64(0), []uint64{1, 3})
	require.False(t, exist)
	require.Empty(t, result)

	result, exist = table.GetMapping(uint64(0), []uint64{1})
	require.True(t, exist)
	require.Equal(t, []string{"localhost:8000"}, result)

	table.ClearMappings(uint64(0))
	result, exist = table.GetMapping(uint64(0), []uint64{1})
	require.False(t, exist)
	require.Empty(t, result)
}

func TestFragmenter(t *testing.T) {
	lp := createTestLogicalPlan(t, time.Now(), time.Now(), 0, "sum(rate(node_cpu_seconds_total{mode!=\"idle\"}[5m])) + sum(rate(node_memory_Active_bytes[5m]))")
	res, err := FragmentLogicalPlanNode(lp.Root())
	require.NoError(t, err)
	require.Equal(t, 3, len(res))

	lp2 := createTestLogicalPlan(t, time.Now(), time.Now(), 0, "sum(rate(http_requests_total{job=\"api\"}[5m])) + sum(rate(http_requests_total{job=\"web\"}[5m])) - sum(rate(http_requests_total{job=\"cache\"}[5m]))")
	res2, err2 := FragmentLogicalPlanNode(lp2.Root())
	require.NoError(t, err2)
	require.Equal(t, 5, len(res2))
}

func createTestLogicalPlan(t *testing.T, startTime time.Time, endTime time.Time, step time.Duration, q string) logicalplan.Plan {
	qOpts := query.Options{
		Start:              startTime,
		End:                startTime,
		Step:               0,
		StepsBatch:         10,
		LookbackDelta:      0,
		EnablePerStepStats: false,
	}

	if step != 0 {
		qOpts.End = endTime
		qOpts.Step = step
	}

	expr, err := parser.NewParser(q, parser.WithFunctions(parser.Functions)).ParseExpr()
	require.NoError(t, err)

	planOpts := logicalplan.PlanOptions{
		DisableDuplicateLabelCheck: false,
	}

	logicalPlan := logicalplan.NewFromAST(expr, &qOpts, planOpts)
	optimizedPlan, _ := logicalPlan.Optimize(logicalplan.DefaultOptimizers)
	dOptimizer := tripperware.DistributedOptimizer{}
	dOptimizedPlanNode, _ := dOptimizer.Optimize(optimizedPlan.Root(), &qOpts)
	lp := logicalplan.New(dOptimizedPlanNode, &qOpts, planOpts)

	return lp
}
