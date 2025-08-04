package tripperware

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/promql-engine/logicalplan"
	"testing"
	"time"
)

func TestDistributedOptimizer(t *testing.T) {
	testCases := []struct {
		name     string
		query    string
		start    int64
		end      int64
		step     time.Duration
		expected struct {
			childrenCount   int
			remoteExecCount int // the value of remote nodes depends on how the distributed optimizer is designed
			remoteExec      bool
			result          string
		}
	}{
		{
			name:  "binary operation with aggregations",
			query: "sum(rate(node_cpu_seconds_total{mode!=\"idle\"}[5m])) + sum(rate(node_memory_Active_bytes[5m]))",
			start: 100000,
			end:   100000,
			step:  time.Minute,
			expected: struct {
				childrenCount   int
				remoteExecCount int
				remoteExec      bool
				result          string
			}{
				remoteExecCount: 2,
				remoteExec:      true,
				result:          "dedup(remote(sum(rate(node_cpu_seconds_total{mode!=\"idle\"}[5m]))) [1970-01-01 00:01:40 +0000 UTC, 1970-01-01 00:01:40 +0000 UTC]) + dedup(remote(sum(rate(node_memory_Active_bytes[5m]))) [1970-01-01 00:01:40 +0000 UTC, 1970-01-01 00:01:40 +0000 UTC])",
			},
		},
		{
			name:  "binary operation with aggregations",
			query: "sum(rate(node_cpu_seconds_total{mode!=\"idle\"}[5m])) + sum(rate(node_memory_Active_bytes[5m]))",
			start: 100000,
			end:   100000,
			step:  time.Minute,
			expected: struct {
				childrenCount   int
				remoteExecCount int
				remoteExec      bool
				result          string
			}{
				remoteExecCount: 2,
				remoteExec:      true,
				result:          "dedup(remote(sum(rate(node_cpu_seconds_total{mode!=\"idle\"}[5m]))) [1970-01-01 00:01:40 +0000 UTC, 1970-01-01 00:01:40 +0000 UTC]) + dedup(remote(sum(rate(node_memory_Active_bytes[5m]))) [1970-01-01 00:01:40 +0000 UTC, 1970-01-01 00:01:40 +0000 UTC])",
			},
		},
		{
			name:  "multiple binary operations with aggregations",
			query: "sum(rate(http_requests_total{job=\"api\"}[5m])) + sum(rate(http_requests_total{job=\"web\"}[5m])) - sum(rate(http_requests_total{job=\"cache\"}[5m]))",
			start: 100000,
			end:   100000,
			step:  time.Minute,
			expected: struct {
				childrenCount   int
				remoteExecCount int
				remoteExec      bool
				result          string
			}{
				remoteExecCount: 4,
				remoteExec:      true,
				result:          "dedup(remote(dedup(remote(sum(rate(http_requests_total{job=\"api\"}[5m]))) [1970-01-01 00:01:40 +0000 UTC, 1970-01-01 00:01:40 +0000 UTC]) + dedup(remote(sum(rate(http_requests_total{job=\"web\"}[5m]))) [1970-01-01 00:01:40 +0000 UTC, 1970-01-01 00:01:40 +0000 UTC])) [1970-01-01 00:01:40 +0000 UTC, 1970-01-01 00:01:40 +0000 UTC]) - dedup(remote(sum(rate(http_requests_total{job=\"cache\"}[5m]))) [1970-01-01 00:01:40 +0000 UTC, 1970-01-01 00:01:40 +0000 UTC])",
			},
		},
		{
			name:  "aggregation with label replacement",
			query: "sum(rate(container_cpu_usage_seconds_total[1m])) by (pod) + sum(rate(container_memory_usage_bytes[1m])) by (pod)",
			start: 100000,
			end:   100000,
			step:  time.Minute,
			expected: struct {
				childrenCount   int
				remoteExecCount int
				remoteExec      bool
				result          string
			}{
				remoteExecCount: 2,
				remoteExec:      true,
				result:          "dedup(remote(sum by (pod) (rate(container_cpu_usage_seconds_total[1m]))) [1970-01-01 00:01:40 +0000 UTC, 1970-01-01 00:01:40 +0000 UTC]) + dedup(remote(sum by (pod) (rate(container_memory_usage_bytes[1m]))) [1970-01-01 00:01:40 +0000 UTC, 1970-01-01 00:01:40 +0000 UTC])",
			},
		},
		{
			name:  "subquery with aggregation",
			query: "sum(rate(container_network_transmit_bytes_total[5m:1m]))",
			start: 100000,
			end:   100000,
			step:  time.Minute,
			expected: struct {
				childrenCount   int
				remoteExecCount int
				remoteExec      bool
				result          string
			}{
				remoteExecCount: 0,
				remoteExec:      true,
				result:          "sum(rate(container_network_transmit_bytes_total[5m:1m]))",
			},
		},
		{
			name:  "avg over vector with offset",
			query: "avg(rate(node_disk_reads_completed_total[5m] offset 1h))",
			start: 100000,
			end:   100000,
			step:  time.Minute,
			expected: struct {
				childrenCount   int
				remoteExecCount int
				remoteExec      bool
				result          string
			}{
				remoteExecCount: 0,
				remoteExec:      true,
				result:          "avg(rate(node_disk_reads_completed_total[5m] offset 1h))",
			},
		},
		{
			name:  "function applied on binary operation",
			query: "rate(http_requests_total[5m]) + rate(http_errors_total[5m]) > bool 0",
			start: 100000,
			end:   100000,
			step:  time.Minute,
			expected: struct {
				childrenCount   int
				remoteExecCount int
				remoteExec      bool
				result          string
			}{
				remoteExecCount: 4,
				remoteExec:      true,
				result:          "dedup(remote(dedup(remote(rate(http_requests_total[5m])) [1970-01-01 00:01:40 +0000 UTC, 1970-01-01 00:01:40 +0000 UTC]) + dedup(remote(rate(http_errors_total[5m])) [1970-01-01 00:01:40 +0000 UTC, 1970-01-01 00:01:40 +0000 UTC])) [1970-01-01 00:01:40 +0000 UTC, 1970-01-01 00:01:40 +0000 UTC]) > bool dedup(remote(0) [1970-01-01 00:01:40 +0000 UTC, 1970-01-01 00:01:40 +0000 UTC])",
			},
		},
		{
			name:  "aggregation without binary, single child",
			query: "sum(rate(process_cpu_seconds_total[5m]))",
			start: 100000,
			end:   100000,
			step:  time.Minute,
			expected: struct {
				childrenCount   int
				remoteExecCount int
				remoteExec      bool
				result          string
			}{
				remoteExecCount: 0,
				remoteExec:      true,
				result:          "sum(rate(process_cpu_seconds_total[5m]))",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := &PrometheusRequest{
				Start: tc.start,
				End:   tc.end,
				Query: tc.query,
			}

			middleware := DistributedQueryMiddleware(tc.step, 5*time.Minute)
			handler := middleware.Wrap(HandlerFunc(func(_ context.Context, req Request) (Response, error) {
				return nil, nil
			}))

			_, err := handler.Do(context.Background(), req)
			require.NoError(t, err)
			require.NotNil(t, req.LogicalPlan, "logical plan should be populated")

			root := req.LogicalPlan.Root()
			remoteNodeCount := 0
			logicalplan.TraverseBottomUp(nil, &root, func(parent, current *logicalplan.Node) bool {
				if logicalplan.RemoteExecutionNode == (*current).Type() {
					remoteNodeCount++
				}
				return false
			})
			require.Equal(t, tc.expected.remoteExecCount, remoteNodeCount)
		})
	}
}
