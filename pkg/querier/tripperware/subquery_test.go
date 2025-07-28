package tripperware

import (
	"fmt"
	"github.com/thanos-io/thanos/pkg/querysharding"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
)

func TestSubQueryStepSizeCheck(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		query       string
		defaultStep time.Duration
		err         error
		maxStep     int64
	}{
		{
			name:  "invalid query",
			query: "sum(up",
		},
		{
			name:  "no subquery",
			query: "up",
		},
		{
			name:    "valid subquery and within step limit",
			query:   "up[60m:1m]",
			maxStep: 100,
		},
		{
			name:    "valid subquery, not within step limit",
			query:   "up[60m:1m]",
			maxStep: 10,
			err:     httpgrpc.Errorf(http.StatusBadRequest, ErrSubQueryStepTooSmall, 10),
		},
		{
			name:        "subquery with no step size defined, use default step and pass",
			query:       "up[60m:]",
			maxStep:     100,
			defaultStep: time.Minute,
		},
		{
			name:        "subquery with no step size defined, use default step and fail",
			query:       "up[60m:]",
			maxStep:     100,
			defaultStep: time.Second,
			err:         httpgrpc.Errorf(http.StatusBadRequest, ErrSubQueryStepTooSmall, 100),
		},
		{
			name:        "two subqueries within functions, one exceeds the limit while another is not",
			query:       "sum_over_time(up[60m:]) + avg_over_time(test[5m:1m])",
			maxStep:     10,
			defaultStep: time.Second,
			err:         httpgrpc.Errorf(http.StatusBadRequest, ErrSubQueryStepTooSmall, 10),
		},
		{
			name:        "two subqueries within functions, all within the limit",
			query:       "sum_over_time(up[60m:]) + avg_over_time(test[5m:1m])",
			maxStep:     100,
			defaultStep: time.Minute,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := SubQueryStepSizeCheck(tc.query, tc.defaultStep, tc.maxStep)
			require.Equal(t, tc.err, err)
		})
	}
}

func TestAAA(t *testing.T) {
	qa := NewDisableBinaryExpressionAnalyzer(querysharding.NewQueryAnalyzer())
	//q := `tanh(({__name__="test_series_b"} / ignoring (status_code) {__name__="test_series_a"}))`
	//q := `avg without () (({__name__="test_series_a"} == on (status_code) {__name__="test_series_b"}))`
	q := `asin(
          acosh(
            (
                -{__name__="test_series_a"} offset 1m11s
              == bool
                timestamp({__name__="test_series_b"} @ end() offset 4m22s)
            )
          )
        )`
	analysis, _ := qa.Analyze(q)
	fmt.Println(analysis.IsShardable(), analysis.ShardBy(), analysis.ShardingLabels())
}
