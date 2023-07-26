package tripperware

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
)

func TestSubQueryStepSizeCheck(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		query         string
		defaultStep   time.Duration
		err           error
		maxStep       int
		maxTotalSteps int
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
			name:          "valid subquery and within step limit",
			query:         "up[60m:1m]",
			maxStep:       100,
			maxTotalSteps: 10000,
		},
		{
			name:    "valid subquery, not within step limit",
			query:   "up[60m:1m]",
			maxStep: 10,
			err:     httpgrpc.Errorf(http.StatusBadRequest, ErrSubQueryStepTooSmall, 10, 60),
		},
		{
			name:          "subquery with no step size defined, use default step and pass",
			query:         "up[60m:]",
			maxStep:       100,
			maxTotalSteps: 10000,
			defaultStep:   time.Minute,
		},
		{
			name:        "subquery with no step size defined, use default step and fail",
			query:       "up[60m:]",
			maxStep:     100,
			defaultStep: time.Second,
			err:         httpgrpc.Errorf(http.StatusBadRequest, ErrSubQueryStepTooSmall, 100, 3600),
		},
		{
			name:        "two subqueries within functions, one exceeds the limit while another is not",
			query:       "sum_over_time(up[60m:]) + avg_over_time(test[5m:1m])",
			maxStep:     10,
			defaultStep: time.Second,
			err:         httpgrpc.Errorf(http.StatusBadRequest, ErrSubQueryStepTooSmall, 10, 3600),
		},
		{
			name:          "two subqueries within functions, all within the limit",
			query:         "sum_over_time(up[60m:]) + avg_over_time(test[5m:1m])",
			maxStep:       100,
			maxTotalSteps: 10000,
			defaultStep:   time.Minute,
		},
		{
			name: "nested subqueries failed total steps",
			query: `avg_over_time(
sum by (cluster) (
    increase(
     test_metric[1m:10s])
  )
[1d:10s])`,
			maxStep:       11000,
			maxTotalSteps: 20000,
			defaultStep:   time.Minute,
			err:           httpgrpc.Errorf(http.StatusBadRequest, ErrSubQueryTotalStepsExceeded, 20000, 51840),
		},
		{
			name:          "failed total steps",
			query:         `avg_over_time(sum(increase(metric[1d:10s]))[1m:10s]) - max_over_time(rate(foo[1d:10s])[30s:10s])`,
			maxStep:       11000,
			maxTotalSteps: 20000,
			defaultStep:   time.Minute,
			err:           httpgrpc.Errorf(http.StatusBadRequest, ErrSubQueryTotalStepsExceeded, 20000, 77760),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := SubQueryStepSizeCheck(tc.query, tc.defaultStep, tc.maxStep, tc.maxTotalSteps)
			require.Equal(t, tc.err, err)
		})
	}
}

func TestGetSubQueryStepsFromQuery(t *testing.T) {
	defaultStep := time.Minute
	for _, tc := range []struct {
		name               string
		query              string
		expectedTotalSteps int
		expectedMaxStep    int
	}{
		{
			name:  "vector",
			query: "test_metric",
		},
		{
			name:  "matrix",
			query: "test_metric[10d]",
		},
		{
			name:  "number literal",
			query: "1",
		},
		{
			name:               "subquery",
			query:              "metric[1d:10s]",
			expectedTotalSteps: 8640,
			expectedMaxStep:    8640,
		},
		{
			name:               "function with subquery",
			query:              "avg_over_time(metric[1d:10s])",
			expectedTotalSteps: 8640,
			expectedMaxStep:    8640,
		},
		{
			name:               "function 2 with subquery",
			query:              "increase(metric[1d:10s])",
			expectedTotalSteps: 8640,
			expectedMaxStep:    8640,
		},
		{
			name:               "aggregation, function with subquery",
			query:              "sum(increase(metric[1d:10s]))",
			expectedTotalSteps: 8640,
			expectedMaxStep:    8640,
		},
		{
			name:               "binary expression, aggregation, function with subquery",
			query:              "sum(increase(metric[1d:10s])) + avg(increase(metric[2d:10s]))",
			expectedTotalSteps: 8640 * 3,
			expectedMaxStep:    8640 * 2,
		},
		{
			name:               "nested function and subqueries",
			query:              "avg_over_time(sum(increase(metric[1d:10s]))[1m:10s])",
			expectedTotalSteps: 8640 * 6,
			expectedMaxStep:    8640,
		},
		{
			name:               "nested function and subqueries + vector",
			query:              "avg_over_time(sum(increase(metric[1d:10s]))[1m:10s]) + foo",
			expectedTotalSteps: 8640 * 6,
			expectedMaxStep:    8640,
		},
		{
			name:               "nested function and subqueries + function without subquery",
			query:              "avg_over_time(sum(increase(metric[1d:10s]))[1m:10s]) + sum_over_time(foo[100d])",
			expectedTotalSteps: 8640 * 6,
			expectedMaxStep:    8640,
		},
		{
			name:               "nested function and subqueries with binary expression",
			query:              "avg_over_time(sum(increase(metric[1d:10s]))[1m:10s]) - max_over_time(rate(foo[1d:10s])[30s:10s])",
			expectedTotalSteps: 8640*6 + 8640*3,
			expectedMaxStep:    8640,
		},
		{
			name:               "crazy",
			expectedTotalSteps: 1296000,
			expectedMaxStep:    8640,
			query: `avg_over_time(
  sum by (review_type) (
    increase(
      risk_manual_account_review_created_total{host_env="prod",review_type!=""}
    [1m:10s])
  )
[1d:10s]) > ((((3 * (((2 * (((1 * avg_over_time(
  sum by (review_type) (
    increase(
      risk_manual_account_review_created_total{host_env="prod",review_type!=""}
    [1m:10s] offset 1w)
  )
[1d:10s])) + avg_over_time(
  sum by (review_type) (
    increase(
      risk_manual_account_review_created_total{host_env="prod",review_type!=""}
    [1m:10s] offset 2w)
  )
[1d:10s])) / 2)) + avg_over_time(
  sum by (review_type) (
    increase(
      risk_manual_account_review_created_total{host_env="prod",review_type!=""}
    [1m:10s] offset 3w)
  )
[1d:10s])) / 3)) + avg_over_time(
  sum by (review_type) (
    increase(
      risk_manual_account_review_created_total{host_env="prod",review_type!=""}
    [1m:10s] offset 4w)
  )
[1d:10s])) / 4) + (2.5 * sqrt((((3 * (((2 * (((1 * (stdvar_over_time(
  sum by (review_type) (
    increase(
      risk_manual_account_review_created_total{host_env="prod",review_type!=""}
    [1m:10s] offset 1w)
  )
[1d:10s]) + (avg_over_time(
  sum by (review_type) (
    increase(
      risk_manual_account_review_created_total{host_env="prod",review_type!=""}
    [1m:10s] offset 1w)
  )
[1d:10s]) * avg_over_time(
  sum by (review_type) (
    increase(
      risk_manual_account_review_created_total{host_env="prod",review_type!=""}
    [1m:10s] offset 1w)
  )
[1d:10s])))) + (stdvar_over_time(
  sum by (review_type) (
    increase(
      risk_manual_account_review_created_total{host_env="prod",review_type!=""}
    [1m:10s] offset 2w)
  )
[1d:10s]) + (avg_over_time(
  sum by (review_type) (
    increase(
      risk_manual_account_review_created_total{host_env="prod",review_type!=""}
    [1m:10s] offset 2w)
  )
[1d:10s]) * avg_over_time(
  sum by (review_type) (
    increase(
      risk_manual_account_review_created_total{host_env="prod",review_type!=""}
    [1m:10s] offset 2w)
  )
[1d:10s])))) / 2)) + (stdvar_over_time(
  sum by (review_type) (
    increase(
      risk_manual_account_review_created_total{host_env="prod",review_type!=""}
    [1m:10s] offset 3w)
  )
[1d:10s]) + (avg_over_time(
  sum by (review_type) (
    increase(
      risk_manual_account_review_created_total{host_env="prod",review_type!=""}
    [1m:10s] offset 3w)
  )
[1d:10s]) * avg_over_time(
  sum by (review_type) (
    increase(
      risk_manual_account_review_created_total{host_env="prod",review_type!=""}
    [1m:10s] offset 3w)
  )
[1d:10s])))) / 3)) + (stdvar_over_time(
  sum by (review_type) (
    increase(
      risk_manual_account_review_created_total{host_env="prod",review_type!=""}
    [1m:10s] offset 4w)
  )
[1d:10s]) + (avg_over_time(
  sum by (review_type) (
    increase(
      risk_manual_account_review_created_total{host_env="prod",review_type!=""}
    [1m:10s] offset 4w)
  )
[1d:10s]) * avg_over_time(
  sum by (review_type) (
    increase(
      risk_manual_account_review_created_total{host_env="prod",review_type!=""}
    [1m:10s] offset 4w)
  )
[1d:10s])))) / 4) - ((((3 * (((2 * (((1 * avg_over_time(
  sum by (review_type) (
    increase(
      risk_manual_account_review_created_total{host_env="prod",review_type!=""}
    [1m:10s] offset 1w)
  )
[1d:10s])) + avg_over_time(
  sum by (review_type) (
    increase(
      risk_manual_account_review_created_total{host_env="prod",review_type!=""}
    [1m:10s] offset 2w)
  )
[1d:10s])) / 2)) + avg_over_time(
  sum by (review_type) (
    increase(
      risk_manual_account_review_created_total{host_env="prod",review_type!=""}
    [1m:10s] offset 3w)
  )
[1d:10s])) / 3)) + avg_over_time(
  sum by (review_type) (
    increase(
      risk_manual_account_review_created_total{host_env="prod",review_type!=""}
    [1m:10s] offset 4w)
  )
[1d:10s])) / 4) * (((3 * (((2 * (((1 * avg_over_time(
  sum by (review_type) (
    increase(
      risk_manual_account_review_created_total{host_env="prod",review_type!=""}
    [1m:10s] offset 1w)
  )
[1d:10s])) + avg_over_time(
  sum by (review_type) (
    increase(
      risk_manual_account_review_created_total{host_env="prod",review_type!=""}
    [1m:10s] offset 2w)
  )
[1d:10s])) / 2)) + avg_over_time(
  sum by (review_type) (
    increase(
      risk_manual_account_review_created_total{host_env="prod",review_type!=""}
    [1m:10s] offset 3w)
  )
[1d:10s])) / 3)) + avg_over_time(
  sum by (review_type) (
    increase(
      risk_manual_account_review_created_total{host_env="prod",review_type!=""}
    [1m:10s] offset 4w)
  )
[1d:10s])) / 4)))))`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			totalSteps, maxStep := GetSubQueryStepsFromQuery(tc.query, defaultStep)
			require.Equal(t, tc.expectedTotalSteps, totalSteps)
			require.Equal(t, tc.expectedMaxStep, maxStep)
		})
	}
}
