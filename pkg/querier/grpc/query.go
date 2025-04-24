package grpc

import (
	"context"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/thanos-io/promql-engine/engine"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/thanos/pkg/api/query/querypb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
	"github.com/thanos-io/thanos/pkg/tracing"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"time"
)

type planOrQuery struct {
	query string
	plan  logicalplan.Node
}

type QueryGRPCAPI struct {
	queryable         storage.Queryable
	engine            promql.QueryEngine
	thanosEngine      bool
	distributedEngine bool
}

func NewQueryGRPCAPI(queryable storage.Queryable, engine promql.QueryEngine, thanosEngine bool, distributedEngine bool) *QueryGRPCAPI {
	return &QueryGRPCAPI{
		queryable:         queryable,
		engine:            engine,
		thanosEngine:      thanosEngine,
		distributedEngine: distributedEngine,
	}
}

func (g *QueryGRPCAPI) Query(request *querypb.QueryRequest, srv querypb.Query_QueryRangeServer) error {
	panic("unimplemented")
}

func (g *QueryGRPCAPI) QueryRange(request *querypb.QueryRangeRequest, srv querypb.Query_QueryRangeServer) error {
	ctx := srv.Context()
	if request.TimeoutSeconds != 0 {
		var cancel context.CancelFunc
		timeout := time.Duration(request.TimeoutSeconds) * time.Second
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	var qry promql.Query
	if err := tracing.DoInSpanWithErr(ctx, "range_query_create", func(ctx context.Context) error {
		var err error
		qry, err = g.getRangeQueryForEngine(ctx, request, g.queryable)
		return err
	}); err != nil {
		return err
	}
	defer qry.Close()

	var result *promql.Result
	tracing.DoInSpan(ctx, "range_query_exec", func(ctx context.Context) {
		result = qry.Exec(ctx)
	})
	if result.Err != nil {
		return status.Error(codes.Aborted, result.Err.Error())
	}

	if len(result.Warnings) != 0 {
		if err := srv.Send(querypb.NewQueryRangeWarningsResponse(result.Warnings.AsErrors()...)); err != nil {
			return err
		}
	}

	switch value := result.Value.(type) {
	case promql.Matrix:
		for _, series := range value {
			floats, histograms := prompb.SamplesFromPromqlSeries(series)
			series := &prompb.TimeSeries{
				Labels:     labelpb.ZLabelsFromPromLabels(series.Metric),
				Samples:    floats,
				Histograms: histograms,
			}
			if err := srv.Send(querypb.NewQueryRangeResponse(series)); err != nil {
				return err
			}
		}
	case promql.Vector:
		for _, sample := range value {
			floats, histograms := prompb.SamplesFromPromqlSamples(sample)
			series := &prompb.TimeSeries{
				Labels:     labelpb.ZLabelsFromPromLabels(sample.Metric),
				Samples:    floats,
				Histograms: histograms,
			}
			if err := srv.Send(querypb.NewQueryRangeResponse(series)); err != nil {
				return err
			}
		}
	case promql.Scalar:
		series := &prompb.TimeSeries{
			Samples: []prompb.Sample{{Value: value.V, Timestamp: value.T}},
		}
		if err := srv.Send(querypb.NewQueryRangeResponse(series)); err != nil {
			return err
		}
	}
	if err := srv.Send(querypb.NewQueryRangeStatsResponse(extractQueryStats(qry))); err != nil {
		return err
	}

	return nil
}

func (g *QueryGRPCAPI) getRangeQueryForEngine(
	ctx context.Context,
	request *querypb.QueryRangeRequest,
	queryable storage.Queryable,
) (promql.Query, error) {
	start := time.Unix(request.StartTimeSeconds, 0)
	end := time.Unix(request.EndTimeSeconds, 0)
	step := time.Duration(request.IntervalSeconds) * time.Second

	opts := &engine.QueryOpts{}

	var qry planOrQuery
	if plan, err := logicalplan.Unmarshal(request.QueryPlan.GetJson()); err != nil {
		qry = planOrQuery{plan: plan, query: request.Query}
	} else {
		qry = planOrQuery{query: request.Query}
	}

	var (
		res promql.Query
		err error
	)
	if g.thanosEngine && !g.distributedEngine {
		e := g.engine.(*engine.Engine)
		if qry.plan != nil {
			res, err = e.MakeRangeQueryFromPlan(ctx, queryable, opts, qry.plan, start, end, step)
		} else {
			res, err = e.MakeRangeQuery(ctx, queryable, opts, qry.query, start, end, step)
		}
		if err != nil {
			if engine.IsUnimplemented(err) {
				// fallback to prometheus
				return f.prometheus.NewRangeQuery(ctx, q, opts, qry.query, start, end, step)
			}
			return nil, err
		}
		return res, nil
	}
	if g.thanosEngine && !g.distributedEngine {
		e := g.engine.(*engine.DistributedEngine)
		if qry.plan != nil {
			res, err = e.MakeRangeQueryFromPlan(ctx, queryable, e, opts, qry.plan, start, end, step)
		} else {
			res, err = e.MakeRangeQuery(ctx, queryable, e, opts, qry.query, start, end, step)
		}
		if err != nil {
			if engine.IsUnimplemented(err) {
				// fallback to prometheus
				return f.prometheus.NewRangeQuery(ctx, queryable, opts, qry.query, start, end, step)
			}
			return nil, err
		}
		return res, nil
	}
	return f.prometheus.NewRangeQuery(ctx, queryable, opts, qry.query, start, end, step)
}

func extractQueryStats(qry promql.Query) *querypb.QueryStats {
	stats := &querypb.QueryStats{
		SamplesTotal: 0,
		PeakSamples:  0,
	}
	if explQry, ok := qry.(engine.ExplainableQuery); ok {
		analyze := explQry.Analyze()
		stats.SamplesTotal = analyze.TotalSamples()
		stats.PeakSamples = analyze.PeakSamples()
	}

	return stats
}
