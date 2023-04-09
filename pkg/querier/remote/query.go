package remote

import (
	"context"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/util/stats"
	"time"
)

type RemoteQuery struct {
	logger log.Logger
	//client Client
	//opts   Opts

	qs       string
	start    time.Time
	end      time.Time
	interval time.Duration

	cancel context.CancelFunc
}

func (r *RemoteQuery) Exec(ctx context.Context) *promql.Result {
	start := time.Now()

	qctx, cancel := context.WithCancel(ctx)
	r.cancel = cancel
	defer cancel()
	_ = qctx

	//request := &querypb.QueryRangeRequest{
	//	Query:                 r.qs,
	//	StartTimeSeconds:      r.start.Unix(),
	//	EndTimeSeconds:        r.end.Unix(),
	//	IntervalSeconds:       int64(r.interval.Seconds()),
	//	TimeoutSeconds:        int64(r.opts.Timeout.Seconds()),
	//	EnablePartialResponse: r.opts.EnablePartialResponse,
	//	// TODO (fpetkovski): Allow specifying these parameters at query time.
	//	// This will likely require a change in the remote engine interface.
	//	ReplicaLabels:        r.opts.ReplicaLabels,
	//	MaxResolutionSeconds: maxResolution,
	//	EnableDedup:          true,
	//}
	//qry, err := r.client.QueryRange(qctx, request)
	//if err != nil {
	//	return &promql.Result{Err: err}
	//}
	//
	//result := make(promql.Matrix, 0)
	//for {
	//	msg, err := qry.Recv()
	//	if err == io.EOF {
	//		break
	//	}
	//	if err != nil {
	//		return &promql.Result{Err: err}
	//	}
	//
	//	if warn := msg.GetWarnings(); warn != "" {
	//		return &promql.Result{Err: errors.New(warn)}
	//	}
	//
	//	ts := msg.GetTimeseries()
	//	if ts == nil {
	//		continue
	//	}
	//	series := promql.Series{
	//		Metric: labelpb.ZLabelsToPromLabels(ts.Labels),
	//		Points: make([]promql.Point, 0, len(ts.Samples)),
	//	}
	//	for _, s := range ts.Samples {
	//		series.Points = append(series.Points, promql.Point{
	//			T: s.Timestamp,
	//			V: s.Value,
	//		})
	//	}
	//	result = append(result, series)
	//}
	level.Debug(r.logger).Log("Executed query", "query", r.qs, "time", time.Since(start))

	return &promql.Result{Value: nil}
}

func (r *RemoteQuery) Close() {
	r.Cancel()
}

func (r *RemoteQuery) Statement() parser.Statement {
	return nil
}

func (r *RemoteQuery) Stats() *stats.Statistics {
	return nil
}

func (r *RemoteQuery) Cancel() {
	if r.cancel != nil {
		r.cancel()
	}
}

func (r *RemoteQuery) String() string {
	return r.qs
}
