package tripperware

import (
	"context"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
)

const (
	// QueryLabel is a reserved label containing an embedded query
	QueryLabel = "__cortex_queries__"
)

type RemoteQueryable struct {
	Req  Request
	Next Handler

	RespToSeriesSetFunc func(sortSeries bool, resp Response) storage.SeriesSet
}

func (q *RemoteQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	return &RemoteQuerier{
		mint:                mint,
		maxt:                maxt,
		next:                q.Next,
		req:                 q.Req,
		respToSeriesSetFunc: q.RespToSeriesSetFunc,
	}, nil
}

type RemoteQuerier struct {
	mint, maxt int64
	next       Handler
	req        Request

	respToSeriesSetFunc func(sortSeries bool, resp Response) storage.SeriesSet
}

func (q *RemoteQuerier) Select(ctx context.Context, sortSeries bool, _ *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	req := q.req
	if len(matchers) == 1 && matchers[0].Type == labels.MatchEqual && matchers[0].Name == QueryLabel {
		req = req.WithQuery(matchers[0].Value)
	}
	resp, err := q.next.Do(ctx, req)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}
	return q.respToSeriesSetFunc(sortSeries, resp)
}

// LabelValues returns all potential values for a label name.
func (q *RemoteQuerier) LabelValues(ctx context.Context, name string, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, errors.Errorf("unimplemented")
}

// LabelNames returns all the unique label names present in the block in sorted order.
func (q *RemoteQuerier) LabelNames(ctx context.Context, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, errors.Errorf("unimplemented")
}

// Close releases the resources of the Querier.
func (q *RemoteQuerier) Close() error {
	return nil
}
