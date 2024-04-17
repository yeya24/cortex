package ingester

import (
	"context"
	"sort"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
)

var (
	allPostingMatcher = labels.MustNewMatcher(labels.MatchEqual, "", "")
)

type ExternalLabelQuerier struct {
	storage.Querier
	label      labels.Label
	matchEqual bool
}

func NewExternalLabelQuerier(q storage.Querier, label labels.Label, matchEqual bool) *ExternalLabelQuerier {
	return &ExternalLabelQuerier{
		Querier:    q,
		label:      label,
		matchEqual: matchEqual,
	}
}

func (q *ExternalLabelQuerier) LabelValues(ctx context.Context, name string, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	if q.matchEqual && name == q.label.Name {
		return []string{q.label.Value}, nil, nil
	}

	var notRegexpMatcher *labels.Matcher
	newMatchers := make([]*labels.Matcher, 0, len(matchers))
	for _, matcher := range matchers {
		if matcher.Name == q.label.Name {
			if q.matchEqual {
				if !matcher.Matches(q.label.Value) {
					return nil, nil, nil
				}
				continue
			}

			if matcher.Type == labels.MatchEqual {
				if notRegexpMatcher == nil {
					notRegexpMatcher = labels.MustNewMatcher(labels.MatchNotRegexp, q.label.Name, q.label.Value)
				}
				if !notRegexpMatcher.Matches(matcher.Value) {
					return nil, nil, nil
				}
				// If match, add the matcher
			}
		}
		newMatchers = append(newMatchers, matcher)
	}

	return q.Querier.LabelValues(ctx, name, newMatchers...)
}

func (q *ExternalLabelQuerier) LabelNames(ctx context.Context, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	newMatchers := make([]*labels.Matcher, 0, len(matchers))
	var notRegexpMatcher *labels.Matcher
	for _, matcher := range matchers {
		if matcher.Name == q.label.Name {
			if q.matchEqual {
				if !matcher.Matches(q.label.Value) {
					return nil, nil, nil
				}
				continue
			}

			if matcher.Type == labels.MatchEqual {
				if notRegexpMatcher == nil {
					notRegexpMatcher = labels.MustNewMatcher(labels.MatchNotRegexp, q.label.Name, q.label.Value)
				}
				if !notRegexpMatcher.Matches(matcher.Value) {
					return nil, nil, nil
				}
				// If match, add the matcher
			}
		}
		newMatchers = append(newMatchers, matcher)
	}
	names, warnings, err := q.Querier.LabelNames(ctx, newMatchers...)
	if err != nil {
		return nil, warnings, err
	}
	if q.matchEqual {
		// Attach external label name and sort.
		names = append(names, q.label.Name)
		sort.Strings(names)
	}
	return names, warnings, nil
}

func (q *ExternalLabelQuerier) Close() error {
	return q.Querier.Close()
}

func (q *ExternalLabelQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	newMatchers := make([]*labels.Matcher, 0, len(matchers))
	var notRegexpMatcher *labels.Matcher
	for _, matcher := range matchers {
		if matcher.Name == q.label.Name {
			if q.matchEqual {
				if !matcher.Matches(q.label.Value) {
					return storage.EmptySeriesSet()
				}
				continue
			}

			if matcher.Type == labels.MatchEqual {
				if notRegexpMatcher == nil {
					notRegexpMatcher = labels.MustNewMatcher(labels.MatchNotRegexp, q.label.Name, q.label.Value)
				}
				if !notRegexpMatcher.Matches(matcher.Value) {
					return storage.EmptySeriesSet()
				}
				// If match, add the matcher
			}
		}
		newMatchers = append(newMatchers, matcher)
	}
	if len(newMatchers) == 0 {
		newMatchers = append(newMatchers, allPostingMatcher)
	}

	if !q.matchEqual {
		return q.Querier.Select(ctx, sortSeries, hints, newMatchers...)
	}
	return &ExternalLabelSeriesSet{
		label:     q.label,
		SeriesSet: q.Querier.Select(ctx, sortSeries, hints, newMatchers...),
		builder:   labels.NewBuilder(labels.EmptyLabels()),
	}
}

type ExternalLabelChunkQuerier struct {
	storage.ChunkQuerier
	label      labels.Label
	matchEqual bool
}

func NewExternalLabelChunkQuerier(q storage.ChunkQuerier, label labels.Label, matchEqual bool) *ExternalLabelChunkQuerier {
	return &ExternalLabelChunkQuerier{
		ChunkQuerier: q,
		label:        label,
		matchEqual:   matchEqual,
	}
}

func (q *ExternalLabelChunkQuerier) LabelValues(ctx context.Context, name string, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	if q.matchEqual && name == q.label.Name {
		return []string{q.label.Value}, nil, nil
	}

	var notRegexpMatcher *labels.Matcher
	newMatchers := make([]*labels.Matcher, 0, len(matchers))
	for _, matcher := range matchers {
		if matcher.Name == q.label.Name {
			if q.matchEqual {
				if !matcher.Matches(q.label.Value) {
					return nil, nil, nil
				}
				continue
			}

			if matcher.Type == labels.MatchEqual {
				if notRegexpMatcher == nil {
					notRegexpMatcher = labels.MustNewMatcher(labels.MatchNotRegexp, q.label.Name, q.label.Value)
				}
				if !notRegexpMatcher.Matches(matcher.Value) {
					return nil, nil, nil
				}
				// If match, add the matcher
			}
		}
		newMatchers = append(newMatchers, matcher)
	}

	return q.ChunkQuerier.LabelValues(ctx, name, newMatchers...)
}

func (q *ExternalLabelChunkQuerier) LabelNames(ctx context.Context, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	var notRegexpMatcher *labels.Matcher
	newMatchers := make([]*labels.Matcher, 0, len(matchers))
	for _, matcher := range matchers {
		if matcher.Name == q.label.Name {
			if q.matchEqual {
				if !matcher.Matches(q.label.Value) {
					return nil, nil, nil
				}
				continue
			}

			if matcher.Type == labels.MatchEqual {
				if notRegexpMatcher == nil {
					notRegexpMatcher = labels.MustNewMatcher(labels.MatchNotRegexp, q.label.Name, q.label.Value)
				}
				if !notRegexpMatcher.Matches(matcher.Value) {
					return nil, nil, nil
				}
				// If match, add the matcher
			}
		}

		newMatchers = append(newMatchers, matcher)
	}
	names, warnings, err := q.ChunkQuerier.LabelNames(ctx, newMatchers...)
	if err != nil {
		return nil, warnings, err
	}
	if q.matchEqual {
		// Attach external label name and sort.
		names = append(names, q.label.Name)
		sort.Strings(names)
	}
	return names, warnings, nil
}

func (q *ExternalLabelChunkQuerier) Close() error {
	return q.ChunkQuerier.Close()
}

func (q *ExternalLabelChunkQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.ChunkSeriesSet {
	newMatchers := make([]*labels.Matcher, 0, len(matchers))
	var notRegexpMatcher *labels.Matcher
	for _, matcher := range matchers {
		if matcher.Name == q.label.Name {
			if q.matchEqual {
				if !matcher.Matches(q.label.Value) {
					return storage.EmptyChunkSeriesSet()
				}
				continue
			}
			if matcher.Type == labels.MatchEqual {
				if notRegexpMatcher == nil {
					notRegexpMatcher = labels.MustNewMatcher(labels.MatchNotRegexp, q.label.Name, q.label.Value)
				}
				if !notRegexpMatcher.Matches(matcher.Value) {
					return storage.EmptyChunkSeriesSet()
				}
				// If match, add the matcher
			}
		}

		newMatchers = append(newMatchers, matcher)
	}
	if len(newMatchers) == 0 {
		newMatchers = append(newMatchers, allPostingMatcher)
	}
	if !q.matchEqual {
		return q.ChunkQuerier.Select(ctx, sortSeries, hints, newMatchers...)
	}
	return &ExternalLabelChunkSeriesSet{
		label:          q.label,
		ChunkSeriesSet: q.ChunkQuerier.Select(ctx, sortSeries, hints, newMatchers...),
		builder:        labels.NewBuilder(labels.EmptyLabels()),
	}
}

type ExternalLabelSeriesSet struct {
	label   labels.Label
	builder *labels.Builder
	storage.SeriesSet
}

// At returns full series. Returned series should be iterable even after Next is called.
func (s ExternalLabelSeriesSet) At() storage.Series {
	series := s.SeriesSet.At()
	// Attach external labels
	s.builder.Reset(series.Labels())
	s.builder.Set(s.label.Name, s.label.Value)
	return &storage.SeriesEntry{
		Lset:             s.builder.Labels(),
		SampleIteratorFn: series.Iterator,
	}
}

type ExternalLabelChunkSeriesSet struct {
	label   labels.Label
	builder *labels.Builder
	storage.ChunkSeriesSet
}

// At returns full series. Returned series should be iterable even after Next is called.
func (s ExternalLabelChunkSeriesSet) At() storage.ChunkSeries {
	series := s.ChunkSeriesSet.At()
	// Attach external labels
	s.builder.Reset(series.Labels())
	s.builder.Set(s.label.Name, s.label.Value)
	return &storage.ChunkSeriesEntry{
		Lset:            s.builder.Labels(),
		ChunkIteratorFn: series.Iterator,
	}
}

type ShardByMetricNameQuerier struct {
	storage.Querier
	shardCount, shardIdx uint64
}

func NewShardByMetricNameQuerier(q storage.Querier, shardCount, shardIdx uint64) *ShardByMetricNameQuerier {
	return &ShardByMetricNameQuerier{
		Querier:    q,
		shardCount: shardCount,
		shardIdx:   shardIdx,
	}
}

func (q *ShardByMetricNameQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	for _, matcher := range matchers {
		if matcher.Name == labels.MetricName && matcher.Type == labels.MatchEqual {
			if hash(matcher.Value)%q.shardCount != q.shardIdx {
				return storage.EmptySeriesSet()
			}
		}
	}
	return q.Querier.Select(ctx, sortSeries, hints, matchers...)
}

type ShardByMetricNameChunkQuerier struct {
	storage.ChunkQuerier
	shardCount, shardIdx uint64
}

func NewShardByMetricNameChunkQuerier(q storage.ChunkQuerier, shardCount, shardIdx uint64) *ShardByMetricNameChunkQuerier {
	return &ShardByMetricNameChunkQuerier{
		ChunkQuerier: q,
		shardCount:   shardCount,
		shardIdx:     shardIdx,
	}
}

func (q *ShardByMetricNameChunkQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.ChunkSeriesSet {
	for _, matcher := range matchers {
		if matcher.Name == labels.MetricName && matcher.Type == labels.MatchEqual {
			if hash(matcher.Value)%q.shardCount != q.shardIdx {
				return storage.EmptyChunkSeriesSet()
			}
		}
	}
	return q.ChunkQuerier.Select(ctx, sortSeries, hints, matchers...)
}
