package distributed_execution

import (
	"fmt"
	"github.com/cortexproject/cortex/pkg/engine/distributed_execution/querierpb"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/promql-engine/execution/model"
	"time"
)

const (
	BATCHSIZE      = 1000
	WritingTimeout = 100 * time.Millisecond
)

type QuerierServer struct {
	queryResultCache *QueryResultCache
}

func NewQuerierServer(cache *QueryResultCache) *QuerierServer {
	return &QuerierServer{
		queryResultCache: cache,
	}
}

func (s *QuerierServer) Series(req *querierpb.SeriesRequest, srv querierpb.Querier_SeriesServer) error {
	key := MakeFragmentKey(req.QueryID, req.FragmentID)

	for {
		// TODO: maybe add a timeout thing here too
		result, ok := s.queryResultCache.Get(key)
		if !ok {
			return fmt.Errorf("fragment not found: %v", key)
		}

		switch result.Status {
		case StatusDone:
			series, ok := result.Data.([]labels.Labels)
			if !ok {
				return fmt.Errorf("invalid data type in cache")
			}

			batchSize := int(req.Batchsize)
			if batchSize <= 0 {
				batchSize = BATCHSIZE
			}

			for i := 0; i < len(series); i += batchSize {
				end := i + batchSize
				if end > len(series) {
					end = len(series)
				}

				for _, s := range series[i:end] {
					protoLabels := make([]*querierpb.Label, len(s))
					for j, l := range s {
						protoLabels[j] = &querierpb.Label{
							Name:  l.Name,
							Value: l.Value,
						}
					}
					if err := srv.Send(&querierpb.OneSeries{
						Labels: protoLabels,
					}); err != nil {
						return err
					}
				}
			}
			return nil

		case StatusError:
			return fmt.Errorf("fragment processing failed")

		case StatusWriting:
			time.Sleep(WritingTimeout)
			continue
		}
	}
}

func (s *QuerierServer) Next(req *querierpb.NextRequest, srv querierpb.Querier_NextServer) error {
	key := MakeFragmentKey(req.QueryID, req.FragmentID)

	for {
		// TODO: maybe add a timeout thing here too
		result, ok := s.queryResultCache.Get(key)
		if !ok {
			return fmt.Errorf("fragment not found: %v", key)
		}

		switch result.Status {
		case StatusDone:
			vectors, ok := result.Data.([]model.StepVector)
			if !ok {
				return fmt.Errorf("invalid data type in cache")
			}

			batchSize := int(req.Batchsize)
			if batchSize <= 0 {
				batchSize = BATCHSIZE
			}

			for i := 0; i < len(vectors); i += batchSize {
				end := i + batchSize
				if end > len(vectors) {
					end = len(vectors)
				}

				batch := &querierpb.StepVectorBatch{
					StepVectors: make([]*querierpb.StepVector, 0, end-i),
				}

				for _, v := range vectors[i:end] {
					protoVector := &querierpb.StepVector{
						T:             v.T,
						Sample_IDs:    v.SampleIDs,
						Samples:       v.Samples,
						Histogram_IDs: v.HistogramIDs,
						Histograms:    FloatHistogramsToFloatHistogramProto(v.Histograms),
					}
					batch.StepVectors = append(batch.StepVectors, protoVector)
				}

				if err := srv.Send(batch); err != nil {
					return err
				}
			}
			return nil

		case StatusError:
			return fmt.Errorf("fragment processing failed")

		case StatusWriting:
			time.Sleep(WritingTimeout)
			continue
		}
	}
}

func FloatHistogramsToFloatHistogramProto(histograms []*histogram.FloatHistogram) []querierpb.Histogram {
	if histograms == nil {
		return nil
	}

	protoHistograms := make([]querierpb.Histogram, 0, len(histograms))
	for _, h := range histograms {
		if h != nil {
			protoHist := FloatHistogramToFloatHistogramProto(h)
			protoHistograms = append(protoHistograms, *protoHist)
		}
	}
	return protoHistograms
}

func FloatHistogramToFloatHistogramProto(h *histogram.FloatHistogram) *querierpb.Histogram {
	if h == nil {
		return nil
	}

	return &querierpb.Histogram{
		ResetHint:     querierpb.Histogram_ResetHint(h.CounterResetHint),
		Schema:        h.Schema,
		ZeroThreshold: h.ZeroThreshold,
		Count: &querierpb.Histogram_CountFloat{
			CountFloat: h.Count,
		},
		ZeroCount: &querierpb.Histogram_ZeroCountFloat{
			ZeroCountFloat: h.ZeroCount,
		},
		Sum:            h.Sum,
		PositiveSpans:  spansToSpansProto(h.PositiveSpans),
		PositiveCounts: h.PositiveBuckets,
		NegativeSpans:  spansToSpansProto(h.NegativeSpans),
		NegativeCounts: h.NegativeBuckets,
	}
}

func spansToSpansProto(spans []histogram.Span) []querierpb.BucketSpan {
	if spans == nil {
		return nil
	}
	protoSpans := make([]querierpb.BucketSpan, len(spans))
	for i, span := range spans {
		protoSpans[i] = querierpb.BucketSpan{
			Offset: span.Offset,
			Length: span.Length,
		}
	}
	return protoSpans
}
