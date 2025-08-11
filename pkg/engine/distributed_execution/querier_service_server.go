package distributed_execution

import (
	"fmt"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"time"

	"github.com/prometheus/prometheus/model/histogram"

	"github.com/cortexproject/cortex/pkg/engine/distributed_execution/querierpb"
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
		result, ok := s.queryResultCache.Get(*key)
		if !ok {
			return fmt.Errorf("fragment not found: %v", key)
		}

		batchSize := int(req.Batchsize)
		if batchSize <= 0 {
			batchSize = BATCHSIZE
		}

		switch result.Status {
		case StatusDone:
			fragmentResult := result.Data.(FragmentResult)
			v1ResultData := fragmentResult.Data.(*v1.QueryData)

			if v1ResultData.ResultType == parser.ValueTypeMatrix {
				series := v1ResultData.Result.(promql.Matrix)
				for i := 0; i < len(series); i += batchSize {
					end := i + batchSize
					if end > len(series) {
						end = len(series)
					}

					for _, s := range (series)[i:end] {
						protoLabels := make([]*querierpb.Label, len(s.Metric))
						for j, l := range s.Metric {
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

			} else if v1ResultData.ResultType == parser.ValueTypeVector {
				series := v1ResultData.Result.(promql.Vector)
				for i := 0; i < len(series); i += batchSize {
					end := i + batchSize
					if end > len(series) {
						end = len(series)
					}

					for _, s := range (series)[i:end] {
						protoLabels := make([]*querierpb.Label, len(s.Metric))
						for j, l := range s.Metric {
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
			}

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
		result, ok := s.queryResultCache.Get(*key)
		if !ok {
			return fmt.Errorf("fragment not found: %v", key)
		}

		switch result.Status {
		case StatusDone:
			batchSize := int(req.Batchsize)
			if batchSize <= 0 {
				batchSize = BATCHSIZE
			}

			fragmentResult := result.Data.(FragmentResult)
			v1ResultData := fragmentResult.Data.(*v1.QueryData)

			if v1ResultData.ResultType == parser.ValueTypeMatrix {
				matrix := v1ResultData.Result.(promql.Matrix)

				for i := 0; i < len(matrix); i += batchSize {
					end := i + batchSize
					if end > len(matrix) {
						end = len(matrix)
					}

					batch := &querierpb.StepVectorBatch{
						StepVectors: make([]*querierpb.StepVector, 0, end-i),
					}

					for _, v := range (matrix)[i:end] {
						var floats []float64
						var histograms []*histogram.FloatHistogram

						for _, f := range v.Floats {
							floats = append(floats, f.F)
						}

						for _, h := range v.Histograms {
							histograms = append(histograms, h.H)
						}

						protoVector := &querierpb.StepVector{
							T:          0,
							Samples:    floats,
							Histograms: FloatHistogramsToFloatHistogramProto(histograms),
						}
						batch.StepVectors = append(batch.StepVectors, protoVector)
					}
					if err := srv.Send(batch); err != nil {
						return err
					}
				}

			} else if v1ResultData.ResultType == parser.ValueTypeVector {
				vector := v1ResultData.Result.(promql.Vector)

				for i := 0; i < len(vector); i += batchSize {
					end := i + batchSize
					if end > len(vector) {
						end = len(vector)
					}

					batch := &querierpb.StepVectorBatch{
						StepVectors: make([]*querierpb.StepVector, 0, end-i),
					}

					for _, v := range (vector)[i:end] {
						protoVector := &querierpb.StepVector{
							T:          v.T,
							Samples:    []float64{v.F},
							Histograms: FloatHistogramsToFloatHistogramProto([]*histogram.FloatHistogram{v.H}),
						}
						batch.StepVectors = append(batch.StepVectors, protoVector)
					}

					if err := srv.Send(batch); err != nil {
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
