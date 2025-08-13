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

		batchSize := int(req.Batchsize) // TODO: remove this
		if batchSize <= 0 {
			batchSize = BATCHSIZE
		}

		switch result.Status {
		case StatusDone:
			fragmentResult := result.Data.(FragmentResult)
			v1ResultData := fragmentResult.Data.(*v1.QueryData)

			switch v1ResultData.ResultType {
			case parser.ValueTypeMatrix:
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

			case parser.ValueTypeVector:
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

	batchSize := int(req.Batchsize)
	if batchSize <= 0 {
		batchSize = BATCHSIZE
	}

	for {
		result, ok := s.queryResultCache.Get(*key)
		if !ok {
			return fmt.Errorf("fragment not found: %v", key)
		}

		switch result.Status {
		case StatusDone:
			fragmentResult := result.Data.(FragmentResult)
			v1ResultData := fragmentResult.Data.(*v1.QueryData)

			switch v1ResultData.ResultType {
			case parser.ValueTypeMatrix:
				matrix := v1ResultData.Result.(promql.Matrix)

				numTimeSteps := matrix.TotalSamples()

				for timeStep := 0; timeStep < numTimeSteps; timeStep++ {

					for i, series := range matrix {
						batch := &querierpb.StepVectorBatch{
							StepVectors: make([]*querierpb.StepVector, 0, len(matrix)),
						}
						vector, err := s.createVectorForTimestep(&series, timeStep, uint64(i))
						if err != nil {
							return err
						}
						batch.StepVectors = append(batch.StepVectors, vector)
						if err := srv.Send(batch); err != nil {
							return fmt.Errorf("error sending batch: %w", err)
						}
					}

				}
			case parser.ValueTypeVector:
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

			default:
				return fmt.Errorf("unsupported result type: %v", v1ResultData.ResultType)
			}
		case StatusError:
			return fmt.Errorf("fragment processing failed")
		case StatusWriting:
			time.Sleep(WritingTimeout)
			continue
		}
	}
}

func (s *QuerierServer) createVectorForTimestep(series *promql.Series, timeStep int, sampleID uint64) (*querierpb.StepVector, error) {
	var samples []float64
	var sampleIDs []uint64
	var histograms []*histogram.FloatHistogram
	var histogramIDs []uint64
	var timestamp int64

	if timeStep < len(series.Floats) {
		point := series.Floats[timeStep]
		timestamp = point.T
		samples = append(samples, point.F)
		sampleIDs = append(sampleIDs, sampleID)
	}

	if timeStep < len(series.Histograms) {
		point := series.Histograms[timeStep]
		timestamp = point.T
		histograms = append(histograms, point.H)
		histogramIDs = append(histogramIDs, uint64(timeStep))
	}

	return &querierpb.StepVector{
		T:             timestamp,
		Sample_IDs:    sampleIDs,
		Samples:       samples,
		Histogram_IDs: histogramIDs,
		Histograms:    FloatHistogramsToFloatHistogramProto(histograms),
	}, nil
}

func FloatHistogramsToFloatHistogramProto(histograms []*histogram.FloatHistogram) []querierpb.Histogram {
	if histograms == nil {
		return []querierpb.Histogram{}
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
