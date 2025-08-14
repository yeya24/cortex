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

		switch result.Status {
		case StatusDone:
			fragmentResult := result.Data.(FragmentResult)
			v1ResultData := fragmentResult.Data.(*v1.QueryData)

			switch v1ResultData.ResultType {
			case parser.ValueTypeMatrix:
				series := v1ResultData.Result.(promql.Matrix)

				seriesBatch := []*querierpb.OneSeries{}
				for _, s := range series {
					oneSeries := querierpb.OneSeries{}
					for j, l := range s.Metric {
						oneSeries.Labels[j] = &querierpb.Label{
							Name:  l.Name,
							Value: l.Value}
					}
					seriesBatch = append(seriesBatch, &oneSeries)
				}
				if err := srv.Send(&querierpb.SeriesBatch{
					OneSeries: seriesBatch}); err != nil {
					return err
				}

				return nil

			case parser.ValueTypeVector:
				samples := v1ResultData.Result.(promql.Vector)

				seriesBatch := []*querierpb.OneSeries{}
				for _, s := range samples {
					oneSeries := querierpb.OneSeries{}
					for j, l := range s.Metric {
						oneSeries.Labels[j] = &querierpb.Label{
							Name:  l.Name,
							Value: l.Value,
						}
					}
					seriesBatch = append(seriesBatch, &oneSeries)
				}
				if err := srv.Send(&querierpb.SeriesBatch{
					OneSeries: seriesBatch,
				}); err != nil {
					return err
				}
				return nil
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

				for timeStep := 0; timeStep < numTimeSteps; timeStep += batchSize {
					batch := &querierpb.StepVectorBatch{
						StepVectors: make([]*querierpb.StepVector, 0, len(matrix)),
					}
					for t := 0; t < batchSize; t++ {
						for i, series := range matrix {
							vector, err := s.createVectorForTimestep(&series, timeStep+t, uint64(i))
							if err != nil {
								return err
							}
							batch.StepVectors = append(batch.StepVectors, vector)
						}
					}
					if err := srv.Send(batch); err != nil {
						return fmt.Errorf("error sending batch: %w", err)
					}
				}
				return nil

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

					//sampleIDs := make([]uint64, 0, batchSize)
					//samples := make([]float64, 0, batchSize)
					//histogramIDs := make([]uint64, 0, batchSize)
					//histograms := make([]*querierpb.Histogram, 0, batchSize)

					for j, sample := range (vector)[i:end] {
						vec := &querierpb.StepVector{}

						if sample.H == nil {
							vec = &querierpb.StepVector{
								T:             sample.T,            // all samples have the same timestamp
								Sample_IDs:    []uint64{uint64(j)}, // only one sample
								Samples:       []float64{sample.F},
								Histogram_IDs: []uint64{},
								Histograms:    FloatHistogramsToFloatHistogramProto([]*histogram.FloatHistogram{sample.H}),
							}
						} else {
							vec = &querierpb.StepVector{
								T:             sample.T,
								Sample_IDs:    []uint64{},
								Samples:       []float64{sample.F},
								Histogram_IDs: []uint64{uint64(j)},
								Histograms:    FloatHistogramsToFloatHistogramProto([]*histogram.FloatHistogram{sample.H}),
							}
						}
						batch.StepVectors = append(batch.StepVectors, vec)
					}

					if err := srv.Send(batch); err != nil {
						return err
					}
				}
				return nil

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
