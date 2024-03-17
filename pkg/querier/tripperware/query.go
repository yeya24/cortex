package tripperware

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	jsoniter "github.com/json-iterator/go"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/util/runutil"
)

var (
	json = jsoniter.Config{
		EscapeHTML:             false, // No HTML in our responses.
		SortMapKeys:            true,
		ValidateJsonRawMessage: false,
	}.Froze()
)

// Codec is used to encode/decode query range requests and responses so they can be passed down to middlewares.
type Codec interface {
	Merger
	// DecodeRequest decodes a Request from an http request.
	DecodeRequest(_ context.Context, request *http.Request, forwardHeaders []string) (Request, error)
	// DecodeResponse decodes a Response from an http response.
	// The original request is also passed as a parameter this is useful for implementation that needs the request
	// to merge result or build the result correctly.
	DecodeResponse(context.Context, *http.Response, Request) (Response, error)
	// EncodeRequest encodes a Request into an http request.
	EncodeRequest(context.Context, Request) (*http.Request, error)
	// EncodeResponse encodes a Response into an http response.
	EncodeResponse(context.Context, Response) (*http.Response, error)
}

// Merger is used by middlewares making multiple requests to merge back all responses into a single one.
type Merger interface {
	// MergeResponse merges responses from multiple requests into a single Response
	MergeResponse(context.Context, Request, ...Response) (Response, error)
}

// Response represents a query range response.
type Response interface {
	proto.Message
	// HTTPHeaders returns the HTTP headers in the response.
	HTTPHeaders() map[string][]string
}

// Request represents a query range request that can be process by middlewares.
type Request interface {
	// GetStart returns the start timestamp of the request in milliseconds.
	GetStart() int64
	// GetEnd returns the end timestamp of the request in milliseconds.
	GetEnd() int64
	// GetStep returns the step of the request in milliseconds.
	GetStep() int64
	// GetQuery returns the query of the request.
	GetQuery() string
	// WithStartEnd clone the current request with different start and end timestamp.
	WithStartEnd(startTime int64, endTime int64) Request
	// WithQuery clone the current request with a different query.
	WithQuery(string) Request
	proto.Message
	// LogToSpan writes information about this request to an OpenTracing span
	LogToSpan(opentracing.Span)
	// GetStats returns the stats of the request.
	GetStats() string
	// WithStats clones the current `PrometheusRequest` with a new stats.
	WithStats(stats string) Request
}

func decodeSampleStream(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
	lbls := labels.Labels{}
	samples := []cortexpb.Sample{}
	histograms := []SampleHistogramPair{}
	for field := iter.ReadObject(); field != ""; field = iter.ReadObject() {
		switch field {
		case "metric":
			iter.ReadVal(&lbls)
		case "values":
			for {
				if !iter.ReadArray() {
					break
				}
				s := cortexpb.Sample{}
				cortexpb.SampleJsoniterDecode(unsafe.Pointer(&s), iter)
				samples = append(samples, s)
			}
		case "histograms":
			for iter.ReadArray() {
				if !iter.ReadArray() {
					iter.ReportError("unmarshal model.SampleHistogramPair", "SampleHistogramPair must be [timestamp, {histogram}]")
					return
				}
				p := SampleHistogramPair{}
				p.Timestamp = int64(model.Time(iter.ReadFloat64() * float64(time.Second/time.Millisecond)))

				if !iter.ReadArray() {
					break
				}
				h := &model.SampleHistogram{}
				for key := iter.ReadObject(); key != ""; key = iter.ReadObject() {
					switch key {
					case "count":
						f, err := strconv.ParseFloat(iter.ReadString(), 64)
						if err != nil {
							iter.ReportError("unmarshal model.SampleHistogramPair", "count of histogram is not a float")
							return
						}
						h.Count = model.FloatString(f)
					case "sum":
						f, err := strconv.ParseFloat(iter.ReadString(), 64)
						if err != nil {
							iter.ReportError("unmarshal model.SampleHistogramPair", "sum of histogram is not a float")
							return
						}
						h.Sum = model.FloatString(f)
					case "buckets":
						for {
							if iter.ReadArray() {
								b, err := unmarshalHistogramBucket(iter)
								if err != nil {
									iter.ReportError("unmarshal model.HistogramBucket", err.Error())
									return
								}
								h.Buckets = append(h.Buckets, b)
							} else {
								fmt.Println("break out")
								break
							}
						}
					default:
						iter.ReportError("unmarshal model.SampleHistogramPair", fmt.Sprint("unexpected key in histogram:", key))
						return
					}
				}
				if iter.ReadArray() {
					iter.ReportError("unmarshal model.SampleHistogramPair", "SampleHistogramPair has too many values, must be [timestamp, {histogram}]")
					return
				}
				p.Histogram = modelSampleHistogramToSampleHistogram(h)
				histograms = append(histograms, p)
			}
		}
	}

	*(*SampleStream)(ptr) = SampleStream{
		Samples:    samples,
		Histograms: histograms,
		Labels:     cortexpb.FromLabelsToLabelAdapters(lbls),
	}
}

func encodeSampleStream(ptr unsafe.Pointer, stream *jsoniter.Stream) {
	ss := (*SampleStream)(ptr)
	stream.WriteObjectStart()

	stream.WriteObjectField(`metric`)
	lbls, err := cortexpb.FromLabelAdaptersToLabels(ss.Labels).MarshalJSON()
	if err != nil {
		stream.Error = err
		return
	}
	stream.SetBuffer(append(stream.Buffer(), lbls...))

	if len(ss.Samples) > 0 {
		stream.WriteMore()
		stream.WriteObjectField(`values`)
		stream.WriteArrayStart()
		for i, sample := range ss.Samples {
			if i != 0 {
				stream.WriteMore()
			}
			cortexpb.SampleJsoniterEncode(unsafe.Pointer(&sample), stream)
		}
		stream.WriteArrayEnd()
	}

	if len(ss.Histograms) > 0 {
		stream.WriteMore()
		stream.WriteObjectField(`histograms`)
		stream.WriteArrayStart()
		for i, h := range ss.Histograms {
			if i > 0 {
				stream.WriteMore()
			}
			marshalSampleHistogramPairJSON(unsafe.Pointer(&h), stream)
		}
		stream.WriteArrayEnd()
	}

	stream.WriteObjectEnd()
}

func marshalSampleHistogramPairJSON(ptr unsafe.Pointer, stream *jsoniter.Stream) {
	p := *((*SampleHistogramPair)(ptr))
	stream.WriteArrayStart()
	stream.WriteFloat64(float64(p.Timestamp) / float64(time.Second/time.Millisecond))
	stream.WriteMore()
	marshalHistogram(p.Histogram, stream)
	stream.WriteArrayEnd()
}

// marshalHistogramBucket writes something like: [ 3, "-0.25", "0.25", "3"]
// See marshalHistogram to understand what the numbers mean
func marshalHistogramBucket(b HistogramBucket, stream *jsoniter.Stream) {
	stream.WriteArrayStart()
	stream.WriteInt32(b.Boundaries)
	stream.WriteMore()
	marshalFloat(b.Lower, stream)
	stream.WriteMore()
	marshalFloat(b.Upper, stream)
	stream.WriteMore()
	marshalFloat(b.Count, stream)
	stream.WriteArrayEnd()
}

// marshalHistogram writes something like:
//
//	{
//	    "count": "42",
//	    "sum": "34593.34",
//	    "buckets": [
//	      [ 3, "-0.25", "0.25", "3"],
//	      [ 0, "0.25", "0.5", "12"],
//	      [ 0, "0.5", "1", "21"],
//	      [ 0, "2", "4", "6"]
//	    ]
//	}
//
// The 1st element in each bucket array determines if the boundaries are
// inclusive (AKA closed) or exclusive (AKA open):
//
//	0: lower exclusive, upper inclusive
//	1: lower inclusive, upper exclusive
//	2: both exclusive
//	3: both inclusive
//
// The 2nd and 3rd elements are the lower and upper boundary. The 4th element is
// the bucket count.
func marshalHistogram(h SampleHistogram, stream *jsoniter.Stream) {
	stream.WriteObjectStart()
	stream.WriteObjectField(`count`)
	marshalFloat(h.Count, stream)
	stream.WriteMore()
	stream.WriteObjectField(`sum`)
	marshalFloat(h.Sum, stream)

	bucketFound := false
	for _, bucket := range h.Buckets {
		if bucket.Count == 0 {
			continue // No need to expose empty buckets in JSON.
		}
		stream.WriteMore()
		if !bucketFound {
			stream.WriteObjectField(`buckets`)
			stream.WriteArrayStart()
		}
		bucketFound = true
		marshalHistogramBucket(*bucket, stream)
	}
	if bucketFound {
		stream.WriteArrayEnd()
	}
	stream.WriteObjectEnd()
}

func PrometheusResponseQueryableSamplesStatsPerStepJsoniterDecode(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
	if !iter.ReadArray() {
		iter.ReportError("tripperware.PrometheusResponseQueryableSamplesStatsPerStep", "expected [")
		return
	}

	t := model.Time(iter.ReadFloat64() * float64(time.Second/time.Millisecond))

	if !iter.ReadArray() {
		iter.ReportError("tripperware.PrometheusResponseQueryableSamplesStatsPerStep", "expected ,")
		return
	}
	v := iter.ReadInt64()

	if iter.ReadArray() {
		iter.ReportError("tripperware.PrometheusResponseQueryableSamplesStatsPerStep", "expected ]")
	}

	*(*PrometheusResponseQueryableSamplesStatsPerStep)(ptr) = PrometheusResponseQueryableSamplesStatsPerStep{
		TimestampMs: int64(t),
		Value:       v,
	}
}

func PrometheusResponseQueryableSamplesStatsPerStepJsoniterEncode(ptr unsafe.Pointer, stream *jsoniter.Stream) {
	stats := (*PrometheusResponseQueryableSamplesStatsPerStep)(ptr)
	stream.WriteArrayStart()
	stream.WriteFloat64(float64(stats.TimestampMs) / float64(time.Second/time.Millisecond))
	stream.WriteMore()
	stream.WriteInt64(stats.Value)
	stream.WriteArrayEnd()
}

func init() {
	jsoniter.RegisterTypeEncoderFunc("tripperware.PrometheusResponseQueryableSamplesStatsPerStep", PrometheusResponseQueryableSamplesStatsPerStepJsoniterEncode, func(unsafe.Pointer) bool { return false })
	jsoniter.RegisterTypeDecoderFunc("tripperware.PrometheusResponseQueryableSamplesStatsPerStep", PrometheusResponseQueryableSamplesStatsPerStepJsoniterDecode)
	jsoniter.RegisterTypeEncoderFunc("tripperware.SampleStream", encodeSampleStream, func(unsafe.Pointer) bool { return false })
	jsoniter.RegisterTypeDecoderFunc("tripperware.SampleStream", decodeSampleStream)
}

func EncodeTime(t int64) string {
	f := float64(t) / 1.0e3
	return strconv.FormatFloat(f, 'f', -1, 64)
}

// Buffer can be used to read a response body.
// This allows to avoid reading the body multiple times from the `http.Response.Body`.
type Buffer interface {
	Bytes() []byte
}

func BodyBuffer(res *http.Response, logger log.Logger) ([]byte, error) {
	var buf *bytes.Buffer

	// Attempt to cast the response body to a Buffer and use it if possible.
	// This is because the frontend may have already read the body and buffered it.
	if buffer, ok := res.Body.(Buffer); ok {
		buf = bytes.NewBuffer(buffer.Bytes())
	} else {
		// Preallocate the buffer with the exact size so we don't waste allocations
		// while progressively growing an initial small buffer. The buffer capacity
		// is increased by MinRead to avoid extra allocations due to how ReadFrom()
		// internally works.
		buf = bytes.NewBuffer(make([]byte, 0, res.ContentLength+bytes.MinRead))
		if _, err := buf.ReadFrom(res.Body); err != nil {
			return nil, httpgrpc.Errorf(http.StatusInternalServerError, "error decoding response: %v", err)
		}
	}

	// if the response is gzipped, lets unzip it here
	if strings.EqualFold(res.Header.Get("Content-Encoding"), "gzip") {
		gReader, err := gzip.NewReader(buf)
		if err != nil {
			return nil, err
		}
		defer runutil.CloseWithLogOnErr(logger, gReader, "close gzip reader")

		return io.ReadAll(gReader)
	}

	return buf.Bytes(), nil
}

func BodyBufferFromHTTPGRPCResponse(res *httpgrpc.HTTPResponse, logger log.Logger) ([]byte, error) {
	// if the response is gzipped, lets unzip it here
	headers := http.Header{}
	for _, h := range res.Headers {
		headers[h.Key] = h.Values
	}
	if strings.EqualFold(headers.Get("Content-Encoding"), "gzip") {
		gReader, err := gzip.NewReader(bytes.NewBuffer(res.Body))
		if err != nil {
			return nil, err
		}
		defer runutil.CloseWithLogOnErr(logger, gReader, "close gzip reader")

		return io.ReadAll(gReader)
	}

	return res.Body, nil
}

func StatsMerge(stats map[int64]*PrometheusResponseQueryableSamplesStatsPerStep) *PrometheusResponseStats {
	keys := make([]int64, 0, len(stats))
	for key := range stats {
		keys = append(keys, key)
	}

	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	result := &PrometheusResponseStats{Samples: &PrometheusResponseSamplesStats{}}
	for _, key := range keys {
		result.Samples.TotalQueryableSamplesPerStep = append(result.Samples.TotalQueryableSamplesPerStep, stats[key])
		result.Samples.TotalQueryableSamples += stats[key].Value
	}

	return result
}

func unmarshalHistogramBucket(iter *jsoniter.Iterator) (*model.HistogramBucket, error) {
	b := model.HistogramBucket{}
	if !iter.ReadArray() {
		return nil, errors.New("HistogramBucket must be [boundaries, lower, upper, count]")
	}
	boundaries, err := iter.ReadNumber().Int64()
	if err != nil {
		return nil, err
	}
	b.Boundaries = int32(boundaries)
	if !iter.ReadArray() {
		return nil, errors.New("HistogramBucket must be [boundaries, lower, upper, count]")
	}
	f, err := strconv.ParseFloat(iter.ReadString(), 64)
	if err != nil {
		return nil, err
	}
	b.Lower = model.FloatString(f)
	if !iter.ReadArray() {
		return nil, errors.New("HistogramBucket must be [boundaries, lower, upper, count]")
	}
	f, err = strconv.ParseFloat(iter.ReadString(), 64)
	if err != nil {
		return nil, err
	}
	b.Upper = model.FloatString(f)
	if !iter.ReadArray() {
		return nil, errors.New("HistogramBucket must be [boundaries, lower, upper, count]")
	}
	f, err = strconv.ParseFloat(iter.ReadString(), 64)
	if err != nil {
		return nil, err
	}
	b.Count = model.FloatString(f)
	if iter.ReadArray() {
		return nil, errors.New("HistogramBucket has too many values, must be [boundaries, lower, upper, count]")
	}
	return &b, nil
}

func modelSampleHistogramToSampleHistogram(h *model.SampleHistogram) SampleHistogram {
	res := &SampleHistogram{
		Count:   float64(h.Count),
		Sum:     float64(h.Sum),
		Buckets: make([]*HistogramBucket, len(h.Buckets)),
	}
	for i, bucket := range h.Buckets {
		res.Buckets[i] = &HistogramBucket{
			Boundaries: bucket.Boundaries,
			Lower:      float64(bucket.Lower),
			Upper:      float64(bucket.Upper),
			Count:      float64(bucket.Count),
		}
	}
	return *res
}

func marshalFloat(v float64, stream *jsoniter.Stream) {
	stream.WriteRaw(`"`)
	// Taken from https://github.com/json-iterator/go/blob/master/stream_float.go#L71 as a workaround
	// to https://github.com/json-iterator/go/issues/365 (json-iterator, to follow json standard, doesn't allow inf/nan).
	buf := stream.Buffer()
	abs := math.Abs(v)
	fmt := byte('f')
	// Note: Must use float32 comparisons for underlying float32 value to get precise cutoffs right.
	if abs != 0 {
		if abs < 1e-6 || abs >= 1e21 {
			fmt = 'e'
		}
	}
	buf = strconv.AppendFloat(buf, v, fmt, -1, 64)
	stream.SetBuffer(buf)
	stream.WriteRaw(`"`)
}
