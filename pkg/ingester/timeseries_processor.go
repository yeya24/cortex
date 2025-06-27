package ingester

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/storage/iceberg"
)

// WriteRequest represents a timeseries write request with acknowledgment
type WriteRequest struct {
	UserID     string
	Request    *cortexpb.WriteRequest
	ResponseCh chan WriteResponse
}

// WriteResponse represents the response to a write request
type WriteResponse struct {
	Success bool
	Error   error
	Count   int // Number of samples written
}

// TimeseriesProcessor handles buffered writes to Iceberg storage
type TimeseriesProcessor struct {
	logger log.Logger
	store  *iceberg.IcebergStore

	// Parquet converter for timeseries conversion
	converter *iceberg.TimeseriesParquetConverter

	// Per-user channels and goroutines
	userChannels map[string]chan WriteRequest
	userWg       sync.WaitGroup
	userMu       sync.RWMutex

	// Buffer configuration
	bufferSize    int
	flushInterval time.Duration

	// Metrics
	metrics *TimeseriesProcessorMetrics

	// Context for shutdown
	ctx    context.Context
	cancel context.CancelFunc
}

// TimeseriesProcessorMetrics holds metrics for the processor
type TimeseriesProcessorMetrics struct {
	requestsReceived   prometheus.Counter
	requestsProcessed  prometheus.Counter
	requestsFailed     prometheus.Counter
	bufferFlushes      prometheus.Counter
	samplesWritten     prometheus.Counter
	processingDuration prometheus.Histogram
	bufferSize         prometheus.Gauge
	activeUsers        prometheus.Gauge
}

// NewTimeseriesProcessorMetrics creates new metrics
func NewTimeseriesProcessorMetrics(reg prometheus.Registerer) *TimeseriesProcessorMetrics {
	return &TimeseriesProcessorMetrics{
		requestsReceived: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "iceberg_timeseries_requests_received_total",
			Help: "Total number of timeseries write requests received",
		}),
		requestsProcessed: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "iceberg_timeseries_requests_processed_total",
			Help: "Total number of timeseries write requests processed",
		}),
		requestsFailed: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "iceberg_timeseries_requests_failed_total",
			Help: "Total number of timeseries write requests that failed",
		}),
		bufferFlushes: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "iceberg_timeseries_buffer_flushes_total",
			Help: "Total number of buffer flushes to Iceberg",
		}),
		samplesWritten: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "iceberg_timeseries_samples_written_total",
			Help: "Total number of samples written to Iceberg",
		}),
		processingDuration: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "iceberg_timeseries_processing_duration_seconds",
			Help:    "Time spent processing timeseries requests",
			Buckets: prometheus.DefBuckets,
		}),
		bufferSize: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "iceberg_timeseries_buffer_size",
			Help: "Current size of the timeseries buffer",
		}),
		activeUsers: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "iceberg_timeseries_active_users",
			Help: "Number of active users with pending requests",
		}),
	}
}

// NewTimeseriesProcessor creates a new timeseries processor
func NewTimeseriesProcessor(store *iceberg.IcebergStore, logger log.Logger, reg prometheus.Registerer) *TimeseriesProcessor {
	ctx, cancel := context.WithCancel(context.Background())

	return &TimeseriesProcessor{
		logger:        logger,
		store:         store,
		converter:     iceberg.NewTimeseriesParquetConverter(),
		userChannels:  make(map[string]chan WriteRequest),
		bufferSize:    1000, // Default buffer size
		flushInterval: time.Second,
		metrics:       NewTimeseriesProcessorMetrics(reg),
		ctx:           ctx,
		cancel:        cancel,
	}
}

// SetBufferConfig sets the buffer configuration
func (tp *TimeseriesProcessor) SetBufferConfig(bufferSize int, flushInterval time.Duration) {
	tp.bufferSize = bufferSize
	tp.flushInterval = flushInterval
}

// ProcessWriteRequest processes a write request for a user
func (tp *TimeseriesProcessor) ProcessWriteRequest(ctx context.Context, userID string, req *cortexpb.WriteRequest) error {
	tp.metrics.requestsReceived.Inc()

	// Create response channel
	responseCh := make(chan WriteResponse, 1)

	// Create write request
	writeReq := WriteRequest{
		UserID:     userID,
		Request:    req,
		ResponseCh: responseCh,
	}

	// Get or create user channel
	userCh := tp.getOrCreateUserChannel(userID)

	// Send request to user channel (non-blocking)
	select {
	case userCh <- writeReq:
		// Request sent successfully
	case <-ctx.Done():
		return ctx.Err()
	default:
		// Channel is full, return error
		tp.metrics.requestsFailed.Inc()
		return fmt.Errorf("user channel full for user %s", userID)
	}

	// Wait for acknowledgment
	select {
	case response := <-responseCh:
		if !response.Success {
			tp.metrics.requestsFailed.Inc()
			return response.Error
		}
		tp.metrics.requestsProcessed.Inc()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// getOrCreateUserChannel gets or creates a channel for a user
func (tp *TimeseriesProcessor) getOrCreateUserChannel(userID string) chan WriteRequest {
	tp.userMu.RLock()
	if ch, exists := tp.userChannels[userID]; exists {
		tp.userMu.RUnlock()
		return ch
	}
	tp.userMu.RUnlock()

	// Create new channel and goroutine
	tp.userMu.Lock()
	defer tp.userMu.Unlock()

	// Double-check after acquiring write lock
	if ch, exists := tp.userChannels[userID]; exists {
		return ch
	}

	// Create new channel
	ch := make(chan WriteRequest, tp.bufferSize)
	tp.userChannels[userID] = ch

	// Start goroutine for this user
	tp.userWg.Add(1)
	go tp.userProcessor(userID, ch)

	tp.metrics.activeUsers.Inc()
	level.Info(tp.logger).Log("msg", "created user processor", "user", userID)

	return ch
}

// userProcessor handles timeseries processing for a specific user
func (tp *TimeseriesProcessor) userProcessor(userID string, ch chan WriteRequest) {
	defer func() {
		tp.userWg.Done()
		tp.metrics.activeUsers.Dec()
		level.Info(tp.logger).Log("msg", "user processor stopped", "user", userID)
	}()

	// Buffer for collecting requests
	var buffer []WriteRequest
	ticker := time.NewTicker(tp.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case req := <-ch:
			// Add request to buffer
			buffer = append(buffer, req)
			tp.metrics.bufferSize.Set(float64(len(buffer)))

			// Flush if buffer is full
			if len(buffer) >= tp.bufferSize {
				tp.flushBuffer(userID, buffer)
				buffer = buffer[:0] // Reset buffer
				tp.metrics.bufferSize.Set(0)
			}

		case <-ticker.C:
			// Flush buffer on timer
			if len(buffer) > 0 {
				tp.flushBuffer(userID, buffer)
				buffer = buffer[:0] // Reset buffer
				tp.metrics.bufferSize.Set(0)
			}

		case <-tp.ctx.Done():
			// Flush remaining buffer on shutdown
			if len(buffer) > 0 {
				tp.flushBuffer(userID, buffer)
			}
			return
		}
	}
}

// flushBuffer flushes the buffer to Iceberg storage
func (tp *TimeseriesProcessor) flushBuffer(userID string, buffer []WriteRequest) {
	if len(buffer) == 0 {
		return
	}

	start := time.Now()
	defer func() {
		tp.metrics.processingDuration.Observe(time.Since(start).Seconds())
	}()

	// Combine all requests into a single write request
	var combinedSamples int
	var allTimeseries []cortexpb.PreallocTimeseries

	for _, req := range buffer {
		if req.Request != nil {
			allTimeseries = append(allTimeseries, req.Request.Timeseries...)
			for _, ts := range req.Request.Timeseries {
				combinedSamples += len(ts.Samples)
			}
		}
	}

	if len(allTimeseries) == 0 {
		// Send success responses for empty requests
		for _, req := range buffer {
			req.ResponseCh <- WriteResponse{Success: true, Count: 0}
		}
		return
	}

	// Create combined request
	combinedReq := &cortexpb.WriteRequest{
		Timeseries: allTimeseries,
	}

	// Write to user-specific table using IcebergStore
	err := tp.writeToIceberg(userID, combinedReq)
	if err != nil {
		// Send error responses
		for _, req := range buffer {
			req.ResponseCh <- WriteResponse{
				Success: false,
				Error:   fmt.Errorf("failed to write to Iceberg: %w", err),
			}
		}
		return
	}

	// Send success responses
	for _, req := range buffer {
		req.ResponseCh <- WriteResponse{
			Success: true,
			Count:   combinedSamples,
		}
	}

	tp.metrics.bufferFlushes.Inc()
	tp.metrics.samplesWritten.Add(float64(combinedSamples))

	level.Debug(tp.logger).Log(
		"msg", "flushed buffer to Iceberg",
		"user", userID,
		"table", userID,
		"requests", len(buffer),
		"samples", combinedSamples,
		"duration", time.Since(start),
	)
}

// writeToIceberg writes the timeseries data to user-specific Iceberg table
func (tp *TimeseriesProcessor) writeToIceberg(userID string, req *cortexpb.WriteRequest) error {
	// Use userID as table name
	tableName := userID

	// Create a pipe to stream the parquet data
	pr, pw := io.Pipe()

	// Start conversion in a goroutine
	go func() {
		defer pw.Close()
		if err := tp.converter.ConvertTimeseriesToParquet(tp.ctx, req, pw); err != nil {
			pw.CloseWithError(fmt.Errorf("failed to convert to parquet: %w", err))
		}
	}()

	// Upload the parquet data to the table
	if err := tp.store.Upload(tp.ctx, tableName, pr); err != nil {
		return fmt.Errorf("failed to upload parquet data to table %s: %w", tableName, err)
	}

	return nil
}

// Shutdown gracefully shuts down the processor
func (tp *TimeseriesProcessor) Shutdown(ctx context.Context) error {
	level.Info(tp.logger).Log("msg", "shutting down timeseries processor")

	// Cancel context to stop all goroutines
	tp.cancel()

	// Close all user channels
	tp.userMu.Lock()
	for userID, ch := range tp.userChannels {
		close(ch)
		delete(tp.userChannels, userID)
	}
	tp.userMu.Unlock()

	// Wait for all goroutines to finish
	done := make(chan struct{})
	go func() {
		tp.userWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		level.Info(tp.logger).Log("msg", "timeseries processor shutdown complete")
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// GetActiveUsers returns the number of active users
func (tp *TimeseriesProcessor) GetActiveUsers() int {
	tp.userMu.RLock()
	defer tp.userMu.RUnlock()
	return len(tp.userChannels)
}

// GetBufferSize returns the current buffer size for a user
func (tp *TimeseriesProcessor) GetBufferSize(userID string) int {
	tp.userMu.RLock()
	defer tp.userMu.RUnlock()
	if ch, exists := tp.userChannels[userID]; exists {
		return len(ch)
	}
	return 0
}
