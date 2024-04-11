package ingester

import (
	"context"
	"crypto/rand"
	"fmt"
	"github.com/cespare/xxhash/v2"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/prometheus/prometheus/tsdb/fileutil"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/cortexproject/cortex/pkg/util/validation"
)

const (
	indexFilename                = "index"
	tmpForCreationBlockDirSuffix = ".tmp-for-creation"
)

type ExternalLabelCompactor struct {
	ctx       context.Context
	logger    log.Logger
	userID    string
	overrides *validation.Overrides

	chunkPool                chunkenc.Pool
	maxBlockChunkSegmentSize int64
	metrics                  *tsdb.CompactorMetrics
}

// Plan is a noop since we disable compaction in Ingester.
func (c *ExternalLabelCompactor) Plan(dir string) ([]string, error) {
	return nil, nil
}

// Compact is a noop since we disable compaction in Ingester.
func (c *ExternalLabelCompactor) Compact(dest string, dirs []string, open []*tsdb.Block) (ulid.ULID, error) {
	return ulid.ULID{}, nil
}

func (c *ExternalLabelCompactor) Write(dest string, b tsdb.BlockReader, mint, maxt int64, parent *tsdb.BlockMeta) ([]ulid.ULID, error) {
	shardBy := c.overrides.GetShardBy(c.userID)

	type outputBlock struct {
		postingFunc tsdb.IndexReaderPostingsFunc
		label       labels.Label
	}
	outputBlocks := []outputBlock{}
	// Fallback to the default compactor write.
	if len(shardBy.Limits) == 0 {
		outputBlocks = []outputBlock{
			{postingFunc: tsdb.AllSortedPostings},
		}
	} else {
		values := make([]string, 0, len(shardBy.Limits))
		for value := range shardBy.Limits {
			val := value
			values = append(values, val)
			pf := func(ctx context.Context, reader tsdb.IndexReader) index.Postings {
				postings, err := reader.Postings(ctx, shardBy.LabelName, val)
				if err != nil {
					return index.ErrPostings(err)
				}

				return reader.SortedPostings(postings)
			}
			outputBlocks = append(outputBlocks, outputBlock{
				postingFunc: pf,
				label:       labels.Label{Name: shardBy.LabelName, Value: val},
			})
		}
		rootPF := func(ctx context.Context, reader tsdb.IndexReader) index.Postings {
			k, v := index.AllPostingsKey()
			allPostings, err := reader.Postings(ctx, k, v)
			if err != nil {
				return index.ErrPostings(err)
			}
			valuesPostings, err := reader.Postings(ctx, shardBy.LabelName, values...)
			if err != nil {
				return index.ErrPostings(err)
			}

			return reader.SortedPostings(index.Without(allPostings, valuesPostings))
		}
		outputBlocks = append(outputBlocks, outputBlock{
			postingFunc: rootPF,
		})
	}

	uids := make([]ulid.ULID, 0, len(outputBlocks))
	for _, outputBlock := range outputBlocks {
		start := time.Now()

		uid := ulid.MustNew(ulid.Now(), rand.Reader)

		meta := &metadata.Meta{
			BlockMeta: tsdb.BlockMeta{
				Version: metadata.TSDBVersion1,
				ULID:    uid,
				MinTime: mint,
				MaxTime: maxt,
				Compaction: tsdb.BlockMetaCompaction{
					Level:   1,
					Sources: []ulid.ULID{uid},
				},
			},
		}
		if len(outputBlock.label.Name) > 0 {
			meta.BlockMeta.Compaction.Hints = []string{outputBlock.label.Name + "=" + outputBlock.label.Value}
			meta.Thanos.Labels = map[string]string{outputBlock.label.Name: outputBlock.label.Value}
		}

		if parent != nil {
			meta.Compaction.Parents = []tsdb.BlockDesc{
				{ULID: parent.ULID, MinTime: parent.MinTime, MaxTime: parent.MaxTime},
			}
		}

		err := write(c.ctx, c.metrics, c.logger, c.chunkPool, c.maxBlockChunkSegmentSize, dest, meta, tsdb.DefaultBlockPopulator{}, outputBlock.postingFunc, outputBlock.label, b)
		if err != nil {
			return []ulid.ULID{uid}, err
		}

		if meta.Stats.NumSamples == 0 {
			level.Info(c.logger).Log(
				"msg", "write block resulted in empty block",
				"mint", meta.MinTime,
				"maxt", meta.MaxTime,
				"duration", time.Since(start),
			)
			continue
		}

		uids = append(uids, uid)
		level.Info(c.logger).Log(
			"msg", "write block",
			"mint", meta.MinTime,
			"maxt", meta.MaxTime,
			"ulid", meta.ULID,
			"duration", time.Since(start),
		)
	}

	return uids, nil
}

// write creates a new block that is the union of the provided blocks into dir.
func write(ctx context.Context, metrics *tsdb.CompactorMetrics, logger log.Logger, chunkPool chunkenc.Pool, maxBlockChunkSegmentSize int64, dest string, meta *metadata.Meta, blockPopulator tsdb.BlockPopulator, postingsFunc tsdb.IndexReaderPostingsFunc, filterLabel labels.Label, blocks ...tsdb.BlockReader) (err error) {
	dir := filepath.Join(dest, meta.ULID.String())
	tmp := dir + tmpForCreationBlockDirSuffix
	var closers []io.Closer
	defer func(t time.Time) {
		err = tsdb_errors.NewMulti(err, tsdb_errors.CloseAll(closers)).Err()

		// RemoveAll returns no error when tmp doesn't exist so it is safe to always run it.
		if err := os.RemoveAll(tmp); err != nil {
			level.Error(logger).Log("msg", "removed tmp folder after failed compaction", "err", err.Error())
		}
		metrics.Ran.Inc()
		metrics.Duration.Observe(time.Since(t).Seconds())
	}(time.Now())

	if err = os.RemoveAll(tmp); err != nil {
		return err
	}

	if err = os.MkdirAll(tmp, 0o777); err != nil {
		return err
	}

	// Populate chunk and index files into temporary directory with
	// data of all blocks.
	var chunkw tsdb.ChunkWriter

	chunkw, err = chunks.NewWriterWithSegSize(chunkDir(tmp), maxBlockChunkSegmentSize)
	if err != nil {
		return fmt.Errorf("open chunk writer: %w", err)
	}
	closers = append(closers, chunkw)
	// Record written chunk sizes on level 1 compactions.
	if meta.Compaction.Level == 1 {
		chunkw = &instrumentedChunkWriter{
			ChunkWriter: chunkw,
			size:        metrics.ChunkSize,
			samples:     metrics.ChunkSamples,
			trange:      metrics.ChunkRange,
		}
	}

	indexw, err := index.NewWriterWithEncoder(ctx, filepath.Join(tmp, indexFilename), index.EncodePostingsRaw)
	if err != nil {
		return fmt.Errorf("open index writer: %w", err)
	}
	closers = append(closers, indexw)

	var indexWriter tsdb.IndexWriter
	indexWriter = indexw
	// Use FilteredIndexWriter to remove the external label from the series.
	if len(filterLabel.Name) > 0 {
		indexWriter = &FilteredIndexWriter{IndexWriter: indexw, name: filterLabel.Name, builder: labels.NewBuilder(labels.EmptyLabels())}
	}
	if err := blockPopulator.PopulateBlock(ctx, metrics, logger, chunkPool, storage.NewCompactingChunkSeriesMerger(storage.ChainedSeriesMerge), blocks, &meta.BlockMeta, indexWriter, chunkw, postingsFunc); err != nil {
		return fmt.Errorf("populate block: %w", err)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// We are explicitly closing them here to check for error even
	// though these are covered under defer. This is because in Windows,
	// you cannot delete these unless they are closed and the defer is to
	// make sure they are closed if the function exits due to an error above.
	errs := tsdb_errors.NewMulti()
	for _, w := range closers {
		errs.Add(w.Close())
	}
	closers = closers[:0] // Avoid closing the writers twice in the defer.
	if errs.Err() != nil {
		return errs.Err()
	}

	// Populated block is empty, so exit early.
	if meta.Stats.NumSamples == 0 {
		return nil
	}

	if err := meta.WriteToDir(logger, tmp); err != nil {
		return fmt.Errorf("write merged meta: %w", err)
	}

	df, err := fileutil.OpenDir(tmp)
	if err != nil {
		return fmt.Errorf("open temporary block dir: %w", err)
	}
	defer func() {
		if df != nil {
			df.Close()
		}
	}()

	if err := df.Sync(); err != nil {
		return fmt.Errorf("sync temporary dir file: %w", err)
	}

	// Close temp dir before rename block dir (for windows platform).
	if err = df.Close(); err != nil {
		return fmt.Errorf("close temporary dir: %w", err)
	}
	df = nil

	// Block successfully written, make it visible in destination dir by moving it from tmp one.
	if err := fileutil.Replace(tmp, dir); err != nil {
		return fmt.Errorf("rename block dir: %w", err)
	}

	return nil
}

func chunkDir(dir string) string { return filepath.Join(dir, "chunks") }

// instrumentedChunkWriter is used for level 1 compactions to record statistics
// about compacted chunks.
type instrumentedChunkWriter struct {
	tsdb.ChunkWriter

	size    prometheus.Histogram
	samples prometheus.Histogram
	trange  prometheus.Histogram
}

func (w *instrumentedChunkWriter) WriteChunks(chunks ...chunks.Meta) error {
	for _, c := range chunks {
		w.size.Observe(float64(len(c.Chunk.Bytes())))
		w.samples.Observe(float64(c.Chunk.NumSamples()))
		w.trange.Observe(float64(c.MaxTime - c.MinTime))
	}
	return w.ChunkWriter.WriteChunks(chunks...)
}

type FilteredIndexWriter struct {
	tsdb.IndexWriter
	name    string
	builder *labels.Builder
}

func (w *FilteredIndexWriter) AddSeries(ref storage.SeriesRef, l labels.Labels, chunks ...chunks.Meta) error {
	w.builder.Reset(l)
	w.builder.Del(w.name)
	return w.IndexWriter.AddSeries(ref, w.builder.Labels(), chunks...)
}

type ShardByMetricNameCompactor struct {
	ctx       context.Context
	logger    log.Logger
	userID    string
	overrides *validation.Overrides
	shards    int

	chunkPool                chunkenc.Pool
	maxBlockChunkSegmentSize int64
	metrics                  *tsdb.CompactorMetrics
}

// Plan is a noop since we disable compaction in Ingester.
func (c *ShardByMetricNameCompactor) Plan(dir string) ([]string, error) {
	return nil, nil
}

// Compact is a noop since we disable compaction in Ingester.
func (c *ShardByMetricNameCompactor) Compact(dest string, dirs []string, open []*tsdb.Block) (ulid.ULID, error) {
	return ulid.ULID{}, nil
}

func (c *ShardByMetricNameCompactor) Write(dest string, b tsdb.BlockReader, mint, maxt int64, parent *tsdb.BlockMeta) ([]ulid.ULID, error) {
	type outputBlock struct {
		postingFunc tsdb.IndexReaderPostingsFunc
		label       labels.Label
	}
	outputBlocks := []outputBlock{}
	for i := 0; i < c.shards; i++ {
		i := i
		pf := func(ctx context.Context, reader tsdb.IndexReader) index.Postings {
			k, v := index.AllPostingsKey()
			allPostings, err := reader.Postings(ctx, k, v)
			if err != nil {
				return index.ErrPostings(err)
			}
			var builder labels.ScratchBuilder
			var chks []chunks.Meta
			postings := []storage.SeriesRef{}
			for allPostings.Next() {
				ref := allPostings.At()
				if err := reader.Series(ref, &builder, &chks); err != nil {
					return index.ErrPostings(err)
				}
				fmt.Println(i)
				if int(hash(builder.Labels().Get(labels.MetricName)))%c.shards == i {
					postings = append(postings, ref)
				}
			}
			return reader.SortedPostings(index.NewListPostings(postings))
		}
		outputBlocks = append(outputBlocks, outputBlock{
			postingFunc: pf,
			label:       labels.Label{Name: "__shard_number__", Value: strconv.Itoa(i)},
		})
	}

	uids := make([]ulid.ULID, 0, len(outputBlocks))
	for _, outputBlock := range outputBlocks {
		start := time.Now()

		uid := ulid.MustNew(ulid.Now(), rand.Reader)

		meta := &metadata.Meta{
			BlockMeta: tsdb.BlockMeta{
				Version: metadata.TSDBVersion1,
				ULID:    uid,
				MinTime: mint,
				MaxTime: maxt,
				Compaction: tsdb.BlockMetaCompaction{
					Level:   1,
					Sources: []ulid.ULID{uid},
				},
			},
		}
		meta.BlockMeta.Compaction.Hints = []string{outputBlock.label.Name + "=" + outputBlock.label.Value}
		meta.Thanos.Labels = map[string]string{outputBlock.label.Name: outputBlock.label.Value}

		if parent != nil {
			meta.Compaction.Parents = []tsdb.BlockDesc{
				{ULID: parent.ULID, MinTime: parent.MinTime, MaxTime: parent.MaxTime},
			}
		}

		err := write(c.ctx, c.metrics, c.logger, c.chunkPool, c.maxBlockChunkSegmentSize, dest, meta, tsdb.DefaultBlockPopulator{}, outputBlock.postingFunc, outputBlock.label, b)
		if err != nil {
			return []ulid.ULID{uid}, err
		}

		if meta.Stats.NumSamples == 0 {
			level.Info(c.logger).Log(
				"msg", "write block resulted in empty block",
				"mint", meta.MinTime,
				"maxt", meta.MaxTime,
				"duration", time.Since(start),
			)
			continue
		}

		uids = append(uids, uid)
		level.Info(c.logger).Log(
			"msg", "write block",
			"mint", meta.MinTime,
			"maxt", meta.MaxTime,
			"ulid", meta.ULID,
			"duration", time.Since(start),
		)
	}

	return uids, nil
}

func hash(s string) uint64 {
	h := xxhash.New()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}
