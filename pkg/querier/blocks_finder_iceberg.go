package querier

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	parquet_schema "github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/objstore"
	"golang.org/x/sync/errgroup"

	"github.com/cortexproject/cortex/pkg/cygnus"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
	"github.com/cortexproject/cortex/pkg/storage/parquet"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/bucketindex"
	"github.com/cortexproject/cortex/pkg/util/services"
)

type IcebergBlocksFinderConfig struct {
	IndexLoader              bucketindex.LoaderConfig
	MaxStalePeriod           time.Duration
	IgnoreDeletionMarksDelay time.Duration
	IgnoreBlocksWithin       time.Duration
}

// BucketIndexBlocksFinder implements BlocksFinder interface and find blocks in the bucket
// looking up the bucket index.
type IcebergBlocksFinder struct {
	services.Service

	cfg     BucketIndexBlocksFinderConfig
	catalog table.CatalogIO
}

func NewIcebergBlocksFinder(cfg BucketIndexBlocksFinderConfig, bkt objstore.Bucket, cfgProvider bucket.TenantConfigProvider, logger log.Logger, reg prometheus.Registerer) *IcebergBlocksFinder {
	catalog := cygnus.NewCatalog(ddb, tableName)
	return &IcebergBlocksFinder{
		cfg:     cfg,
		catalog: catalog,
	}
}

// GetBlocks implements BlocksFinder.
func (r *IcebergBlocksFinder) GetBlocks(ctx context.Context, userID string, minT, maxT int64, matchers []*labels.Matcher) (bucketindex.Blocks, map[ulid.ULID]*bucketindex.BlockDeletionMark, error) {
	if r.State() != services.Running {
		return nil, nil, errBucketIndexBlocksFinderNotRunning
	}
	if maxT < minT {
		return nil, nil, errInvalidBlocksRange
	}

	table, err := r.catalog.LoadTable(ctx, table.Identifier([]string{userID, "blocks"}), nil)
	if err != nil {
		return nil, nil, err
	}

	snapshot := table.CurrentSnapshot()
	schema := table.Schema()
	if snapshot == nil {
		return nil, nil, fmt.Errorf("no current snapshot for iceberg table")
	}

	fs, err := iceio.LoadFS(ctx, nil, table.MetadataLocation())
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load file system for table: %w", err)
	}

	// Get manifests from snapshot
	mf, err := snapshot.Manifests(fs)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get manifests for table: %w", err)
	}

	eg, ctx := errgroup.WithContext(ctx)
	results := make([]*bucketindex.Block, 0)
	var resultsMtx sync.Mutex
	for _, manifest := range mf {
		eg.Go(func() error {
			entries, err := manifest.FetchEntries(fs, true)
			if err != nil {
				return fmt.Errorf("failed to fetch entries from manifest: %w", err)
			}

			for _, entry := range entries {
				if !dataFileMaybeUseful(schema, entry.DataFile(), minT, maxT, matchers) {
					continue
				}
				resultsMtx.Lock()
				results = append(results, &bucketindex.Block{
					Parquet: &parquet.ConverterMarkMeta{},
					// TODO: fix it
					ID: entry.DataFile().FilePath(),
				})
				resultsMtx.Unlock()
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, nil, err
	}

	return results, nil, nil
}

// dataFileMaybeUseful checks if the data file contains timestamps within the specified range
func dataFileMaybeUseful(schema *iceberg.Schema, dataFile iceberg.DataFile, mint, maxt int64, matchers []*labels.Matcher) bool {
	// Get the lower and upper bounds from the data file
	lowerBounds := dataFile.LowerBoundValues()
	upperBounds := dataFile.UpperBoundValues()

	timeField, ok := schema.FindFieldByName("timestamp")
	if !ok {
		return true
	}
	timestampColID := timeField.ID

	// Check if the timestamp range overlaps with the query range
	if lowerBounds != nil {
		if lowerBoundBytes, exists := lowerBounds[timestampColID]; exists && len(lowerBoundBytes) > 0 {
			// Convert bytes to int64 (Iceberg uses big-endian encoding)
			if len(lowerBoundBytes) >= 8 {
				lowerBound := int64(binary.LittleEndian.Uint64(lowerBoundBytes))

				if lowerBound > maxt {
					return false // File starts after query end
				}
			}
		}

		for _, matcher := range matchers {
			if matcher.Type == labels.MatchEqual {
				field, ok := schema.FindFieldByName(parquet_schema.LabelToColumn(matcher.Name))
				if !ok && matcher.Value != "" {
					return false
				}
				if lowerBoundBytes, exists := lowerBounds[field.ID]; exists && len(lowerBoundBytes) > 0 {
					val := util.YoloString(lowerBoundBytes)
					if val > matcher.Value {
						return false
					}
				}
			}
		}
	}

	if upperBounds != nil {
		if upperBoundBytes, exists := upperBounds[timestampColID]; exists && len(upperBoundBytes) > 0 {
			// Convert bytes to int64 (Iceberg uses big-endian encoding)
			if len(upperBoundBytes) >= 8 {
				upperBound := int64(binary.LittleEndian.Uint64(upperBoundBytes))

				if upperBound < mint {
					return false // File ends before query start
				}
			}
		}

		for _, matcher := range matchers {
			if matcher.Type == labels.MatchEqual {
				field, ok := schema.FindFieldByName(parquet_schema.LabelToColumn(matcher.Name))
				if !ok && matcher.Value != "" {
					return false
				}
				if upperBoundBytes, exists := upperBounds[field.ID]; exists && len(upperBoundBytes) > 0 {
					val := util.YoloString(upperBoundBytes)
					if val < matcher.Value {
						return false
					}
				}
			}
		}
	}

	return true // File overlaps with query range and metric name.
}
