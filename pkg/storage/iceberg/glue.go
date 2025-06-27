package iceberg

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/url"
	"sync"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog/rest"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/thanos-io/objstore"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/glue"
	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/cortexproject/cortex/pkg/storage/bucket"
	"github.com/cortexproject/cortex/pkg/storage/bucket/s3"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"golang.org/x/sync/errgroup"
)

type IcebergStore struct {
	cfg       IcebergConfig
	Catalog   catalog.Catalog
	Namespace string
	// Default properties for table operations
	DefaultProperties map[string]string
	//bucket  objstore.Bucket

	bkts map[string]objstore.Bucket
	mu   sync.RWMutex

	cache cacheInterface[*ParquetShard]
}

// IcebergConfig holds configuration for AWS Glue catalog
type IcebergConfig struct {
	Backend   string     `yaml:"backend"`
	Namespace string     `yaml:"namespace"`
	DataDir   string     `yaml:"data_dir"`
	Rest      RestConfig `yaml:"rest,omitempty"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *IcebergConfig) RegisterFlags(f *flag.FlagSet, prefix string) {
	f.StringVar(&cfg.Backend, prefix+".backend", "glue", "Iceberg catalog backend (glue, rest)")
	f.StringVar(&cfg.Namespace, prefix+".namespace", "cortex", "Iceberg namespace for tables")
	f.StringVar(&cfg.DataDir, prefix+".data-dir", "./iceberg-data", "Directory to store parquet files before upload")
	cfg.Rest.RegisterFlags(f, prefix+".rest")
}

type RestConfig struct {
	URI               string `yaml:"uri"`
	WarehouseLocation string `yaml:"warehouse_location"`
	Region            string `yaml:"region"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *RestConfig) RegisterFlags(f *flag.FlagSet, prefix string) {
	f.StringVar(&cfg.URI, prefix+".uri", "", "Iceberg REST catalog URI")
	f.StringVar(&cfg.WarehouseLocation, prefix+".warehouse-location", "", "Iceberg warehouse location")
	f.StringVar(&cfg.Region, prefix+".region", "", "AWS region for REST catalog")
}

// NewIcebergGlue creates a new Iceberg catalog using AWS Glue as the catalog provider
func NewIcebergStore(cfg IcebergConfig) (*IcebergStore, error) {
	var err error
	awsCfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}
	var (
		ctlg catalog.Catalog
	)
	switch cfg.Backend {
	case "glue":
		glueCfg := glue.WithAwsConfig(awsCfg)
		ctlg = glue.NewCatalog(glueCfg)
	case "rest":
		ctlg, err = rest.NewCatalog(context.TODO(), "rest", cfg.Rest.URI,
			rest.WithAwsConfig(awsCfg),
			rest.WithSigV4(),
			rest.WithSigV4RegionSvc(cfg.Rest.Region, "s3tables"),
			rest.WithWarehouseLocation(cfg.Rest.WarehouseLocation),
		)
	}
	if err != nil {
		return nil, err
	}

	// Set default properties if not provided
	defaultProps := map[string]string{
		"write.format.default":            "parquet",
		"write.parquet.compression-codec": "zstd",
	}

	store := &IcebergStore{
		Catalog:           ctlg,
		Namespace:         cfg.Namespace,
		DefaultProperties: defaultProps,
		bkts:              make(map[string]objstore.Bucket),
		cfg:               cfg,
		cache:             newCache[*ParquetShard]("a", 1024),
		//bucket:  bucket,
	}

	// Create namespace if it doesn't exist
	if err := store.ensureNamespace(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to ensure namespace exists: %w", err)
	}

	return store, nil
}

// ensureNamespace creates the namespace if it doesn't exist
func (ig *IcebergStore) ensureNamespace(ctx context.Context) error {
	if ig.Namespace == "" {
		return fmt.Errorf("namespace is required")
	}

	// Check if namespace exists
	exists, err := ig.Catalog.CheckNamespaceExists(ctx, table.Identifier{ig.Namespace})
	if err != nil {
		return fmt.Errorf("failed to check namespace existence: %w", err)
	}

	if !exists {
		// Create namespace
		err = ig.Catalog.CreateNamespace(ctx, table.Identifier{ig.Namespace}, iceberg.Properties{})
		if err != nil {
			return fmt.Errorf("failed to create namespace %s: %w", ig.Namespace, err)
		}
	}

	return nil
}

// GetCatalog returns the underlying catalog interface
func (ig *IcebergStore) GetCatalog() catalog.Catalog {
	return ig.Catalog
}

// LoadTable loads a table from the catalog
func (ig *IcebergStore) LoadTable(ctx context.Context, tableName string, props map[string]string) (*table.Table, error) {
	identifier := table.Identifier{ig.Namespace, tableName}
	return ig.Catalog.LoadTable(ctx, identifier, props)
}

// CreateTable creates a new table in the catalog
func (ig *IcebergStore) CreateTable(ctx context.Context, tableName string, schema *iceberg.Schema, props map[string]string) (*table.Table, error) {
	identifier := table.Identifier{ig.Namespace, tableName}

	// Merge default properties with provided properties
	mergedProps := make(map[string]string)
	for k, v := range ig.DefaultProperties {
		mergedProps[k] = v
	}
	for k, v := range props {
		mergedProps[k] = v
	}

	// Convert properties
	icebergProps := iceberg.Properties(mergedProps)

	// Create table with options
	var opts []catalog.CreateTableOpt
	if icebergProps != nil {
		opts = append(opts, catalog.WithProperties(icebergProps))
	}

	return ig.Catalog.CreateTable(ctx, identifier, schema, opts...)
}

// ListTables lists all tables in the catalog
func (ig *IcebergStore) ListTables(ctx context.Context, namespace string) ([]string, error) {
	// Use the configured namespace if none provided
	if namespace == "" {
		namespace = ig.Namespace
	}
	identifier := table.Identifier{namespace}

	var tables []string
	iter := ig.Catalog.ListTables(ctx, identifier)
	for tableIdent, err := range iter {
		if err != nil {
			return nil, err
		}
		tables = append(tables, catalog.TableNameFromIdent(tableIdent))
	}

	return tables, nil
}

// DropTable drops a table from the catalog
func (ig *IcebergStore) DropTable(ctx context.Context, tableName string) error {
	identifier := table.Identifier{ig.Namespace, tableName}
	return ig.Catalog.DropTable(ctx, identifier)
}

func (ig *IcebergStore) scan(ctx context.Context, tableName string, mint, maxt int64, metricName string) ([]*ParquetShard, error) {
	t, err := ig.Catalog.LoadTable(ctx, table.Identifier{ig.Namespace, tableName}, iceberg.Properties{})
	if err != nil {
		return nil, err
	}

	snapshot := t.CurrentSnapshot()
	if snapshot == nil {
		return nil, fmt.Errorf("no current snapshot for table %s", tableName)
	}

	// Get or create bucket for this table
	bkt, err := ig.getOrCreateBucket(ctx, tableName, t.Location())
	if err != nil {
		return nil, fmt.Errorf("failed to get or create bucket for table %s: %w", tableName, err)
	}

	// Load file system for the table
	fs, err := ig.loadFileSystem(ctx, t.MetadataLocation())
	if err != nil {
		return nil, fmt.Errorf("failed to load file system for table %s: %w", tableName, err)
	}

	// Get manifests from snapshot
	mf, err := snapshot.Manifests(fs)
	if err != nil {
		return nil, fmt.Errorf("failed to get manifests for table %s: %w", tableName, err)
	}

	// Use error group to parallelize manifest processing
	g, ctx := errgroup.WithContext(ctx)
	shardsChan := make(chan []*ParquetShard, len(mf))

	// Process each manifest in parallel
	for _, manifest := range mf {
		manifest := manifest // Capture for closure
		g.Go(func() error {
			entries, err := manifest.FetchEntries(fs, true)
			if err != nil {
				return fmt.Errorf("failed to fetch entries from manifest: %w", err)
			}

			manifestShards := make([]*ParquetShard, 0)

			// Process entries in parallel within each manifest
			entryG, entryCtx := errgroup.WithContext(ctx)
			entryShardsChan := make(chan *ParquetShard, len(entries))

			for _, entry := range entries {
				entry := entry // Capture for closure
				entryG.Go(func() error {
					// Check if the data file has timestamps within the query range
					if !ig.dataFileMaybeUseful(entry.DataFile(), mint, maxt, metricName) {
						return nil
					}

					cacheKey := fmt.Sprintf("%s-%s", tableName, entry.DataFile().FilePath())
					s := ig.cache.Get(cacheKey)
					if s == nil {
						ig.cache.Get(tableName + entry.DataFile().FilePath())
						s, err = NewParquetShard(context.WithoutCancel(entryCtx), entry.DataFile(), bkt)
						if err != nil {
							return fmt.Errorf("failed to create parquet shard for table %s: %w", tableName, err)
						}
						ig.cache.Set(cacheKey, s)
					}

					select {
					case entryShardsChan <- s:
					case <-entryCtx.Done():
						return entryCtx.Err()
					}
					return nil
				})
			}

			// Close channel after all goroutines complete
			go func() {
				entryG.Wait()
				close(entryShardsChan)
			}()

			// Collect shards from this manifest
			for s := range entryShardsChan {
				if s != nil {
					manifestShards = append(manifestShards, s)
				}
			}

			if err := entryG.Wait(); err != nil {
				return err
			}

			select {
			case shardsChan <- manifestShards:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		})
	}

	// Close channel after all manifest goroutines complete
	go func() {
		g.Wait()
		close(shardsChan)
	}()

	// Collect all shards
	var allShards []*ParquetShard
	for manifestShards := range shardsChan {
		allShards = append(allShards, manifestShards...)
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return allShards, nil
}

// dataFileMaybeUseful checks if the data file contains timestamps within the specified range
func (ig *IcebergStore) dataFileMaybeUseful(dataFile iceberg.DataFile, mint, maxt int64, metricName string) bool {
	// Get the lower and upper bounds from the data file
	lowerBounds := dataFile.LowerBoundValues()
	upperBounds := dataFile.UpperBoundValues()

	metricNameColID := 1
	timestampColID := 2

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

		if metricName != "" {
			if lowerBoundBytes, exists := lowerBounds[metricNameColID]; exists && len(lowerBoundBytes) > 0 {
				lowerBound := string(lowerBoundBytes)
				if lowerBound > metricName {
					return false
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

		if metricName != "" {
			if upperBoundBytes, exists := upperBounds[metricNameColID]; exists && len(upperBoundBytes) > 0 {
				upperBound := string(upperBoundBytes)
				if upperBound < metricName {
					return false
				}
			}
		}
	}

	return true // File overlaps with query range and metric name.
}

func getTimestamp(lowerBoundBytes []byte) int64 {
	return int64(binary.LittleEndian.Uint64(lowerBoundBytes))
}

// getOrCreateBucket returns an existing bucket or creates a new one for the table
func (ig *IcebergStore) getOrCreateBucket(ctx context.Context, tableName, tableLocation string) (objstore.Bucket, error) {
	ig.mu.RLock()
	bkt, exists := ig.bkts[tableName]
	ig.mu.RUnlock()

	if exists {
		return bkt, nil
	}

	// Create new bucket
	ig.mu.Lock()
	defer ig.mu.Unlock()

	// Double-check after acquiring write lock
	if bkt, exists = ig.bkts[tableName]; exists {
		return bkt, nil
	}

	// Parse the table location to extract bucket information
	u, err := url.Parse(tableLocation)
	if err != nil {
		return nil, fmt.Errorf("failed to parse table location %s: %w", tableLocation, err)
	}

	// Create bucket client
	bucketClient, err := bucket.NewClient(ctx, bucket.Config{
		Backend: "s3",
		S3: s3.Config{
			Endpoint:         fmt.Sprintf("s3tables.%s.amazonaws.com", ig.cfg.Rest.Region),
			BucketName:       u.Host,
			Region:           ig.cfg.Rest.Region,
			BucketLookupType: s3.BucketAutoLookup,
			DisableDualstack: true,
		},
	}, nil, "iceberg", log.NewNopLogger(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create bucket client: %w", err)
	}

	// Wrap with Icebucket
	iceBucket := NewIcebucket("s3://"+u.Host+"/", bucketClient)

	// Store in map
	ig.bkts[tableName] = iceBucket

	return iceBucket, nil
}

// loadFileSystem loads the file system for the given metadata location
func (ig *IcebergStore) loadFileSystem(ctx context.Context, metadataLocation string) (iceio.IO, error) {
	return iceio.LoadFS(ctx, nil, metadataLocation)
}

// Upload uploads parquet data to the specified table
func (ig *IcebergStore) Upload(ctx context.Context, tableName string, parquetData io.Reader) error {
	// Try to load the table first
	t, err := ig.Catalog.LoadTable(ctx, table.Identifier{ig.Namespace, tableName}, iceberg.Properties{})
	if err != nil {
		// Table doesn't exist, create it
		level.Info(log.NewNopLogger()).Log("msg", "table does not exist, creating new table", "table", tableName)

		// Create the table with timeseries schema
		t, err = ig.CreateTable(ctx, tableName, ig.getTimeseriesSchema(), nil)
		if err != nil {
			return fmt.Errorf("failed to create table %s: %w", tableName, err)
		}
	}

	// Get or create bucket for this table
	bkt, err := ig.getOrCreateBucket(ctx, tableName, t.Location())
	if err != nil {
		return fmt.Errorf("failed to get or create bucket for table %s: %w", tableName, err)
	}

	// Generate a unique filename for the parquet file
	locProvider, err := table.LoadLocationProvider(t.Location(), t.Properties())
	if err != nil {
		return fmt.Errorf("failed to load location provider for table %s: %w", tableName, err)
	}
	filePath := locProvider.NewDataLocation(fmt.Sprintf("00000-%d-%s.%s", ulid.Now(), ulid.MustNew(ulid.Now(), nil), "parquet"))

	// Upload the parquet file to the bucket
	if err := bkt.Upload(ctx, filePath, parquetData); err != nil {
		return fmt.Errorf("failed to upload parquet file to bucket: %w", err)
	}

	// Create a new transaction to add the file to the table
	txn := t.NewTransaction()

	// Add the file to the transaction - reference the file in the bucket
	err = txn.AddFiles(ctx, []string{filePath}, nil, true)
	if err != nil {
		return fmt.Errorf("failed to add files to transaction: %w", err)
	}

	// Commit the transaction
	_, err = txn.Commit(ctx)
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// getTimeseriesSchema returns the Iceberg schema for timeseries data
func (ig *IcebergStore) getTimeseriesSchema() *iceberg.Schema {
	return iceberg.NewSchema(0,
		iceberg.NestedField{
			ID:       1,
			Name:     "metric_name",
			Type:     iceberg.PrimitiveTypes.String,
			Required: true,
		},
		iceberg.NestedField{
			ID:       2,
			Name:     "timestamp",
			Type:     iceberg.PrimitiveTypes.Int64,
			Required: true,
		},
		iceberg.NestedField{
			ID:       3,
			Name:     "value",
			Type:     iceberg.PrimitiveTypes.Float64,
			Required: true,
		},
		iceberg.NestedField{
			ID:       4,
			Name:     "labels",
			Type:     iceberg.PrimitiveTypes.Binary,
			Required: false,
		},
	)
}

func generateULID() ulid.ULID {
	t := time.Now()
	entropy := ulid.Monotonic(rand.New(rand.NewSource(t.UnixNano())), 0)
	return ulid.MustNew(ulid.Timestamp(t), entropy)
}
