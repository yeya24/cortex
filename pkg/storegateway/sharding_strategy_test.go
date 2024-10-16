package storegateway

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/extprom"

	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/services"
)

func TestDefaultShardingStrategy(t *testing.T) {
	t.Parallel()
	// The following block IDs have been picked to have increasing hash values
	// in order to simplify the tests.
	block1 := ulid.MustNew(1, nil) // hash: 283204220
	block2 := ulid.MustNew(2, nil) // hash: 444110359
	block3 := ulid.MustNew(5, nil) // hash: 2931974232
	block4 := ulid.MustNew(6, nil) // hash: 3092880371
	numAllBlocks := 4

	block1Hash := cortex_tsdb.HashBlockID(block1)
	block2Hash := cortex_tsdb.HashBlockID(block2)
	block3Hash := cortex_tsdb.HashBlockID(block3)
	block4Hash := cortex_tsdb.HashBlockID(block4)

	registeredAt := time.Now()

	tests := map[string]struct {
		replicationFactor    int
		zoneAwarenessEnabled bool
		setupRing            func(*ring.Desc)
		expectedBlocks       map[string][]ulid.ULID
	}{
		"one ACTIVE instance in the ring with replication factor = 1": {
			replicationFactor: 1,
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{0}, ring.ACTIVE, registeredAt)
			},
			expectedBlocks: map[string][]ulid.ULID{
				"127.0.0.1": {block1, block2, block3, block4},
				"127.0.0.2": {},
			},
		},
		"two ACTIVE instances in the ring with replication factor = 1": {
			replicationFactor: 1,
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1, block3Hash + 1}, ring.ACTIVE, registeredAt)
				r.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1, block4Hash + 1}, ring.ACTIVE, registeredAt)
			},
			expectedBlocks: map[string][]ulid.ULID{
				"127.0.0.1": {block1, block3},
				"127.0.0.2": {block2, block4},
			},
		},
		"one ACTIVE instance in the ring with replication factor = 2": {
			replicationFactor: 2,
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{0}, ring.ACTIVE, registeredAt)
			},
			expectedBlocks: map[string][]ulid.ULID{
				"127.0.0.1": {block1, block2, block3, block4},
				"127.0.0.2": {},
			},
		},
		"two ACTIVE instances in the ring with replication factor = 2": {
			replicationFactor: 2,
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1, block3Hash + 1}, ring.ACTIVE, registeredAt)
				r.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1, block4Hash + 1}, ring.ACTIVE, registeredAt)
			},
			expectedBlocks: map[string][]ulid.ULID{
				"127.0.0.1": {block1, block2, block3, block4},
				"127.0.0.2": {block1, block2, block3, block4},
			},
		},
		"multiple ACTIVE instances in the ring with replication factor = 2": {
			replicationFactor: 2,
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1, block3Hash + 1}, ring.ACTIVE, registeredAt)
				r.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt)
				r.AddIngester("instance-3", "127.0.0.3", "", []uint32{block4Hash + 1}, ring.ACTIVE, registeredAt)
			},
			expectedBlocks: map[string][]ulid.ULID{
				"127.0.0.1": {block1, block3 /* replicated: */, block2, block4},
				"127.0.0.2": {block2 /* replicated: */, block1},
				"127.0.0.3": {block4 /* replicated: */, block3},
			},
		},
		"multiple ACTIVE instances in the ring with replication factor = 2 and zone-awareness enabled": {
			replicationFactor:    2,
			zoneAwarenessEnabled: true,
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "zone-a", []uint32{block1Hash + 1, block3Hash + 1}, ring.ACTIVE, registeredAt)
				r.AddIngester("instance-2", "127.0.0.2", "zone-a", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt)
				r.AddIngester("instance-3", "127.0.0.3", "zone-b", []uint32{block4Hash + 1}, ring.ACTIVE, registeredAt)
			},
			expectedBlocks: map[string][]ulid.ULID{
				"127.0.0.1": {block1, block3, block4},
				"127.0.0.2": {block2},
				"127.0.0.3": {block1, block2, block3, block4},
			},
		},
		"one unhealthy instance in the ring with replication factor = 1": {
			replicationFactor: 1,
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1, block3Hash + 1}, ring.ACTIVE, registeredAt)
				r.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt)

				r.Ingesters["instance-3"] = ring.InstanceDesc{
					Addr:      "127.0.0.3",
					Timestamp: time.Now().Add(-time.Hour).Unix(),
					State:     ring.ACTIVE,
					Tokens:    []uint32{block4Hash + 1},
				}
			},
			expectedBlocks: map[string][]ulid.ULID{
				// No shard has the blocks of the unhealthy instance.
				"127.0.0.1": {block1, block3},
				"127.0.0.2": {block2},
				"127.0.0.3": {},
			},
		},
		"one unhealthy instance in the ring with replication factor = 2": {
			replicationFactor: 2,
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1, block3Hash + 1}, ring.ACTIVE, registeredAt)
				r.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt)

				r.Ingesters["instance-3"] = ring.InstanceDesc{
					Addr:      "127.0.0.3",
					Timestamp: time.Now().Add(-time.Hour).Unix(),
					State:     ring.ACTIVE,
					Tokens:    []uint32{block4Hash + 1},
				}
			},
			expectedBlocks: map[string][]ulid.ULID{
				"127.0.0.1": {block1, block3 /* replicated: */, block2, block4},
				"127.0.0.2": {block2 /* replicated: */, block1},
				"127.0.0.3": {},
			},
		},
		"two unhealthy instances in the ring with replication factor = 2": {
			replicationFactor: 2,
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt)

				r.Ingesters["instance-2"] = ring.InstanceDesc{
					Addr:      "127.0.0.2",
					Timestamp: time.Now().Add(-time.Hour).Unix(),
					State:     ring.ACTIVE,
					Tokens:    []uint32{block2Hash + 1, block3Hash + 1},
				}

				r.Ingesters["instance-3"] = ring.InstanceDesc{
					Addr:      "127.0.0.3",
					Timestamp: time.Now().Add(-time.Hour).Unix(),
					State:     ring.ACTIVE,
					Tokens:    []uint32{block4Hash + 1},
				}
			},
			expectedBlocks: map[string][]ulid.ULID{
				// There may be some blocks missing depending if there are shared blocks
				// between the two unhealthy nodes.
				"127.0.0.1": {block1 /* replicated: */, block4},
				"127.0.0.2": {},
				"127.0.0.3": {},
			},
		},
		"two unhealthy instances in the ring with replication factor = 3": {
			replicationFactor: 3,
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt)
				r.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt)

				r.Ingesters["instance-3"] = ring.InstanceDesc{
					Addr:      "127.0.0.3",
					Timestamp: time.Now().Add(-time.Hour).Unix(),
					State:     ring.ACTIVE,
					Tokens:    []uint32{block3Hash + 1},
				}

				r.Ingesters["instance-4"] = ring.InstanceDesc{
					Addr:      "127.0.0.4",
					Timestamp: time.Now().Add(-time.Hour).Unix(),
					State:     ring.ACTIVE,
					Tokens:    []uint32{block4Hash + 1},
				}
			},
			expectedBlocks: map[string][]ulid.ULID{
				// There may be some blocks missing depending if there are shared blocks
				// between the two unhealthy nodes.
				"127.0.0.1": {block1 /* replicated: */, block3, block4},
				"127.0.0.2": {block2 /* replicated: */, block1, block4},
				"127.0.0.3": {},
				"127.0.0.4": {},
			},
		},
		"LEAVING instance in the ring should continue to keep its shard blocks but they should also be replicated to another instance": {
			replicationFactor: 1,
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1, block3Hash + 1}, ring.ACTIVE, registeredAt)
				r.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt)
				r.AddIngester("instance-3", "127.0.0.3", "", []uint32{block4Hash + 1}, ring.LEAVING, registeredAt)
			},
			expectedBlocks: map[string][]ulid.ULID{
				"127.0.0.1": {block1, block3 /* replicated: */, block4},
				"127.0.0.2": {block2},
				"127.0.0.3": {block4},
			},
		},
		"JOINING instance in the ring should get its shard blocks and they should not be replicated to another instance": {
			replicationFactor: 1,
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1, block3Hash + 1}, ring.ACTIVE, registeredAt)
				r.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt)
				r.AddIngester("instance-3", "127.0.0.3", "", []uint32{block4Hash + 1}, ring.JOINING, registeredAt)
			},
			expectedBlocks: map[string][]ulid.ULID{
				"127.0.0.1": {block1, block3},
				"127.0.0.2": {block2},
				"127.0.0.3": {block4},
			},
		},
	}

	for testName, testData := range tests {
		testName := testName
		testData := testData

		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			store, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
			t.Cleanup(func() { assert.NoError(t, closer.Close()) })

			// Initialize the ring state.
			require.NoError(t, store.CAS(ctx, "test", func(in interface{}) (interface{}, bool, error) {
				d := ring.NewDesc()
				testData.setupRing(d)
				return d, true, nil
			}))

			cfg := ring.Config{
				ReplicationFactor:    testData.replicationFactor,
				HeartbeatTimeout:     time.Minute,
				ZoneAwarenessEnabled: testData.zoneAwarenessEnabled,
			}

			r, err := ring.NewWithStoreClientAndStrategy(cfg, "test", "test", store, ring.NewIgnoreUnhealthyInstancesReplicationStrategy(), nil, nil)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(ctx, r))
			defer services.StopAndAwaitTerminated(ctx, r) //nolint:errcheck

			// Wait until the ring client has synced.
			require.NoError(t, ring.WaitInstanceState(ctx, r, "instance-1", ring.ACTIVE))

			for instanceAddr, expectedBlocks := range testData.expectedBlocks {
				filter := NewDefaultShardingStrategy(r, instanceAddr, log.NewNopLogger(), nil)
				synced := extprom.NewTxGaugeVec(nil, prometheus.GaugeOpts{}, []string{"state"})
				synced.WithLabelValues(shardExcludedMeta).Set(0)

				metas := map[ulid.ULID]*metadata.Meta{
					block1: {},
					block2: {},
					block3: {},
					block4: {},
				}

				err = filter.FilterBlocks(ctx, "user-1", metas, map[ulid.ULID]struct{}{}, synced)
				require.NoError(t, err)

				var actualBlocks []ulid.ULID
				for id := range metas {
					actualBlocks = append(actualBlocks, id)
				}

				assert.ElementsMatch(t, expectedBlocks, actualBlocks)

				// Assert on the metric used to keep track of the blocks filtered out.
				synced.Submit()
				assert.Equal(t, float64(numAllBlocks-len(testData.expectedBlocks[instanceAddr])), testutil.ToFloat64(synced))
			}
		})
	}
}

func TestShuffleShardingStrategy(t *testing.T) {
	t.Parallel()
	// The following block IDs have been picked to have increasing hash values
	// in order to simplify the tests.
	block1 := ulid.MustNew(1, nil) // hash: 283204220
	block2 := ulid.MustNew(2, nil) // hash: 444110359
	block3 := ulid.MustNew(5, nil) // hash: 2931974232
	block4 := ulid.MustNew(6, nil) // hash: 3092880371
	numAllBlocks := 4

	block1Hash := cortex_tsdb.HashBlockID(block1)
	block2Hash := cortex_tsdb.HashBlockID(block2)
	block3Hash := cortex_tsdb.HashBlockID(block3)
	block4Hash := cortex_tsdb.HashBlockID(block4)

	userID := "user-A"
	registeredAt := time.Now()

	type usersExpectation struct {
		instanceID   string
		instanceAddr string
		users        []string
	}

	type blocksExpectation struct {
		instanceID   string
		instanceAddr string
		blocks       []ulid.ULID
	}

	tests := map[string]struct {
		replicationFactor int
		limits            ShardingLimits
		setupRing         func(*ring.Desc)
		expectedUsers     []usersExpectation
		expectedBlocks    []blocksExpectation
		isTenantDisabled  bool
	}{
		"one ACTIVE instance in the ring with RF = 1 and SS = 1": {
			replicationFactor: 1,
			limits:            &shardingLimitsMock{storeGatewayTenantShardSize: 1},
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{0}, ring.ACTIVE, registeredAt)
			},
			expectedUsers: []usersExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", users: []string{userID}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", users: nil},
			},
			expectedBlocks: []blocksExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", blocks: []ulid.ULID{block1, block2, block3, block4}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", blocks: []ulid.ULID{}},
			},
		},
		"one ACTIVE instance in the ring with RF = 1 SS = 1 and user disabled": {
			replicationFactor: 1,
			limits:            &shardingLimitsMock{storeGatewayTenantShardSize: 1},
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{0}, ring.ACTIVE, registeredAt)
			},
			expectedUsers: []usersExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", users: nil},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", users: nil},
			},
			isTenantDisabled: true,
		},
		"one ACTIVE instance in the ring with RF = 2 and SS = 1 (should still sync blocks on the only available instance)": {
			replicationFactor: 1,
			limits:            &shardingLimitsMock{storeGatewayTenantShardSize: 1},
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{0}, ring.ACTIVE, registeredAt)
			},
			expectedUsers: []usersExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", users: []string{userID}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", users: nil},
			},
			expectedBlocks: []blocksExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", blocks: []ulid.ULID{block1, block2, block3, block4}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", blocks: []ulid.ULID{}},
			},
		},
		"one ACTIVE instance in the ring with RF = 2 and SS = 2 (should still sync blocks on the only available instance)": {
			replicationFactor: 1,
			limits:            &shardingLimitsMock{storeGatewayTenantShardSize: 2},
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{0}, ring.ACTIVE, registeredAt)
			},
			expectedUsers: []usersExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", users: []string{userID}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", users: nil},
			},
			expectedBlocks: []blocksExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", blocks: []ulid.ULID{block1, block2, block3, block4}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", blocks: []ulid.ULID{}},
			},
		},
		"two ACTIVE instances in the ring with RF = 1 and SS = 1 (should sync blocks on 1 instance because of the shard size)": {
			replicationFactor: 1,
			limits:            &shardingLimitsMock{storeGatewayTenantShardSize: 1},
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1, block3Hash + 1}, ring.ACTIVE, registeredAt)
				r.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1, block4Hash + 1}, ring.ACTIVE, registeredAt)
			},
			expectedUsers: []usersExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", users: []string{userID}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", users: nil},
			},
			expectedBlocks: []blocksExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", blocks: []ulid.ULID{block1, block2, block3, block4}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", blocks: []ulid.ULID{}},
			},
		},
		"two ACTIVE instances in the ring with RF = 1 and SS = 2 (should sync blocks on 2 instances because of the shard size)": {
			replicationFactor: 1,
			limits:            &shardingLimitsMock{storeGatewayTenantShardSize: 2},
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1, block3Hash + 1}, ring.ACTIVE, registeredAt)
				r.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1, block4Hash + 1}, ring.ACTIVE, registeredAt)
			},
			expectedUsers: []usersExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", users: []string{userID}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", users: []string{userID}},
			},
			expectedBlocks: []blocksExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", blocks: []ulid.ULID{block1, block3}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", blocks: []ulid.ULID{block2, block4}},
			},
		},
		"two ACTIVE instances in the ring with RF = 2 and SS = 1 (should sync blocks on 1 instance because of the shard size)": {
			replicationFactor: 2,
			limits:            &shardingLimitsMock{storeGatewayTenantShardSize: 1},
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1, block3Hash + 1}, ring.ACTIVE, registeredAt)
				r.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1, block4Hash + 1}, ring.ACTIVE, registeredAt)
			},
			expectedUsers: []usersExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", users: []string{userID}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", users: nil},
			},
			expectedBlocks: []blocksExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", blocks: []ulid.ULID{block1, block2, block3, block4}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", blocks: []ulid.ULID{}},
			},
		},
		"two ACTIVE instances in the ring with RF = 2 and SS = 2 (should sync all blocks on 2 instances)": {
			replicationFactor: 2,
			limits:            &shardingLimitsMock{storeGatewayTenantShardSize: 2},
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1, block3Hash + 1}, ring.ACTIVE, registeredAt)
				r.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1, block4Hash + 1}, ring.ACTIVE, registeredAt)
			},
			expectedUsers: []usersExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", users: []string{userID}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", users: []string{userID}},
			},
			expectedBlocks: []blocksExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", blocks: []ulid.ULID{block1, block2, block3, block4}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", blocks: []ulid.ULID{block1, block2, block3, block4}},
			},
		},
		"multiple ACTIVE instances in the ring with RF = 2 and SS = 3": {
			replicationFactor: 2,
			limits:            &shardingLimitsMock{storeGatewayTenantShardSize: 3},
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1, block3Hash + 1}, ring.ACTIVE, registeredAt)
				r.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt)
				r.AddIngester("instance-3", "127.0.0.3", "", []uint32{block4Hash + 1}, ring.ACTIVE, registeredAt)
			},
			expectedUsers: []usersExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", users: []string{userID}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", users: []string{userID}},
				{instanceID: "instance-3", instanceAddr: "127.0.0.3", users: []string{userID}},
			},
			expectedBlocks: []blocksExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", blocks: []ulid.ULID{block1, block3 /* replicated: */, block2, block4}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", blocks: []ulid.ULID{block2 /* replicated: */, block1}},
				{instanceID: "instance-3", instanceAddr: "127.0.0.3", blocks: []ulid.ULID{block4 /* replicated: */, block3}},
			},
		},
		"one unhealthy instance in the ring with RF = 1 and SS = 3": {
			replicationFactor: 1,
			limits:            &shardingLimitsMock{storeGatewayTenantShardSize: 3},
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1, block3Hash + 1}, ring.ACTIVE, registeredAt)
				r.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt)

				r.Ingesters["instance-3"] = ring.InstanceDesc{
					Addr:      "127.0.0.3",
					Timestamp: time.Now().Add(-time.Hour).Unix(),
					State:     ring.ACTIVE,
					Tokens:    []uint32{block4Hash + 1},
				}
			},
			expectedUsers: []usersExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", users: []string{userID}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", users: []string{userID}},
				{instanceID: "instance-3", instanceAddr: "127.0.0.3", users: []string{userID}},
			},
			expectedBlocks: []blocksExpectation{
				// No shard has the blocks of the unhealthy instance.
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", blocks: []ulid.ULID{block1, block3}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", blocks: []ulid.ULID{block2}},
				{instanceID: "instance-3", instanceAddr: "127.0.0.3", blocks: []ulid.ULID{}},
			},
		},
		"one unhealthy instance in the ring with RF = 2 and SS = 3": {
			replicationFactor: 2,
			limits:            &shardingLimitsMock{storeGatewayTenantShardSize: 3},
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1, block3Hash + 1}, ring.ACTIVE, registeredAt)
				r.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt)

				r.Ingesters["instance-3"] = ring.InstanceDesc{
					Addr:      "127.0.0.3",
					Timestamp: time.Now().Add(-time.Hour).Unix(),
					State:     ring.ACTIVE,
					Tokens:    []uint32{block4Hash + 1},
				}
			},
			expectedUsers: []usersExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", users: []string{userID}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", users: []string{userID}},
				{instanceID: "instance-3", instanceAddr: "127.0.0.3", users: []string{userID}},
			},
			expectedBlocks: []blocksExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", blocks: []ulid.ULID{block1, block3 /* replicated: */, block2, block4}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", blocks: []ulid.ULID{block2 /* replicated: */, block1}},
				{instanceID: "instance-3", instanceAddr: "127.0.0.3", blocks: []ulid.ULID{}},
			},
		},
		"one unhealthy instance in the ring with RF = 2 and SS = 2": {
			replicationFactor: 2,
			limits:            &shardingLimitsMock{storeGatewayTenantShardSize: 2},
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1, block4Hash + 1}, ring.ACTIVE, registeredAt)
				r.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt)

				r.Ingesters["instance-3"] = ring.InstanceDesc{
					Addr:      "127.0.0.3",
					Timestamp: time.Now().Add(-time.Hour).Unix(),
					State:     ring.ACTIVE,
					Tokens:    []uint32{block3Hash + 1},
				}
			},
			expectedUsers: []usersExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", users: []string{userID}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", users: nil},
				{instanceID: "instance-3", instanceAddr: "127.0.0.3", users: []string{userID}},
			},
			expectedBlocks: []blocksExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", blocks: []ulid.ULID{block1, block2, block3, block4}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", blocks: []ulid.ULID{ /* no blocks because not belonging to the shard */ }},
				{instanceID: "instance-3", instanceAddr: "127.0.0.3", blocks: []ulid.ULID{ /* no blocks because unhealthy */ }},
			},
		},
		"LEAVING instance in the ring should continue to keep its shard blocks but they should also be replicated to another instance": {
			replicationFactor: 1,
			limits:            &shardingLimitsMock{storeGatewayTenantShardSize: 2},
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1, block3Hash + 1}, ring.ACTIVE, registeredAt)
				r.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt)
				r.AddIngester("instance-3", "127.0.0.3", "", []uint32{block4Hash + 1}, ring.LEAVING, registeredAt)
			},
			expectedUsers: []usersExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", users: []string{userID}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", users: nil},
				{instanceID: "instance-3", instanceAddr: "127.0.0.3", users: []string{userID}},
			},
			expectedBlocks: []blocksExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", blocks: []ulid.ULID{block1, block2, block3 /* replicated: */, block4}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", blocks: []ulid.ULID{ /* no blocks because not belonging to the shard */ }},
				{instanceID: "instance-3", instanceAddr: "127.0.0.3", blocks: []ulid.ULID{block4}},
			},
		},
		"JOINING instance in the ring should get its shard blocks and they should not be replicated to another instance": {
			replicationFactor: 1,
			limits:            &shardingLimitsMock{storeGatewayTenantShardSize: 2},
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1, block3Hash + 1}, ring.ACTIVE, registeredAt)
				r.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt)
				r.AddIngester("instance-3", "127.0.0.3", "", []uint32{block4Hash + 1}, ring.JOINING, registeredAt)
			},
			expectedUsers: []usersExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", users: []string{userID}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", users: nil},
				{instanceID: "instance-3", instanceAddr: "127.0.0.3", users: []string{userID}},
			},
			expectedBlocks: []blocksExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", blocks: []ulid.ULID{block1, block2, block3}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", blocks: []ulid.ULID{ /* no blocks because not belonging to the shard */ }},
				{instanceID: "instance-3", instanceAddr: "127.0.0.3", blocks: []ulid.ULID{block4}},
			},
		},
		"SS = 0 disables shuffle sharding": {
			replicationFactor: 1,
			limits:            &shardingLimitsMock{storeGatewayTenantShardSize: 0},
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1, block3Hash + 1}, ring.ACTIVE, registeredAt)
				r.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1, block4Hash + 1}, ring.ACTIVE, registeredAt)
			},
			expectedUsers: []usersExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", users: []string{userID}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", users: []string{userID}},
			},
			expectedBlocks: []blocksExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", blocks: []ulid.ULID{block1, block3}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", blocks: []ulid.ULID{block2, block4}},
			},
		},
	}

	for testName, testData := range tests {
		for _, zoneStableShuffleSharding := range []bool{false, true} {
			testName := testName
			testData := testData

			t.Run(fmt.Sprintf("%s %s", testName, strconv.FormatBool(zoneStableShuffleSharding)), func(t *testing.T) {
				t.Parallel()

				ctx := context.Background()
				store, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
				t.Cleanup(func() { assert.NoError(t, closer.Close()) })

				// Initialize the ring state.
				require.NoError(t, store.CAS(ctx, "test", func(in interface{}) (interface{}, bool, error) {
					d := ring.NewDesc()
					testData.setupRing(d)
					return d, true, nil
				}))

				cfg := ring.Config{
					ReplicationFactor:    testData.replicationFactor,
					HeartbeatTimeout:     time.Minute,
					SubringCacheDisabled: true,
				}

				r, err := ring.NewWithStoreClientAndStrategy(cfg, "test", "test", store, ring.NewIgnoreUnhealthyInstancesReplicationStrategy(), nil, nil)
				require.NoError(t, err)
				require.NoError(t, services.StartAndAwaitRunning(ctx, r))
				defer services.StopAndAwaitTerminated(ctx, r) //nolint:errcheck

				// Wait until the ring client has synced.
				require.NoError(t, ring.WaitInstanceState(ctx, r, "instance-1", ring.ACTIVE))

				var allowedTenants *util.AllowedTenants
				if testData.isTenantDisabled {
					allowedTenants = util.NewAllowedTenants(nil, []string{userID})
				}

				// Assert on filter users.
				for _, expected := range testData.expectedUsers {
					filter := NewShuffleShardingStrategy(r, expected.instanceID, expected.instanceAddr, testData.limits, log.NewNopLogger(), allowedTenants, zoneStableShuffleSharding) //nolint:govet
					assert.Equal(t, expected.users, filter.FilterUsers(ctx, []string{userID}))
				}

				// Assert on filter blocks.
				for _, expected := range testData.expectedBlocks {
					filter := NewShuffleShardingStrategy(r, expected.instanceID, expected.instanceAddr, testData.limits, log.NewNopLogger(), allowedTenants, zoneStableShuffleSharding) //nolint:govet
					synced := extprom.NewTxGaugeVec(nil, prometheus.GaugeOpts{}, []string{"state"})
					synced.WithLabelValues(shardExcludedMeta).Set(0)

					metas := map[ulid.ULID]*metadata.Meta{
						block1: {},
						block2: {},
						block3: {},
						block4: {},
					}

					err = filter.FilterBlocks(ctx, userID, metas, map[ulid.ULID]struct{}{}, synced)
					require.NoError(t, err)

					var actualBlocks []ulid.ULID
					for id := range metas {
						actualBlocks = append(actualBlocks, id)
					}

					assert.ElementsMatch(t, expected.blocks, actualBlocks)

					// Assert on the metric used to keep track of the blocks filtered out.
					synced.Submit()
					assert.Equal(t, float64(numAllBlocks-len(expected.blocks)), testutil.ToFloat64(synced))
				}
			})
		}
	}
}

type shardingLimitsMock struct {
	storeGatewayTenantShardSize float64
}

func (m *shardingLimitsMock) StoreGatewayTenantShardSize(_ string) float64 {
	return m.storeGatewayTenantShardSize
}

func TestSGAutoForget(t *testing.T) {
	// The following block IDs have been picked to have increasing hash values
	// in order to simplify the tests.
	block1 := ulid.MustNew(4, nil) // hash: 122298081

	block1Hash := cortex_tsdb.HashBlockID(block1)

	userID := "user-A"
	registeredAt := time.Now().Add(-10 * time.Minute)

	type usersExpectation struct {
		instanceID   string
		instanceAddr string
		users        []string
	}

	type blocksExpectation struct {
		instanceID   string
		instanceAddr string
		blocks       []ulid.ULID
	}

	tests := map[string]struct {
		replicationFactor int
		limits            ShardingLimits
		setupRing         func(*ring.Desc)
		expectedUsers     []usersExpectation
		expectedBlocks    []blocksExpectation
		isTenantDisabled  bool
	}{
		"some test": {
			replicationFactor: 9,
			limits:            &shardingLimitsMock{storeGatewayTenantShardSize: 0.4},
			setupRing: func(d *ring.Desc) {
				d.AddIngester("instance-01", "127.0.0.1", "1", []uint32{block1Hash - 5}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-02", "127.0.0.2", "2", []uint32{block1Hash - 4}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-03", "127.0.0.3", "3", []uint32{block1Hash - 3}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-04", "127.0.0.4", "1", []uint32{block1Hash - 2}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-05", "127.0.0.5", "2", []uint32{block1Hash + 5}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-06", "127.0.0.6", "3", []uint32{block1Hash + 6}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-07", "127.0.0.7", "1", []uint32{block1Hash + 7}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-08", "127.0.0.8", "2", []uint32{block1Hash + 8}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-09", "127.0.0.9", "3", []uint32{block1Hash + 9}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-10", "127.0.0.10", "1", []uint32{block1Hash + 10}, ring.ACTIVE, registeredAt)
				// d.AddIngester("instance-11", "127.0.0.11", "2", []uint32{block1Hash + 11}, ring.PENDING, registeredAt) // REMOVE
				// d.AddIngester("instance-12", "127.0.0.12", "3", []uint32{block1Hash + 12}, ring.PENDING, registeredAt) // REMOVE
				d.AddIngester("instance-13", "127.0.0.13", "1", []uint32{block1Hash + 13}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-14", "127.0.0.14", "2", []uint32{block1Hash + 14}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-15", "127.0.0.15", "3", []uint32{block1Hash + 15}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-16", "127.0.0.16", "1", []uint32{block1Hash + 16}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-17", "127.0.0.17", "2", []uint32{block1Hash + 17}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-18", "127.0.0.18", "3", []uint32{block1Hash + 18}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-19", "127.0.0.19", "1", []uint32{block1Hash + 19}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-20", "127.0.0.20", "2", []uint32{block1Hash + 20}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-21", "127.0.0.21", "3", []uint32{block1Hash + 21}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-22", "127.0.0.22", "1", []uint32{block1Hash + 22}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-23", "127.0.0.23", "2", []uint32{block1Hash + 23}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-24", "127.0.0.24", "3", []uint32{block1Hash + 24}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-25", "127.0.0.25", "1", []uint32{block1Hash + 25}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-26", "127.0.0.26", "2", []uint32{block1Hash + 26}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-27", "127.0.0.27", "3", []uint32{block1Hash + 27}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-28", "127.0.0.28", "1", []uint32{block1Hash + 28}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-29", "127.0.0.29", "2", []uint32{block1Hash + 29}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-30", "127.0.0.30", "3", []uint32{block1Hash + 30}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-31", "127.0.0.31", "1", []uint32{block1Hash + 31}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-32", "127.0.0.32", "2", []uint32{block1Hash + 32}, ring.ACTIVE, registeredAt)
			},
		},
	}

	for testName, testData := range tests {
		testName := testName
		testData := testData

		t.Run(fmt.Sprintf("%s", testName), func(t *testing.T) {

			ctx := context.Background()
			store, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
			t.Cleanup(func() { assert.NoError(t, closer.Close()) })

			// Initialize the ring state.
			require.NoError(t, store.CAS(ctx, "test", func(in interface{}) (interface{}, bool, error) {
				d := ring.NewDesc()
				testData.setupRing(d)
				return d, true, nil
			}))

			cfg := ring.Config{
				ReplicationFactor:    testData.replicationFactor,
				HeartbeatTimeout:     time.Minute,
				SubringCacheDisabled: true,
				ZoneAwarenessEnabled: true,
			}

			r, err := ring.NewWithStoreClientAndStrategy(cfg, "test", "test", store, ring.NewIgnoreUnhealthyInstancesReplicationStrategy(), nil, nil)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(ctx, r))
			defer services.StopAndAwaitTerminated(ctx, r) //nolint:errcheck

			// Wait until the ring client has synced.
			require.NoError(t, ring.WaitInstanceState(ctx, r, "instance-01", ring.ACTIVE))

			addrs := []string{"127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.0.5"}
			insts := []string{"instance-1", "instance-2", "instance-3", "instance-4", "instance-5"}

			synced := extprom.NewTxGaugeVec(nil, prometheus.GaugeOpts{}, []string{"state"})
			synced.WithLabelValues(shardExcludedMeta).Set(0)

			for i := 0; i < 5; i++ {
				filter := NewShuffleShardingStrategy(r, insts[i], addrs[i], testData.limits, log.NewNopLogger(), nil, true)
				metas := map[ulid.ULID]*metadata.Meta{
					block1: {},
				}

				err = filter.FilterBlocks(ctx, userID, metas, map[ulid.ULID]struct{}{}, synced)
				fmt.Printf("%s: %s --> len(metas): %d\n", insts[i], addrs[i], len(metas))
				require.NoError(t, err)
			}
		})
	}
}
