package fragmenter

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSchedulerCoordination(t *testing.T) {
	table := NewFragmentTable()
	table.AddMapping(uint64(0), uint64(1), "localhost:8000")
	table.AddMapping(uint64(0), uint64(2), "localhost:8001")

	result, exist := table.GetMapping(uint64(0), []uint64{1, 2})
	require.True(t, exist)
	require.Equal(t, []string{"localhost:8000", "localhost:8001"}, result)

	result, exist = table.GetMapping(uint64(0), []uint64{1, 3})
	require.False(t, exist)
	require.Empty(t, result)

	result, exist = table.GetMapping(uint64(0), []uint64{1})
	require.True(t, exist)
	require.Equal(t, []string{"localhost:8000"}, result)

	table.ClearMappings(uint64(0))
	result, exist = table.GetMapping(uint64(0), []uint64{1})
	require.False(t, exist)
	require.Empty(t, result)
}
