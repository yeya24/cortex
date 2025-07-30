package fragmenter

import (
	"github.com/thanos-io/promql-engine/logicalplan"
)

type Fragment struct {
	Node       logicalplan.Node
	FragmentID uint64
	ChildIDs   []uint64
	IsRoot     bool
}

func getNewID() uint64 {
	return 1 // for dummy fragmenter testing
}

func FragmentLogicalPlanNode(node logicalplan.Node) ([]Fragment, error) {
	// TODO: remote node fragmentation logic
	return []Fragment{
		{
			Node:       node,
			FragmentID: getNewID(),
			ChildIDs:   []uint64{},
			IsRoot:     true,
		},
	}, nil
}

type fragmentKey struct {
	queryID    uint64
	fragmentID uint64
}

type FragmentTable struct {
	mappings map[fragmentKey]string
}

func NewFragmentTable() *FragmentTable {
	return &FragmentTable{
		mappings: make(map[fragmentKey]string),
	}
}

func (f *FragmentTable) AddMapping(queryID uint64, fragmentID uint64, addr string) {
	key := fragmentKey{queryID: queryID, fragmentID: fragmentID}
	f.mappings[key] = addr
}

func (f *FragmentTable) GetMapping(queryID uint64, fragmentIDs []uint64) ([]string, bool) {
	var addresses []string
	for _, fragmentID := range fragmentIDs {
		key := fragmentKey{queryID: queryID, fragmentID: fragmentID}
		if addr, ok := f.mappings[key]; ok {
			addresses = append(addresses, addr)
		} else {
			return nil, false
		}
	}
	return addresses, true
}

func (f *FragmentTable) ClearMappings(queryID uint64) {
	for key := range f.mappings {
		if key.queryID == queryID {
			delete(f.mappings, key)
		}
	}
}
