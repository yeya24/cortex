package plan_fragments

import (
	"encoding/binary"
	"github.com/cortexproject/cortex/pkg/engine/distributed_execution"
	"github.com/google/uuid"
	"github.com/thanos-io/promql-engine/logicalplan"
)

type Fragment struct {
	Node       logicalplan.Node
	FragmentID uint64
	ChildIDs   []uint64
	IsRoot     bool
}

func getNewID() uint64 {
	id := uuid.New()
	return binary.BigEndian.Uint64(id[:8])
}

func (s *Fragment) IsEmpty() bool {
	if s.Node != nil {
		return false
	}
	if s.FragmentID != 0 {
		return false
	}
	if s.IsRoot {
		return false
	}
	if len(s.ChildIDs) != 0 {
		return false
	}
	return true
}

func FragmentLogicalPlanNode(node logicalplan.Node) ([]Fragment, error) {
	newFragment := Fragment{}
	fragments := []Fragment{}
	nextChildrenIDs := []uint64{}
	logicalplan.TraverseBottomUp(nil, &node, func(parent, current *logicalplan.Node) bool {
		if parent == nil { // if we have reached the root
			newFragment = Fragment{
				Node:       node,
				FragmentID: getNewID(),
				ChildIDs:   nextChildrenIDs,
				IsRoot:     true,
			}
			fragments = append(fragments, newFragment)
			return false // break the loop
		}
		if distributed_execution.RemoteNode == (*parent).Type() {
			newFragment = Fragment{
				Node:       *current,
				FragmentID: getNewID(),
				ChildIDs:   []uint64{},
				IsRoot:     false,
			}
			fragments = append(fragments, newFragment)
			nextChildrenIDs = append(nextChildrenIDs, newFragment.FragmentID)
		}
		return false
	})

	return fragments, nil
}
