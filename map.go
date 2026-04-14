package node

import (
	"context"
	"sync"
	"time"

	"github.com/nikitakosatka/hive/pkg/hive"
)

// Version is a logical LWW version for one key.
// Ordering is lexicographic: (Counter, NodeID).
type Version struct {
	Counter uint64
	NodeID  string
}

// StateEntry stores one OR-Map key state.
type StateEntry struct {
	Value     string
	Tombstone bool
	Version   Version
}

// MapState is an exported snapshot representation used by Merge.
type MapState map[string]StateEntry

// CRDTMapNode is a state-based OR-Map with LWW values.
type CRDTMapNode struct {
	*hive.BaseNode

	mu sync.RWMutex

	state   MapState
	counter uint64
	nodeID  string
	nodes   []string
}

// NewCRDTMapNode creates a CRDT map node for the provided peer set.
func NewCRDTMapNode(id string, allNodeIDs []string) *CRDTMapNode {
	nodes := make([]string, 0)
	for _, pid := range allNodeIDs {
		if pid != id {
			nodes = append(nodes, pid)
		}
	}

	return &CRDTMapNode{
		BaseNode: hive.NewBaseNode(id),
		state:    make(MapState),
		counter:  0,
		nodeID:   id,
		nodes:    nodes,
	}
}

// Start starts message processing and anti-entropy broadcast (flood/gossip).
func (n *CRDTMapNode) Start(ctx context.Context) error {
	if err := n.BaseNode.Start(ctx); err != nil {
		return err
	}

	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				state := n.State()
				for _, nodeID := range n.nodes {
					n.Send(nodeID, state)
				}
			}
		}
	}()

	return nil
}

// Put writes a value with a fresh local version.
func (n *CRDTMapNode) Put(k, v string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.counter++
	n.state[k] = StateEntry{
		Value:     v,
		Tombstone: false,
		Version:   Version{Counter: n.counter, NodeID: n.nodeID},
	}
}

// Get returns the current visible value for key k.
func (n *CRDTMapNode) Get(k string) (string, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	entry, exists := n.state[k]
	if !exists || entry.Tombstone {
		return "", false
	}

	return entry.Value, true
}

// Delete marks the key as removed via a tombstone.
func (n *CRDTMapNode) Delete(k string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.counter++
	n.state[k] = StateEntry{
		Tombstone: true,
		Version:   Version{Counter: n.counter, NodeID: n.nodeID},
	}
}

// Merge joins local state with a remote state snapshot.
func (n *CRDTMapNode) Merge(remote MapState) {
	n.mu.Lock()
	defer n.mu.Unlock()

	for remoteKey, remoteEntry := range remote {
		entry, exists := n.state[remoteKey]
		if !exists || n.isNewerVersion(remoteEntry.Version, entry.Version) {
			n.state[remoteKey] = remoteEntry
		}
	}
}

func (n *CRDTMapNode) isNewerVersion(left, right Version) bool {
	return left.Counter > right.Counter ||
		(left.Counter == right.Counter && left.NodeID > right.NodeID)
}

// State returns a copy of the full CRDT state.
func (n *CRDTMapNode) State() MapState {
	n.mu.RLock()
	defer n.mu.RUnlock()

	stateCopy := make(MapState)
	for key, entry := range n.state {
		stateCopy[key] = entry
	}

	return stateCopy
}

// ToMap returns a value-only map view without tombstones.
func (n *CRDTMapNode) ToMap() map[string]string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	mapResult := make(map[string]string)
	for key, entry := range n.state {
		if !entry.Tombstone {
			mapResult[key] = entry.Value
		}
	}

	return mapResult
}

// Receive applies remote state snapshots.
func (n *CRDTMapNode) Receive(msg *hive.Message) error {
	receivedState, ok := msg.Payload.(MapState)
	if !ok {
		return nil
	}

	n.Merge(receivedState)

	return nil
}
