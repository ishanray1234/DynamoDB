package gossip

import (
	"fmt"
	"testing"
	"time"
)

func TestGossipFiveNodes(t *testing.T) {
	addresses := []string{
		"127.0.0.1:9101",
		"127.0.0.1:9102",
		"127.0.0.1:9103",
		"127.0.0.1:9104",
		"127.0.0.1:9105",
	}

	nodes := make([]*GossipNode, 5)

	// Create nodes
	for i := 0; i < 5; i++ {
		id := string(rune('A' + i))
		nodes[i] = NewGossipNode(id, addresses[i], 2*time.Second)
	}

	// Set up peer lists
	for i := 0; i < 5; i++ {
		for j := 0; j < 5; j++ {
			if i != j {
				peerID := string(rune('A' + j))
				nodes[i].Peers[peerID] = &NodeInfo{
					ID:      peerID,
					Address: addresses[j],
					Status:  Alive,
				}
			}
		}
	}

	// Start all nodes
	for _, node := range nodes {
		node.Start()
	}

	// Let gossip run for a while
	time.Sleep(8 * time.Second)

	// Check that each node sees all others as Alive
	for _, node := range nodes {
		node.Mu.RLock()
		for id, peer := range node.Peers {
			if peer.Status != Alive {
				t.Errorf("Node %s sees peer %s as %v, expected Alive", node.ID, id, peer.Status)
			}
		}
		node.Mu.RUnlock()
		fmt.Printf("Node %s peer statuses verified.\n", node.ID)
	}
}
