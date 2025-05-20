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
		nodes[i] = NewGossipNode(id, addresses[i], 2*time.Second, 3)
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

	// Scenario 1: All nodes initially Alive
	for _, node := range nodes {
		node.Mu.RLock()
		for id, peer := range node.Peers {
			if peer.Status != Alive {
				t.Errorf("Node %s sees peer %s as %v, expected Alive", node.ID, id, peer.Status)
			}
		}
		node.Mu.RUnlock()
		fmt.Printf("Node %s peer statuses verified for Alive state.\n", node.ID)
	}

	// Scenario 2: Simulate one node going down
	// nodes[4] = nil // Simulate node E going down (index 4)
	nodes[4].Stop()
	fmt.Println("Node E is simulated to be down.")
	time.Sleep(10 * time.Second)
	// wait briefly if needed: time.Sleep(500 * time.Millisecond)
	// nodes[4].Start()

	// Verify that remaining nodes eventually mark E as Dead
	for _, node := range nodes[:4] {
		node.Mu.RLock()
		if peer, ok := node.Peers["E"]; ok {
			if peer.Status != Dead {
				t.Errorf("Node %s sees E as %v, expected Dead", node.ID, peer.Status)
			}
		}
		node.Mu.RUnlock()
		fmt.Printf("Node %s correctly marked E as Dead.\n", node.ID)
	}

	// Scenario 3: Simulate node E coming back
	nodes[4] = NewGossipNode("E", addresses[4], 2*time.Second, 3)
	for i := 0; i < 4; i++ {
		nodes[4].Peers[string(rune('A'+i))] = &NodeInfo{
			ID:      string(rune('A' + i)),
			Address: addresses[i],
			Status:  Alive,
		}
		nodes[i].Peers["E"] = &NodeInfo{
			ID:      "E",
			Address: addresses[4],
			Status:  Dead, // initially marked dead
		}
	}
	nodes[4].Start()
	fmt.Println("Node E restarted.")
	time.Sleep(10 * time.Second)

	// Verify that node E is eventually marked Alive again
	for _, node := range nodes[:4] {
		node.Mu.RLock()
		if peer, ok := node.Peers["E"]; ok {
			if peer.Status != Alive {
				t.Errorf("Node %s sees E as %v, expected Alive after restart", node.ID, peer.Status)
			}
		}
		node.Mu.RUnlock()
		fmt.Printf("Node %s correctly marked E as Alive after restart.\n", node.ID)
	}
}
