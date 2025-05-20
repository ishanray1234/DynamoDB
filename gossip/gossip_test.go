package gossip

import (
	"testing"
	"time"
)

func TestGossipCommunication(t *testing.T) {
	nodeA := NewGossipNode("A", "127.0.0.1:9001", 2*time.Second)
	nodeB := NewGossipNode("B", "127.0.0.1:9002", 2*time.Second)

	nodeA.Peers["B"] = &NodeInfo{
		ID: "B", Address: "127.0.0.1:9002", Status: Alive,
	}
	nodeB.Peers["A"] = &NodeInfo{
		ID: "A", Address: "127.0.0.1:9001", Status: Alive,
	}

	nodeA.Start()
	nodeB.Start()

	time.Sleep(5 * time.Second)

	if nodeA.Peers["B"].Status != Alive || nodeB.Peers["A"].Status != Alive {
		t.Errorf("Nodes did not gossip successfully")
	}
}
