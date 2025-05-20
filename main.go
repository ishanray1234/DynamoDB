package main

import (
	"time"

	"github.com/ishanray1234/DynamoDB/gossip"
)

func main() {
	// node := gossip.NewGossipNode("Node1", "127.0.0.1:8001", 3*time.Second)
	// node.Peers["Node2"] = &gossip.NodeInfo{
	// 	ID: "Node2", Address: "127.0.0.1:8002", Status: gossip.Alive,
	// }
	node := gossip.NewGossipNode("Node2", "127.0.0.1:8002", 3*time.Second)
	node.Peers["Node1"] = &gossip.NodeInfo{
		ID: "Node1", Address: "127.0.0.1:8001", Status: gossip.Alive,
	}

	node.Start()
	select {} // Run forever
}
