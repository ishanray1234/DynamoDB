package hashing_test

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/ishanray1234/DynamoDB/gossip"
)

func createTestCluster(t *testing.T, count int, basePort int, replicationFactor int) []*gossip.GossipNode {
	nodes := make([]*gossip.GossipNode, count)
	addresses := make([]string, count)

	for i := 0; i < count; i++ {
		addresses[i] = fmt.Sprintf("127.0.0.1:%d", basePort+i)
	}

	for i := 0; i < count; i++ {
		id := fmt.Sprintf("node%d", i+1)
		nodes[i] = gossip.NewGossipNode(id, addresses[i], 1*time.Second, replicationFactor)
		nodes[i].Peers = make(map[string]*gossip.NodeInfo)

		for j := 0; j < count; j++ {
			if i != j {
				nodes[i].Peers[fmt.Sprintf("node%d", j+1)] = &gossip.NodeInfo{
					ID:      fmt.Sprintf("node%d", j+1),
					Address: addresses[j],
					Status:  gossip.Alive,
				}
			}
		}
	}

	for _, node := range nodes {
		go node.Start()
		time.Sleep(200 * time.Millisecond)
	}

	time.Sleep(4 * time.Second) // allow gossip to converge
	return nodes
}

func TestReplicationAndRecoveryAfterFailure(t *testing.T) {
	nodes := createTestCluster(t, 5, 9200, 3)

	// defer func() {
	// 	for _, n := range nodes {
	// 		n.Stop()
	// 	}
	// }()

	//run the BuildHashRing function on all nodes
	for _, node := range nodes {
		node.BuildHashRing()
	}

	key := "criticalKey"
	value := "highAvailability"

	// Step 1: Store on node[0]
	conn, err := net.Dial("tcp", nodes[0].Address)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer conn.Close()

	// storeReq := gossip.StoreRequest{Key: key, Value: value}
	// storeData, _ := json.Marshal(storeReq)

	// if _, err = conn.Write([]byte("STORE\n")); err != nil {
	// 	t.Fatalf("Send STORE command failed: %v", err)
	// }
	// if _, err = conn.Write(storeData); err != nil {
	// 	t.Fatalf("Send STORE data failed: %v", err)
	// }

	err = nodes[0].Store(key, value, 3)
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}
	val, err := nodes[0].Retrieve(key, 3)
	if err != nil {
		t.Fatalf("Retrieve failed: %v", err)
	}
	if val != value {
		t.Fatalf("Expected %q, got %q", value, val)
	} else {
		println("Successfully stored value on node[0]:", val)
	}
	time.Sleep(2 * time.Second) // replication window

	// Step 2: Kill node[0]
	nodes[4].Stop()
	fmt.Println("Simulated failure: node stopped")
	// t.Log("Simulated failure: node[0] stopped")

	time.Sleep(8 * time.Second) // wait for gossip update

	// Step 3: Retrieve from node[3]
	conn2, err := net.Dial("tcp", nodes[2].Address)
	if err != nil {
		t.Fatalf("Connect to node[2] failed: %v", err)
	}
	defer conn2.Close()

	// retrieveReq := gossip.RetrieveRequest{Key: key}
	// retrieveData, _ := json.Marshal(retrieveReq)

	// if _, err = conn2.Write([]byte("RETRIEVE\n")); err != nil {
	// 	t.Fatalf("Send RETRIEVE command failed: %v", err)
	// }
	// if _, err = conn2.Write(retrieveData); err != nil {
	// 	t.Fatalf("Send RETRIEVE payload failed: %v", err)
	// }

	val, err = nodes[2].Retrieve(key, 3)
	if err != nil {
		t.Fatalf("Retrieve failed: %v", err)
	}
	if val != value {
		t.Fatalf("Expected %q, got %q", value, val)
	} else {
		t.Logf("Successfully retrieved value from node[3]: %s", val)
	}

	// var resp gossip.RetrieveResponse
	// decoder := json.NewDecoder(conn2)
	// if err := decoder.Decode(&resp); err != nil {
	// 	t.Fatalf("Decode response failed: %v", err)
	// }

	// if !resp.Found {
	// 	t.Fatal("Key not found in surviving node")
	// }
	// if resp.Value != value {
	// 	t.Errorf("Expected %q, got %q", value, resp.Value)
	// } else {
	// 	t.Logf("Successfully retrieved value from node[3]: %s", resp.Value)
	// }
}
