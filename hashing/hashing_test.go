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

	// time.Sleep(2 * time.Second) // allow gossip to converge
	return nodes
}

func TestReplicationAndRecoveryAfterFailure(t *testing.T) {
	nodes := createTestCluster(t, 5, 9200, 3)

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
	// time.Sleep(2 * time.Second) // replication window

	// Step 2: Kill node[0]
	nodes[4].Stop()
	fmt.Println("Simulated failure: node stopped")
	// t.Log("Simulated failure: node[0] stopped")

	time.Sleep(2 * time.Second) // wait for gossip update

	// Step 3: Retrieve from node[3]
	conn2, err := net.Dial("tcp", nodes[2].Address)
	if err != nil {
		t.Fatalf("Connect to node[2] failed: %v", err)
	}
	defer conn2.Close()

	val, err = nodes[2].Retrieve(key, 3)
	if err != nil {
		t.Fatalf("Retrieve failed: %v", err)
	}
	if val != value {
		t.Fatalf("Expected %q, got %q", value, val)
	} else {
		t.Logf("Successfully retrieved value from node[3]: %s", val)
	}

	// Step 4: Update the value for the same key
	// updatedValue := "updatedHighAvailability"
	// err = nodes[1].Update(key, updatedValue, 3)
	// if err != nil {
	// 	t.Fatalf("Update failed: %v", err)
	// }
	// time.Sleep(2 * time.Second) // give time for replication

	// Step 5: Retrieve from node[1] to confirm update
	// val, err = nodes[1].Retrieve(key, 3)
	// if err != nil {
	// 	t.Fatalf("Retrieve after update failed: %v", err)
	// }
	// if val != updatedValue {
	// 	t.Fatalf("Update failed, expected %q, got %q", updatedValue, val)
	// } else {
	// 	t.Logf("Update confirmed on node[1]: %s", val)
	// }

	// Step 6: Delete the key from all replicas
	// err = nodes[2].Delete(key, 3)
	// if err != nil {
	// 	t.Fatalf("Delete failed: %v", err)
	// }
	// time.Sleep(1 * time.Second) // allow deletion to propagate

	// Step 7: Try retrieving again (should fail)
	// _, err = nodes[3].Retrieve(key, 3)
	// if err == nil {
	// 	t.Fatalf("Expected error on retrieving deleted key, but got value")
	// } else {
	// 	t.Logf("Confirmed deletion on node[3], retrieve error: %v", err)
	// }

	// add values in node 1
	nodes[1].Store("key1", "value1", 3)
	nodes[1].Store("key2", "value2", 3)
	nodes[1].Store("key3", "value3", 3)
	nodes[2].Store("key4", "value4", 3)

	val, err = nodes[1].Retrieve("key1", 3)
	if err != nil {
		t.Fatalf("Retrieve failed: %v", err)
	} else {
		t.Logf("Successfully retrieved value from node[1]: %s", val)
	}

	// Step 8: Print all key-values from node[1]
	allData := nodes[0].GetAllLocalData()
	t.Logf("All local data on node: %+v", allData)

}
