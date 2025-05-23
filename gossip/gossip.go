package gossip

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/ishanray1234/DynamoDB/hashing"
)

type NodeStatus int

const (
	Alive NodeStatus = iota
	Suspected
	Dead
)

type NodeInfo struct {
	ID       string
	Address  string
	Status   NodeStatus
	LastSeen time.Time
}

type GossipMessage struct {
	SenderID   string
	SenderAddr string
	Peers      map[string]NodeInfo
}

type GossipNode struct {
	ID       string
	Address  string
	Peers    map[string]*NodeInfo
	Mu       sync.RWMutex
	Interval time.Duration
	Fanout   int          // Number of random peers to gossip to
	listener net.Listener // Keep a reference to the listener
	stopCh   chan struct{}
	wg       sync.WaitGroup

	// Hashing
	Ring    *hashing.HashRing
	Storage map[string]string
	storeMu sync.RWMutex
}

func NewGossipNode(id, address string, interval time.Duration, fanout int) *GossipNode {
	node := &GossipNode{
		ID:       id,
		Address:  address,
		Peers:    make(map[string]*NodeInfo),
		Interval: interval,
		Fanout:   fanout,
		Ring:     hashing.NewHashRing(3), // 3 virtual nodes per physical node
		Storage:  make(map[string]string),
	}
	// node.Ring.AddNode(id)
	return node
}

func (n *GossipNode) Start() {
	n.stopCh = make(chan struct{})
	n.wg = sync.WaitGroup{}

	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		n.gossipLoop()
	}()

	n.announceSelfOnce()

	go n.listen()

	go func() {
		for {
			select {
			case <-n.stopCh:
				return
			default:
				n.checkForDeadNodes(2 * n.Interval)
				time.Sleep(n.Interval)
			}
		}
	}()

	//Announce self to all known peers after a small delay
	// go func() {
	// 	time.Sleep(2 * time.Second) // Give listener time to start
	// 	n.announceSelf()
	// }()
}

func (node *GossipNode) announceSelfOnce() {
	fanout := 3 // You can tweak this based on network size
	peerCount := len(node.Peers)

	if peerCount == 0 {
		fmt.Println("No peers to announce to.")
		return
	}

	// Shuffle peers
	keys := make([]string, 0, peerCount)
	for k := range node.Peers {
		keys = append(keys, k)
	}
	shuffled := rand.Perm(peerCount)
	for i := 0; i < fanout && i < peerCount; i++ {
		peer := node.Peers[keys[shuffled[i]]]
		go node.sendGossip(peer) // Send gossip to selected peers
	}

	fmt.Printf("Announced to %d peer(s) at bootstrap.\n", min(fanout, peerCount))
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// func (n *GossipNode) announceSelf() {
// 	n.Mu.RLock()
// 	defer n.Mu.RUnlock()

// 	for id, peer := range n.Peers {
// 		if id == n.ID || peer.Status == Dead {
// 			continue
// 		}
// 		fmt.Printf("Node %s announcing itself to %s\n", n.ID, id)
// 		go n.sendGossip(peer)
// 	}
// }

func (n *GossipNode) Stop() {
	close(n.stopCh)
	if n.listener != nil {
		_ = n.listener.Close()
	}
	n.wg.Wait()
}

func (n *GossipNode) listen() {
	fmt.Printf("Node %s listening on %s\n", n.ID, n.Address)
	var err error
	n.listener, err = net.Listen("tcp", n.Address)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", n.Address, err)
	}
	log.Printf("Node %s listening on %s", n.ID, n.Address)

	n.wg.Add(1)
	defer n.wg.Done()

	for {
		conn, err := n.listener.Accept()
		if err != nil {
			select {
			case <-n.stopCh:
				return // listener was closed
			default:
				log.Printf("Accept error: %v", err)
				continue
			}
		}
		go n.handleConnection(conn)
	}
}

// old Stop function
// func (n *GossipNode) Stop() {
// 	close(n.stopCh) // signal stop
// 	if n.listener != nil {
// 		_ = n.listener.Close() // close listener to unblock Accept()
// 	}
// 	n.wg.Wait() // wait for listen() to exit
// }

//old handleConnection function
// func (n *GossipNode) handleConnection(conn net.Conn) {
// 	defer conn.Close()
// 	reader := bufio.NewReader(conn)
// 	decoder := json.NewDecoder(reader)
// 	var msg GossipMessage
// 	if err := decoder.Decode(&msg); err != nil {
// 		fmt.Println("Failed to decode message")
// 		return
// 	}
// 	fmt.Printf("Node %s received gossip from %s\n", n.ID, msg.SenderID)
// 	fmt.Fprintf(conn, "ACK from %s\n", n.ID)
// 	n.mergePeerInfo(msg)
// }

func (n *GossipNode) handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	command, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println(command)
		fmt.Println("Failed to read command:", err)
		return
	}
	// command = command[:len(command)-1] // remove newline
	command = strings.TrimSpace(command)
	// print("Command received: ", command)

	decoder := json.NewDecoder(reader)
	switch command {
	case "STORE":
		var req StoreRequest
		if err := decoder.Decode(&req); err != nil {
			fmt.Println("Failed to decode STORE request:", err)
			return
		}
		n.storeKey(req.Key, req.Value)
		fmt.Fprintln(conn, "STORED")

	case "RETRIEVE":
		var req RetrieveRequest
		if err := decoder.Decode(&req); err != nil {
			fmt.Println("Failed to decode RETRIEVE request:", err)
			return
		}
		val, found := n.retrieveKey(req.Key)
		resp := RetrieveResponse{Value: val, Found: found}
		json.NewEncoder(conn).Encode(resp)

	case "DELETE":
		var req DeleteRequest
		json.NewDecoder(conn).Decode(&req)
		n.deleteKey(req.Key)
		fmt.Fprintln(conn, "DELETED")

	default:
		var msg GossipMessage
		if err := decoder.Decode(&msg); err != nil {
			fmt.Println("Failed to decode gossip message")
			return
		}
		fmt.Printf("Node %s received gossip from %s\n", n.ID, msg.SenderID)
		fmt.Fprintf(conn, "ACK from %s\n", n.ID)
		n.mergePeerInfo(msg)
	}
	// decoder := json.NewDecoder(reader)
	// var msg GossipMessage
	// if err := decoder.Decode(&msg); err != nil {
	// 	fmt.Println("Failed to decode message")
	// 	return
	// }
	// fmt.Printf("Node %s received gossip from %s\n", n.ID, msg.SenderID)
	// fmt.Fprintf(conn, "ACK from %s\n", n.ID)
	// n.mergePeerInfo(msg)
}

func (n *GossipNode) gossipLoop() {
	for {
		select {
		case <-n.stopCh:
			return
		default:
			n.Mu.RLock()
			peers := make([]*NodeInfo, 0, len(n.Peers))
			for _, p := range n.Peers {
				if p.Status == Alive {
					peers = append(peers, p)
				}
			}
			n.Mu.RUnlock()

			if len(peers) > 0 {
				perm := rand.Perm(len(peers))
				for i := 0; i < n.Fanout && i < len(peers); i++ {
					target := peers[perm[i]]
					fmt.Printf("Node %s gossiping to %s\n", n.ID, target.ID)
					go n.sendGossip(target)
				}
			}
			time.Sleep(n.Interval)
		}
	}
}

func (n *GossipNode) sendGossip(target *NodeInfo) {
	conn, err := net.Dial("tcp", target.Address)
	if err != nil {
		n.updatePeer(target.ID, target.Address, Suspected)
		fmt.Printf("Failed to connect to %s\n", target.ID)
		return
	}
	defer conn.Close()

	msg := GossipMessage{
		SenderID:   n.ID,
		SenderAddr: n.Address,
		Peers:      n.copyPeers(),
	}

	fmt.Fprintln(conn, "GOSSIP")
	encoder := json.NewEncoder(conn)
	if err := encoder.Encode(msg); err != nil {
		fmt.Println("Failed to send gossip")
		return
	}

	reader := bufio.NewReader(conn)
	ack, _ := reader.ReadString('\n')
	fmt.Printf("ACK received at %s from %s: %s", n.ID, target.ID, ack)
	n.updatePeer(target.ID, target.Address, Alive)
}

func (n *GossipNode) mergePeerInfo(msg GossipMessage) {
	n.updatePeer(msg.SenderID, msg.SenderAddr, Alive)

	n.Mu.Lock()
	defer n.Mu.Unlock()

	for id, info := range msg.Peers {
		if id == n.ID {
			continue
		}
		existing, ok := n.Peers[id]
		if !ok || existing.LastSeen.Before(info.LastSeen) {
			n.Peers[id] = &NodeInfo{
				ID:       id,
				Address:  info.Address,
				Status:   info.Status,
				LastSeen: info.LastSeen,
			}
		}
	}
}

func (n *GossipNode) updatePeer(id, address string, status NodeStatus) {
	n.Mu.Lock()
	defer n.Mu.Unlock()
	peer, ok := n.Peers[id]
	if !ok {
		n.Peers[id] = &NodeInfo{
			ID:       id,
			Address:  address,
			Status:   status,
			LastSeen: time.Now(),
		}
		n.Ring.AddNode(id)
	} else {
		peer.Status = status
		peer.LastSeen = time.Now()
		if address != "" {
			peer.Address = address
		}
	}
}

func (n *GossipNode) copyPeers() map[string]NodeInfo {
	n.Mu.RLock()
	defer n.Mu.RUnlock()
	copy := make(map[string]NodeInfo)
	for id, p := range n.Peers {
		copy[id] = *p
	}
	return copy
}

func (n *GossipNode) checkForDeadNodes(timeout time.Duration) {
	n.Mu.Lock()
	defer n.Mu.Unlock()
	for _, peer := range n.Peers {
		if time.Since(peer.LastSeen) > timeout && peer.Status != Dead {
			fmt.Printf("Node %s marking %s as Dead\n", n.ID, peer.ID)
			peer.Status = Dead
			n.Ring.RemoveNode(peer.ID)

			// fmt.Printf("Node %s rebalancing data from %s\n", n.ID, peer.ID)
			// deadNodeData := n.getDataForNode(peer.ID) // You need to implement this to fetch keys stored on dead node
			// //print deadNodeData data
			// fmt.Println("deadNodeData:", deadNodeData)
			// n.RebalanceData(peer.ID, deadNodeData)
		}
	}
}

// *********STORE IMPLEMENTATION*********//
type StoreRequest struct {
	Key   string
	Value string
}

type RetrieveRequest struct {
	Key string
}

type RetrieveResponse struct {
	Value string
	Found bool
}

type DeleteRequest struct {
	Key string
}

func (n *GossipNode) BuildHashRing() {
	n.Ring = hashing.NewHashRing(3) // Re-initialize ring
	n.Ring.AddNode(n.ID)
	for peerID := range n.Peers {
		n.Ring.AddNode(peerID)
	}
}

func (n *GossipNode) storeKey(key, value string) {
	n.storeMu.Lock()
	defer n.storeMu.Unlock()
	n.Storage[key] = value
}

func (n *GossipNode) retrieveKey(key string) (string, bool) {
	n.storeMu.RLock()
	defer n.storeMu.RUnlock()
	val, ok := n.Storage[key]
	fmt.Println("n:", n.ID)
	fmt.Println("val:", val)
	return val, ok
}

func (n *GossipNode) Store(key, value string, m int) error {
	replicaNodes := n.Ring.GetNodes(key, m)
	for _, nodeID := range replicaNodes {
		if nodeID == n.ID {
			n.storeKey(key, value)
			continue
		}
		peer, ok := n.Peers[nodeID]
		if !ok || peer.Status != Alive {
			fmt.Printf("peer %s not available for storing key %s\n", nodeID, key)
			continue
		}
		conn, err := net.Dial("tcp", peer.Address)
		if err != nil {
			fmt.Printf("Failed to connect to %s: %v\n", peer.ID, err)
			continue
		}
		fmt.Fprintln(conn, "STORE")
		json.NewEncoder(conn).Encode(StoreRequest{Key: key, Value: value})
		ack, _ := bufio.NewReader(conn).ReadString('\n')
		conn.Close()
		if ack != "STORED\n" {
			fmt.Printf("Failed to store on node %s\n", nodeID)
		}
	}
	return nil
}

func (n *GossipNode) Retrieve(key string, m int) (string, error) {
	fmt.Println("Retrieve key:", key)
	replicaNodes := n.Ring.GetNodes(key, m)
	fmt.Println("replicaNodes:", replicaNodes)
	for _, nodeID := range replicaNodes {
		if nodeID == n.ID {
			val, ok := n.retrieveKey(key)
			if ok {
				return val, nil
			}
			continue
		}
		peer, ok := n.Peers[nodeID]
		if !ok || peer.Status != Alive {
			continue
		}
		conn, err := net.Dial("tcp", peer.Address)
		if err != nil {
			continue
		}
		fmt.Fprintln(conn, "RETRIEVE")
		json.NewEncoder(conn).Encode(RetrieveRequest{Key: key})

		var resp RetrieveResponse
		err = json.NewDecoder(conn).Decode(&resp)
		conn.Close()
		if err == nil && resp.Found {
			return resp.Value, nil
		}
	}
	return "", fmt.Errorf("key not found in any of the %d replicas", m)
}

func (n *GossipNode) Delete(key string, m int) error {
	fmt.Println("Delete key:", key)
	replicaNodes := n.Ring.GetNodes(key, m) // returns m nodes

	for _, nodeID := range replicaNodes {
		if nodeID == n.ID {
			n.deleteKey(key)
			continue
		}
		peer, ok := n.Peers[nodeID]
		if !ok || peer.Status != Alive {
			continue
		}
		conn, err := net.Dial("tcp", peer.Address)
		if err != nil {
			fmt.Printf("Delete: failed to connect to %s: %v\n", peer.ID, err)
			continue
		}
		defer conn.Close()

		fmt.Fprintln(conn, "DELETE")
		err = json.NewEncoder(conn).Encode(DeleteRequest{Key: key})
		if err != nil {
			fmt.Printf("Delete: failed to encode request for %s: %v\n", peer.ID, err)
			continue
		}
		ack, _ := bufio.NewReader(conn).ReadString('\n')
		if ack != "DELETED\n" {
			fmt.Printf("Delete: failed to delete on node %s\n", nodeID)
		}
		fmt.Printf("Delete: deleted key %s from node %s\n", key, nodeID)
	}
	return nil
}

func (n *GossipNode) deleteKey(key string) {
	n.storeMu.Lock()
	defer n.storeMu.Unlock()
	delete(n.Storage, key)
}

func (n *GossipNode) Update(key, newValue string, m int) error {
	replicaNodes := n.Ring.GetNodes(key, m)

	for _, nodeID := range replicaNodes {
		if nodeID == n.ID {
			n.storeKey(key, newValue)
			continue
		}
		peer, ok := n.Peers[nodeID]
		if !ok || peer.Status != Alive {
			continue
		}
		conn, err := net.Dial("tcp", peer.Address)
		if err != nil {
			fmt.Printf("Update: failed to connect to %s: %v\n", peer.ID, err)
			continue
		}
		defer conn.Close()

		fmt.Fprintln(conn, "STORE")
		json.NewEncoder(conn).Encode(StoreRequest{Key: key, Value: newValue})
		ack, _ := bufio.NewReader(conn).ReadString('\n')
		if ack != "STORED\n" {
			fmt.Printf("Update: failed to store on node %s\n", nodeID)
		}
	}
	return nil
}

func (n *GossipNode) GetAllLocalData() map[string]string {
	n.storeMu.RLock()
	defer n.storeMu.RUnlock()
	fmt.Println("GetAllLocalData n.Storage:", n.Storage)
	result := make(map[string]string)

	// maps.Copy(result, n.Storage)
	for key, value := range n.Storage {
		result[key] = value
		fmt.Println(":", key, value)
	}

	return result
}

//single hashing function
// func (n *GossipNode) Store(key, value string) error {
// 	responsibleNode := n.Ring.GetNodes(key)
// 	if responsibleNode == n.ID {
// 		n.storeKey(key, value)
// 		return nil
// 	}
// 	peer, ok := n.Peers[responsibleNode]
// 	if !ok || peer.Status != Alive {
// 		return fmt.Errorf("responsible node %s not available", responsibleNode)
// 	}
// 	conn, err := net.Dial("tcp", peer.Address)
// 	if err != nil {
// 		return err
// 	}
// 	defer conn.Close()

// 	fmt.Fprintln(conn, "STORE")
// 	json.NewEncoder(conn).Encode(StoreRequest{Key: key, Value: value})
// 	ack, _ := bufio.NewReader(conn).ReadString('\n')
// 	if ack != "STORED\n" {
// 		return fmt.Errorf("failed to store on node %s", responsibleNode)
// 	}
// 	return nil
// }

// func (n *GossipNode) Retrieve(key string) (string, error) {
// 	responsibleNode := n.Ring.GetNode(key)
// 	fmt.Println("ID & responsibleNode:", n.ID, responsibleNode)
// 	if responsibleNode == n.ID {
// 		fmt.Println("responsibleNode == n.ID")
// 		val, ok := n.retrieveKey(key)
// 		if !ok {
// 			return "", fmt.Errorf("1.key not found")
// 		}
// 		return val, nil
// 	}
// 	peer, ok := n.Peers[responsibleNode]
// 	if !ok || peer.Status != Alive {
// 		return "", fmt.Errorf("responsible node %s not available", responsibleNode)
// 	}
// 	conn, err := net.Dial("tcp", peer.Address)
// 	if err != nil {
// 		return "", err
// 	}
// 	defer conn.Close()

// 	fmt.Fprintln(conn, "RETRIEVE")
// 	json.NewEncoder(conn).Encode(RetrieveRequest{Key: key})

// 	var resp RetrieveResponse
// 	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
// 		return "", err
// 	}
// 	if !resp.Found {
// 		return "", fmt.Errorf("2.key not found")
// 	}
// 	return resp.Value, nil
// }

// func (n *GossipNode) getDataForNode(nodeID string) map[string]string {
// 	n.storeMu.RLock()
// 	defer n.storeMu.RUnlock()

// 	data := make(map[string]string)
// 	for key, value := range n.Storage {
// 		if n.Ring.GetNode(key) == nodeID {
// 			data[key] = value
// 		}
// 	}
// 	return data
// }

// func (n *GossipNode) RebalanceData(removedNodeID string, removedData map[string]string) {
// 	for key, value := range removedData {
// 		newOwner := n.Ring.GetNode(key)
// 		if newOwner == n.ID {
// 			n.storeKey(key, value)
// 		} else if peer, ok := n.Peers[newOwner]; ok && peer.Status == Alive {
// 			conn, err := net.Dial("tcp", peer.Address)
// 			if err != nil {
// 				fmt.Printf("Failed to connect to %s during rebalance: %v\n", newOwner, err)
// 				continue
// 			}

// 			fmt.Fprintln(conn, "STORE")
// 			json.NewEncoder(conn).Encode(StoreRequest{Key: key, Value: value})
// 			ack, _ := bufio.NewReader(conn).ReadString('\n')
// 			if ack != "STORED\n" {
// 				fmt.Printf("Failed to store %s on new owner %s\n", key, newOwner)
// 			}
// 			fmt.Printf("Rebalanced %s to %s\n", key, newOwner)
// 			conn.Close()
// 		}
// 	}
// }
