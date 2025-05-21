package gossip

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"
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
}

func NewGossipNode(id, address string, interval time.Duration, fanout int) *GossipNode {
	return &GossipNode{
		ID:       id,
		Address:  address,
		Peers:    make(map[string]*NodeInfo),
		Interval: interval,
		Fanout:   fanout,
	}
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

func (n *GossipNode) announceSelf() {
	n.Mu.RLock()
	defer n.Mu.RUnlock()

	for id, peer := range n.Peers {
		if id == n.ID || peer.Status == Dead {
			continue
		}
		fmt.Printf("Node %s announcing itself to %s\n", n.ID, id)
		go n.sendGossip(peer)
	}
}

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

// func (n *GossipNode) Stop() {
// 	close(n.stopCh) // signal stop
// 	if n.listener != nil {
// 		_ = n.listener.Close() // close listener to unblock Accept()
// 	}
// 	n.wg.Wait() // wait for listen() to exit
// }

func (n *GossipNode) handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	decoder := json.NewDecoder(reader)
	var msg GossipMessage
	if err := decoder.Decode(&msg); err != nil {
		fmt.Println("Failed to decode message")
		return
	}
	fmt.Printf("Node %s received gossip from %s\n", n.ID, msg.SenderID)
	fmt.Fprintf(conn, "ACK from %s\n", n.ID)
	n.mergePeerInfo(msg)
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
		}
	}
}
