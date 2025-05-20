package gossip

import (
	"fmt"
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

type GossipNode struct {
	ID       string
	Address  string
	Peers    map[string]*NodeInfo
	Mu       sync.RWMutex
	Interval time.Duration
}

func NewGossipNode(id, address string, interval time.Duration) *GossipNode {
	return &GossipNode{
		ID:       id,
		Address:  address,
		Peers:    make(map[string]*NodeInfo),
		Interval: interval,
	}
}

func (n *GossipNode) Start() {
	go n.listen()
	go n.gossipLoop()
}

func (n *GossipNode) listen() {
	fmt.Printf("Node %s listening on %s\n", n.ID, n.Address)
	ln, err := net.Listen("tcp", n.Address)
	if err != nil {
		panic(err)
	}
	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}

		var msg string
		fmt.Fscan(conn, &msg)
		fmt.Fprintf(conn, "ACK from %s\n", n.ID)
		conn.Close()

		n.Mu.Lock()
		peer, ok := n.Peers[msg]
		if !ok {
			n.Peers[msg] = &NodeInfo{
				ID:       msg,
				Address:  "", // Update if needed
				Status:   Alive,
				LastSeen: time.Now(),
			}
		} else {
			peer.Status = Alive
			peer.LastSeen = time.Now()
		}
		n.Mu.Unlock()
	}
}

func (n *GossipNode) gossipLoop() {
	for {
		n.Mu.RLock()
		peers := make([]*NodeInfo, 0, len(n.Peers))
		for _, p := range n.Peers {
			peers = append(peers, p)
		}
		n.Mu.RUnlock()

		if len(peers) > 0 {
			target := peers[rand.Intn(len(peers))]
			fmt.Printf("Node %s gossiping to %s\n", n.ID, target.ID)
			go n.sendGossip(target)
		}

		time.Sleep(n.Interval)
	}
}

func (n *GossipNode) sendGossip(target *NodeInfo) {
	conn, err := net.Dial("tcp", target.Address)
	if err != nil {
		fmt.Printf("Failed to connect to %s\n", target.ID)
		n.Mu.Lock()
		target.Status = Suspected
		n.Mu.Unlock()
		return
	}
	fmt.Fprintf(conn, "%s\n", n.ID)
	fmt.Printf("Gossip sent from %s to %s\n", n.ID, target.ID)
	defer conn.Close()

	fmt.Fprintln(conn, n.ID)
}
