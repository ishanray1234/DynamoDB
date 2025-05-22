package hashing

import (
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
)

type HashRing struct {
	nodes    map[uint32]string
	sorted   []uint32
	replicas int
	mu       sync.RWMutex
}

func NewHashRing(replicas int) *HashRing {
	return &HashRing{
		nodes:    make(map[uint32]string),
		replicas: replicas,
	}
}

func (hr *HashRing) AddNode(nodeID string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	for i := 0; i < hr.replicas; i++ {
		hash := hr.hashKey(nodeID + strconv.Itoa(i))
		hr.nodes[hash] = nodeID
		hr.sorted = append(hr.sorted, hash)
	}
	sort.Slice(hr.sorted, func(i, j int) bool { return hr.sorted[i] < hr.sorted[j] })
}

func (hr *HashRing) RemoveNode(nodeID string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	newSorted := make([]uint32, 0, len(hr.sorted))
	for i := 0; i < hr.replicas; i++ {
		hash := hr.hashKey(nodeID + strconv.Itoa(i))
		delete(hr.nodes, hash)
	}
	for _, h := range hr.sorted {
		if hr.nodes[h] != "" {
			newSorted = append(newSorted, h)
		}
	}
	hr.sorted = newSorted
}

// single hashing function
//
//	func (hr *HashRing) GetNode(key string) string {
//		hr.mu.RLock()
//		defer hr.mu.RUnlock()
//		if len(hr.sorted) == 0 {
//			return ""
//		}
//		hash := hr.hashKey(key)
//		idx := sort.Search(len(hr.sorted), func(i int) bool {
//			return hr.sorted[i] >= hash
//		})
//		if idx == len(hr.sorted) {
//			idx = 0
//		}
//		return hr.nodes[hr.sorted[idx]]
//	}
func (hr *HashRing) GetNodes(key string, m int) []string {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	if len(hr.sorted) == 0 || m <= 0 {
		return nil
	}
	hash := hr.hashKey(key)
	idx := sort.Search(len(hr.sorted), func(i int) bool {
		return hr.sorted[i] >= hash
	})
	nodes := []string{}
	seen := map[string]bool{}
	for i := 0; len(nodes) < m && i < len(hr.sorted); i++ {
		currIdx := (idx + i) % len(hr.sorted)
		node := hr.nodes[hr.sorted[currIdx]]
		if !seen[node] {
			seen[node] = true
			nodes = append(nodes, node)
		}
	}
	return nodes
}

func (hr *HashRing) hashKey(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}
