package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/spf13/pflag"
)

const (
	DeletedValue  = -1
	MaxRetryCount = 10
)

var (
	nodeID     = pflag.Int("id", 0, "Node ID")
	nodesCount = pflag.Int("n", 3, "Number of nodes")
)

type NodeState struct {
	NodeID int          `json:"node_id"`
	Data   map[int]Pair `json:"data"`
}

type Request struct {
	Key   int `json:"key"`
	Value int `json:"value"`
}

type VResponse struct {
	Value int `json:"value"`
}

type Pair struct {
	Timestamp int `json:"timestamp"`
	Value     int `json:"value"`
}

type Node struct {
	selfID int

	nodes     []string
	invisible map[int]bool // for conflict testing
	data      map[int]Pair
	mutex     sync.RWMutex
}

func NewNode(nodeID, nodesCount int) *Node {
	nodes := make([]string, nodesCount)
	invisible := make(map[int]bool)
	for i := 0; i < nodesCount; i++ {
		nodes[i] = fmt.Sprintf("http://localhost:%d", 8080+i)
		invisible[i] = false
	}

	node := Node{
		selfID:    nodeID,
		invisible: invisible,
		nodes:     nodes,
		data:      make(map[int]Pair),
	}

	for i := 0; i < nodesCount; i++ {
		if i != nodeID {
			resp, err := http.Get(nodes[i] + "/state")
			if err != nil {
				continue
			}
			defer resp.Body.Close()

			var state NodeState
			err = json.NewDecoder(resp.Body).Decode(&state)
			if err != nil {
				continue
			}

			node.Conflict(state.NodeID, state.Data)
		}
	}

	return &node
}

func (n *Node) GetStates() {
	i := 0
	for {
		if i != *nodeID {
			resp, err := http.Get(n.nodes[i] + "/state")
			if err != nil {
				continue
			}
			defer resp.Body.Close()

			var state NodeState
			err = json.NewDecoder(resp.Body).Decode(&state)
			if err != nil {
				continue
			}

			n.mutex.Lock()
			n.Conflict(state.NodeID, state.Data)
			n.mutex.Unlock()
		}
		time.Sleep(3 * time.Second)

		i = (i + 1) % *nodesCount
	}
}

func (n *Node) Conflict(replicaID int, data map[int]Pair) {
	for k, v := range data {
		pair, ok := n.data[k]
		if !ok || pair.Timestamp < v.Timestamp || (pair.Timestamp == v.Timestamp && replicaID < n.selfID) {
			n.data[k] = Pair{v.Timestamp, v.Value}
		}
	}
}

func (n *Node) Patch(w http.ResponseWriter, r *http.Request) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	var requests []Request
	err := json.NewDecoder(r.Body).Decode(&requests)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	for i := 0; i < len(requests); i++ {
		pair, ok := n.data[requests[i].Key]
		timestamp := 1
		if ok {
			timestamp += pair.Timestamp
		}
		n.data[requests[i].Key] = Pair{timestamp, requests[i].Value}
	}

	quorumCount, retryCount := 1, 0
	isReplicated := make([]bool, *nodesCount)

	var replicaBody *bytes.Reader
	for quorumCount < *nodesCount && retryCount < MaxRetryCount {
		retryCount++
		for i := 0; i < *nodesCount; i++ {
			if i != n.selfID && !n.invisible[i] {
				if !isReplicated[i] {
					replica, _ := json.Marshal(NodeState{NodeID: n.selfID, Data: n.data})

					replicaBody = bytes.NewReader(replica)

					resp, err := http.Post(n.nodes[i]+"/replication", "application/json", replicaBody)
					if err != nil {
						continue
					}
					defer resp.Body.Close()

					if resp.StatusCode == http.StatusOK {
						isReplicated[i] = true
						quorumCount++
					}
				}
			}
		}
	}
}

func (n *Node) Read(w http.ResponseWriter, r *http.Request) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	vars := mux.Vars(r)
	resourceID, ok := vars["resourceID"]
	if !ok {
		http.Error(w, "add resourceID", http.StatusBadRequest)
		return
	}

	key, err := strconv.Atoi(resourceID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	pair, ok := n.data[key]

	if !ok || pair.Value == DeletedValue {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	err = json.NewEncoder(w).Encode(VResponse{Value: pair.Value})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

func (n *Node) Delete(w http.ResponseWriter, r *http.Request) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	vars := mux.Vars(r)
	resourceID, ok := vars["resourceID"]
	if !ok {
		http.Error(w, "add resourceID", http.StatusBadRequest)
		return
	}

	key, err := strconv.Atoi(resourceID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	pair, ok := n.data[key]
	if !ok || pair.Value == DeletedValue {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}

	n.data[key] = Pair{pair.Timestamp + 1, DeletedValue}

	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	err = json.NewEncoder(w).Encode(VResponse{Value: pair.Value})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	quorumCount, retryCount := 1, 0
	isReplicated := make([]bool, *nodesCount)

	var replicaBody *bytes.Reader
	for quorumCount < *nodesCount && retryCount < MaxRetryCount {
		retryCount++
		for i := 0; i < *nodesCount; i++ {
			if i != n.selfID && !n.invisible[i] {
				if !isReplicated[i] {
					replica, _ := json.Marshal(NodeState{NodeID: n.selfID, Data: n.data})

					replicaBody = bytes.NewReader(replica)

					resp, err := http.Post(n.nodes[i]+"/replication", "application/json", replicaBody)
					if err != nil {
						continue
					}
					defer resp.Body.Close()

					if resp.StatusCode == http.StatusOK {
						isReplicated[i] = true
						quorumCount++
					}
				}
			}
		}
	}
}

func (n *Node) State(w http.ResponseWriter, r *http.Request) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	err := json.NewEncoder(w).Encode(NodeState{Data: n.data})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

func (n *Node) Replication(w http.ResponseWriter, r *http.Request) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	var replica NodeState
	err := json.NewDecoder(r.Body).Decode(&replica)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	n.Conflict(replica.NodeID, replica.Data)
}

func (n *Node) Invisible(w http.ResponseWriter, r *http.Request) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	vars := mux.Vars(r)
	resourceID, ok := vars["resourceID"]
	if !ok {
		http.Error(w, "add resourceID", http.StatusBadRequest)
		return
	}

	key, err := strconv.Atoi(resourceID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	v, _ := n.invisible[key]
	if v {
		n.invisible[key] = false
	} else {
		n.invisible[key] = true
	}
}

func main() {
	pflag.Parse()

	node := NewNode(*nodeID, *nodesCount)

	server := mux.NewRouter()

	// internal
	server.HandleFunc("/state", node.State)
	server.HandleFunc("/replication", node.Replication)

	go func() {
		node.GetStates()
	}()

	// external
	server.HandleFunc("/patch", node.Patch)
	server.HandleFunc("/read/{resourceID}", node.Read)
	server.HandleFunc("/delete/{resourceID}", node.Delete)

	// testing
	server.HandleFunc("/invisible/{resourceID}", node.Invisible)

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", 8080+(*nodeID)), server))
}
