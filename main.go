package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/spf13/pflag"
)

const (
	Hash = 12345
)

const (
	Create = iota
	Read
	Update
	Delete

	Leader
	Candidate
	Follower
)

type Operation struct {
	Mode  int `json:"mode"`
	Term  int `json:"term"`
	Key   int `json:"key"`
	Value int `json:"value"`
}

type KVRequest struct {
	Key   int `json:"key"`
	Value int `json:"value"`
}

type VRequest struct {
	Value int `json:"value"`
}

type VResponse struct {
	Value int `json:"value"`
}

type Node struct {
	selfID   int
	leaderID int

	term  int
	state int

	nodes   []string
	history []Operation
	data    map[int]int
	mutex   sync.RWMutex
}

var (
	nodeID     = pflag.Int("id", 0, "Node ID")
	nodesCount = pflag.Int("n", 0, "Number of nodes")
)

func NewNode(nodeID, nodesCount int) *Node {
	nodes := make([]string, nodesCount)

	for i := 0; i < nodesCount; i++ {
		nodes[i] = fmt.Sprintf(":%d", 8080+i)
	}

	return &Node{
		selfID:  nodeID,
		nodes:   nodes,
		history: make([]Operation, 0),
		data:    make(map[int]int),
	}
}

func (n *Node) CheckLeader() {
	for {
		if n.state == Leader {
			time.Sleep(2 * time.Second)
			continue
		}
		resp, err := http.Get(n.nodes[n.selfID].)
		if err != nil {

		}
	}
}

//func (n *Node) FindLeader() {
//
//}

func (n *Node) Create(w http.ResponseWriter, r *http.Request) {
	var request KVRequest
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	n.mutex.Lock()
	n.data[request.Key] = request.Value
	n.mutex.Unlock()
}

func (n *Node) Read(w http.ResponseWriter, r *http.Request) {
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

	shardID := key % *nodesCount
	if shardID < n.selfID && shardID+(*nodesCount) > n.selfID {
		for i := shardID; i < shardID+(*nodesCount); i++ {
			resp, err := http.Get(n.nodes[shardID] + "/read/" + resourceID)
			if err == nil && resp.StatusCode == http.StatusOK {
				bodyBytes, err := io.ReadAll(resp.Body)
				if err == nil {
					continue
				}
				w.Header().Set("Content-Type", "application/json; charset=UTF-8")
				_, _ = w.Write(bodyBytes)
				return
			}
		}
	}

	n.mutex.RLock()
	value, ok := n.data[key]
	n.mutex.RUnlock()

	if !ok {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	err = json.NewEncoder(w).Encode(VResponse{Value: value})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

func (n *Node) Update(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	resourceID, ok := vars["resourceID"]
	if !ok {
		http.Error(w, "Need post's id", http.StatusNotFound)
		return
	}

	key, err := strconv.Atoi(resourceID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	n.mutex.RLock()
	_, ok = n.data[key]
	n.mutex.RUnlock()
	if !ok {
		http.Error(w, "Key not found", http.StatusNotFound)
	}

	var request VRequest
	err = json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	n.mutex.Lock()
	n.data[key] = request.Value
	n.mutex.Unlock()
}

func (n *Node) Delete(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	resourceID, ok := vars["resourceID"]
	if !ok {
		http.Error(w, "Need post's id", http.StatusNotFound)
		return
	}

	key, err := strconv.Atoi(resourceID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	n.mutex.RLock()
	_, ok = n.data[key]
	n.mutex.RUnlock()
	if !ok {
		http.Error(w, "Key not found", http.StatusNotFound)
	}

	n.mutex.Lock()
	delete(n.data, key)
	n.mutex.Unlock()
}

func (n *Node) IsLife(http.ResponseWriter, *http.Request) {
}

func main() {
	pflag.Parse()

	node := NewNode(*nodeID, *nodesCount)

	server := mux.NewRouter()

	// external
	server.HandleFunc("/create", node.Create)
	server.HandleFunc("/read/{resourceID}", node.Read)
	server.HandleFunc("/update/{resourceID}", node.Update)
	server.HandleFunc("/delete/{resourceID}", node.Delete)

	// internal
	//server.HandleFunc("/create", node.Create)

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", 8080+(*nodeID)), server))
}
