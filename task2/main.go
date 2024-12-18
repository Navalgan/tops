package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/spf13/pflag"
)

const (
	Create = iota
	Update
	Delete

	Leader
	Candidate
	Follower

	MaxRetryCount = 10
)

var (
	nodeID     = pflag.Int("id", 0, "Node ID")
	nodesCount = pflag.Int("n", 3, "Number of nodes")
)

type Operation struct {
	Mode  int `json:"mode"`
	Term  int `json:"term"`
	Key   int `json:"key"`
	Value int `json:"value"`
}

type NodeState struct {
	NodeID    int `json:"node_id"`
	State     int `json:"state"`
	LeaderID  int `json:"leader_id"`
	Term      int `json:"term"`
	LogLength int `json:"log_length"`
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

type ReplicationState struct {
	NodeID    int `json:"node_id"`
	Mode      int `json:"mode"`
	Term      int `json:"term"`
	LogLength int `json:"log_length"`
	PrevTerm  int `json:"prev_term"`
	Key       int `json:"key"`
	Value     int `json:"value"`
}

type OperationState struct {
	NodeID  int         `json:"node_id"`
	State   int         `json:"state"`
	Term    int         `json:"term"`
	History []Operation `json:"history"`
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

	wasHeartbeat bool
}

func NewNode(nodeID, nodesCount int) *Node {
	nodes := make([]string, nodesCount)

	for i := 0; i < nodesCount; i++ {
		nodes[i] = fmt.Sprintf("http://localhost:%d", 8080+i)
	}

	return &Node{
		selfID:       nodeID,
		nodes:        nodes,
		state:        Candidate,
		history:      make([]Operation, 0),
		data:         make(map[int]int),
		wasHeartbeat: false,
	}
}

func (n *Node) GetReaderState() *bytes.Reader {
	state, _ := json.Marshal(NodeState{
		NodeID:    n.selfID,
		State:     n.state,
		LeaderID:  n.leaderID,
		Term:      n.term,
		LogLength: len(n.history),
	})

	return bytes.NewReader(state)
}

func (n *Node) CheckLeader() {
	for {
		if n.state == Leader {
			for i := 0; i < *nodesCount; i++ {
				if i != *nodeID {
					n.mutex.Lock()
					stateBody := n.GetReaderState()
					n.mutex.Unlock()

					replicaResp, err := http.Post(n.nodes[i]+"/is_life", "application/json", stateBody)
					if err != nil {
						continue
					}

					var replicaState NodeState
					_ = json.NewDecoder(replicaResp.Body).Decode(&replicaState)
					replicaResp.Body.Close()

					n.mutex.Lock()
					if replicaState.LogLength < len(n.history) {
						historyState, _ := json.Marshal(OperationState{
							NodeID:  n.selfID,
							State:   n.state,
							Term:    n.term,
							History: n.history,
						})

						historyStateBody := bytes.NewReader(historyState)
						n.mutex.Unlock()
						_, _ = http.Post(n.nodes[i]+"/recovery", "application/json", historyStateBody)
						n.mutex.Lock()
					}
					n.mutex.Unlock()
				}
			}
			time.Sleep(time.Millisecond * 150)
			continue
		} else {
			time.Sleep((2 + time.Duration(rand.Int()%3)) * time.Second)

			n.mutex.Lock()
			fmt.Printf("Cur leader ID %d for node %d with term %d and length %d\n", n.leaderID, n.selfID, n.term, len(n.history))

			if n.wasHeartbeat {
				n.wasHeartbeat = false
				n.mutex.Unlock()
				continue
			}

			n.state = Candidate
			n.term++

			fmt.Printf("Node %d Candidate now with term %d\n", n.selfID, n.term)

			n.mutex.Unlock()

			quorumCount := 1
			for i := 0; i < *nodesCount; i++ {
				if i != *nodeID {
					n.mutex.Lock()
					if n.wasHeartbeat {
						n.mutex.Unlock()
						break
					}
					stateBody := n.GetReaderState()
					n.mutex.Unlock()

					votingResp, err := http.Post(n.nodes[i]+"/voting", "application/json", stateBody)
					if err != nil {
						continue
					}
					if votingResp.StatusCode == http.StatusOK {
						fmt.Printf("Replica %d voited for this\n", i)

						quorumCount++
					}
				}
			}

			n.mutex.Lock()
			if n.wasHeartbeat {
				n.state = Follower
				n.wasHeartbeat = false
				n.mutex.Unlock()
				continue
			}
			if quorumCount > *nodesCount/2 {
				n.state = Leader
				n.leaderID = n.selfID

				fmt.Printf("This replica is leader now\n")
			} else {
				n.state = Follower

				fmt.Printf("This replica is Follower now and retry CheckLeader\n")

				time.Sleep(time.Duration(rand.Int()%5) * time.Second)

			}
			n.mutex.Unlock()
		}
	}
}

func (n *Node) Create(w http.ResponseWriter, r *http.Request) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	if n.state != Leader {
		http.Error(w, fmt.Sprintf("Only leader can do Create operation. Leader ID %d, this ID %d", n.leaderID, n.selfID), http.StatusBadRequest)
		return
	}

	var request KVRequest
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	_, ok := n.data[request.Key]
	if ok {
		http.Error(w, "Key already exists", http.StatusBadRequest)
		return
	}

	curLength := len(n.history)
	prevTerm := -1
	if curLength > 0 {
		prevTerm = n.history[curLength-1].Term
	}
	curTerm := n.term
	n.history = append(n.history, Operation{Mode: Create, Term: curTerm, Key: request.Key, Value: request.Value})
	n.data[request.Key] = request.Value

	quorumCount := 1
	isReplicated := make([]bool, *nodesCount)

	retryCount := 0
	var replicaBody *bytes.Reader
	for quorumCount < *nodesCount/2 && retryCount < MaxRetryCount {
		retryCount++
		for i := 0; i < *nodesCount; i++ {
			if i != n.selfID {
				if !isReplicated[i] {
					replica, _ := json.Marshal(ReplicationState{
						NodeID:    n.selfID,
						Mode:      Create,
						Term:      curTerm,
						PrevTerm:  prevTerm,
						LogLength: curLength,
						Key:       request.Key,
						Value:     request.Value,
					})

					replicaBody = bytes.NewReader(replica)

					resp, err := http.Post(n.nodes[i]+"/replication", "application/json", replicaBody)
					if err != nil {
						continue
					}
					if resp.StatusCode == http.StatusOK {
						isReplicated[i] = true
						quorumCount++
					}
				}
			}
		}
	}
	if quorumCount < *nodesCount/2 {
		http.Error(w, "Too long quorum", http.StatusRequestTimeout)
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

	value, ok := n.data[key]

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
	n.mutex.Lock()
	defer n.mutex.Unlock()

	if n.state != Leader {
		http.Error(w, fmt.Sprintf("Only leader can do Update operation. Leader ID %d, this ID %d", n.leaderID, n.selfID), http.StatusBadRequest)
		return
	}

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

	_, ok = n.data[key]
	if !ok {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}

	var request VRequest
	err = json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	n.data[key] = request.Value

	curLength := len(n.history)
	prevTerm := -1
	if curLength > 0 {
		prevTerm = n.history[curLength-1].Term
	}
	curTerm := n.term
	n.history = append(n.history, Operation{Mode: Create, Term: curTerm, Key: key, Value: request.Value})
	n.data[key] = request.Value

	quorumCount := 1
	isReplicated := make([]bool, *nodesCount)

	retryCount := 0
	var replicaBody *bytes.Reader
	for quorumCount < *nodesCount/2 && retryCount < MaxRetryCount {
		retryCount++
		for i := 0; i < *nodesCount; i++ {
			if i != n.selfID {
				if !isReplicated[i] {
					replica, _ := json.Marshal(ReplicationState{
						NodeID:    n.selfID,
						Mode:      Update,
						Term:      curTerm,
						PrevTerm:  prevTerm,
						LogLength: curLength,
						Key:       key,
						Value:     request.Value,
					})

					replicaBody = bytes.NewReader(replica)

					resp, err := http.Post(n.nodes[i]+"/replication", "application/json", replicaBody)
					if err != nil {
						continue
					}
					if resp.StatusCode == http.StatusOK {
						isReplicated[i] = true
						quorumCount++
					}
				}
			}
		}
	}
	if quorumCount < *nodesCount/2 {
		http.Error(w, "Too long quorum", http.StatusRequestTimeout)
	}
}

func (n *Node) Delete(w http.ResponseWriter, r *http.Request) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	if n.state != Leader {
		http.Error(w, fmt.Sprintf("Only leader can do Delete operation. Leader ID %d, this ID %d", n.leaderID, n.selfID), http.StatusBadRequest)
		return
	}

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

	_, ok = n.data[key]
	if !ok {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}

	delete(n.data, key)

	curLength := len(n.history)
	prevTerm := -1
	if curLength > 0 {
		prevTerm = n.history[curLength-1].Term
	}
	curTerm := n.term
	n.history = append(n.history, Operation{Mode: Create, Term: curTerm, Key: key, Value: 0})

	quorumCount := 1
	isReplicated := make([]bool, *nodesCount)

	retryCount := 0
	var replicaBody *bytes.Reader
	for quorumCount < *nodesCount/2 && retryCount < MaxRetryCount {
		retryCount++
		for i := 0; i < *nodesCount; i++ {
			if i != n.selfID {
				if !isReplicated[i] {
					replica, _ := json.Marshal(ReplicationState{
						NodeID:    n.selfID,
						Mode:      Delete,
						Term:      n.term,
						PrevTerm:  prevTerm,
						LogLength: curLength,
						Key:       key,
						Value:     0,
					})

					replicaBody = bytes.NewReader(replica)

					resp, err := http.Post(n.nodes[i]+"/replication", "application/json", replicaBody)
					if err != nil {
						continue
					}
					if resp.StatusCode == http.StatusOK {
						isReplicated[i] = true
						quorumCount++
					}
				}
			}
		}
	}
	if quorumCount < *nodesCount/2 {
		http.Error(w, "Too long quorum", http.StatusRequestTimeout)
	}
}

func (n *Node) IsLife(w http.ResponseWriter, r *http.Request) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	var nodeState NodeState
	err := json.NewDecoder(r.Body).Decode(&nodeState)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if n.leaderID == nodeState.LeaderID && nodeState.State == Leader {
		n.wasHeartbeat = true
		if len(n.history) < nodeState.LogLength || (len(n.history) == nodeState.LogLength && n.term < nodeState.Term) {
			n.term = nodeState.Term
			n.state = Follower
		}
	} else if len(n.history) < nodeState.LogLength || (len(n.history) == nodeState.LogLength && n.term < nodeState.Term) {
		n.term = nodeState.Term
		n.state = Follower
		n.leaderID = nodeState.LeaderID
	}

	lastTerm := -1
	if len(n.history) > 0 {
		lastTerm = n.history[len(n.history)-1].Term
	}

	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	err = json.NewEncoder(w).Encode(NodeState{NodeID: n.selfID, State: n.state, LeaderID: n.leaderID, Term: lastTerm, LogLength: len(n.history)})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

func (n *Node) Voting(w http.ResponseWriter, r *http.Request) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	if n.state != Follower {
		http.Error(w, "Conflict", http.StatusBadRequest)
		return
	}

	var candidateState NodeState
	err := json.NewDecoder(r.Body).Decode(&candidateState)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if candidateState.State == Candidate && n.term < candidateState.Term && len(n.history) <= candidateState.LogLength {
		n.state = Follower
		n.leaderID = candidateState.NodeID
		n.term = candidateState.Term

		fmt.Printf("New leader ID: %d\n", candidateState.NodeID)
	} else {
		http.Error(w, "Conflict", http.StatusBadRequest)
	}
}

func (n *Node) Replication(w http.ResponseWriter, r *http.Request) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	var replica ReplicationState
	err := json.NewDecoder(r.Body).Decode(&replica)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if replica.NodeID == n.selfID || replica.NodeID != n.leaderID {
		http.Error(w, "Only leader can do replication call", http.StatusBadRequest)
		return
	}

	prevTerm := -1
	if len(n.history) > 0 {
		prevTerm = n.history[len(n.history)-1].Term
	}

	if prevTerm == replica.PrevTerm && replica.LogLength == len(n.history) {
		n.history = append(n.history, Operation{Mode: replica.Mode, Term: n.term, Key: replica.Key, Value: replica.Value})
		if replica.Mode == Create || replica.Mode == Update {
			n.data[replica.Key] = replica.Value
		} else if replica.Mode == Delete {
			delete(n.data, replica.Key)
		}
		n.term = replica.Term
	} else {
		http.Error(w, "Conflict", http.StatusBadRequest)
	}
}

func (n *Node) Recovery(w http.ResponseWriter, r *http.Request) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	var replica OperationState
	err := json.NewDecoder(r.Body).Decode(&replica)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if replica.NodeID == n.selfID || replica.NodeID != n.leaderID {
		http.Error(w, "Only leader can do recovery call", http.StatusBadRequest)
		return
	}

	if len(replica.History) < len(n.history) {
		http.Error(w, "You can't delete longer history", http.StatusBadRequest)
		return
	}

	n.history = replica.History
	for i := 0; i < len(n.history); i++ {
		if n.history[i].Mode == Create || n.history[i].Mode == Update {
			n.data[n.history[i].Key] = n.history[i].Value
		} else if n.history[i].Mode == Delete {
			delete(n.data, n.history[i].Key)
		}
	}
}

func main() {
	pflag.Parse()

	node := NewNode(*nodeID, *nodesCount)

	server := mux.NewRouter()

	// internal
	server.HandleFunc("/is_life", node.IsLife)
	server.HandleFunc("/voting", node.Voting)
	server.HandleFunc("/replication", node.Replication)
	server.HandleFunc("/recovery", node.Recovery)

	go func() {
		time.Sleep(1 * time.Second)
		node.CheckLeader()
	}()

	// external
	server.HandleFunc("/create", node.Create)
	server.HandleFunc("/read/{resourceID}", node.Read)
	server.HandleFunc("/update/{resourceID}", node.Update)
	server.HandleFunc("/delete/{resourceID}", node.Delete)

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", 8080+(*nodeID)), server))
}
