package broadcast

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/google/uuid"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	EchoType        = "echo"
	EchoOkType      = "echo_ok"
	GenerateType    = "generate"
	GenerateOkType  = "generate_ok"
	BroadcastType   = "broadcast"
	BroadcastOkType = "broadcast_ok"
	ReadType        = "read"
	ReadOkType      = "read_ok"
	TopologyType    = "topology"
	TopologyOkType  = "topology_ok"
	GossipType      = "gossip"
)

const (
	DefaultTimeout = 2 * time.Second
	BatchSize      = 750
	GossipInterval = 3 * time.Second
)

type server struct {
	node        *maelstrom.Node
	topology    []string
	store       map[int]struct{}
	received    []int
	mu          sync.RWMutex
	newMessages chan int
	lastSent    int
	gossipTimer *time.Timer
}

func newServer(node *maelstrom.Node) *server {
	return &server{
		node:        node,
		topology:    []string{},
		store:       make(map[int]struct{}),
		received:    []int{},
		newMessages: make(chan int, 100),
		lastSent:    0,
		gossipTimer: time.NewTimer(GossipInterval),
	}
}

func (s *server) handleEcho(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	// Update the message type to return back
	body["type"] = EchoOkType

	return s.node.Reply(msg, body)
}

func (s *server) handleGenerate(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	body["type"] = GenerateOkType
	body["id"] = uuid.New().String()

	return s.node.Reply(msg, body)
}

func (s *server) handleBroadcast(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	if messages, ok := body["messages"].([]interface{}); ok {
		lastSent := int(body["last_sent"].(float64))
		for i, m := range messages {
			if intMsg, ok := m.(float64); ok {
				s.storeMessage(int(intMsg), lastSent+i)
			}
		}
	} else if singleMsg, ok := body["message"].(float64); ok {
		s.storeMessage(int(singleMsg), -1)
	}

	body["type"] = BroadcastOkType
	delete(body, "message")
	delete(body, "messages")
	delete(body, "last_sent")
	return s.node.Reply(msg, body)
}

func (s *server) handleRead(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	body["type"] = ReadOkType
	body["messages"] = append([]int(nil), s.received...)

	return s.node.Reply(msg, body)
}

func (s *server) handleTopology(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	topologyData := body["topology"].(map[string]interface{})
	nodeList := topologyData[s.node.ID()].([]interface{})

	s.topology = make([]string, len(nodeList))
	for i, node := range nodeList {
		s.topology[i] = node.(string)
	}

	body["type"] = TopologyOkType
	delete(body, TopologyType)
	return s.node.Reply(msg, body)
}

func (s *server) startGossip() {
	go func() {
		batchTicker := time.NewTicker(100 * time.Millisecond)
		defer batchTicker.Stop()
		batch := make([]int, 0, BatchSize)
		for {
			select {
			case <-s.gossipTimer.C:
				s.gossipToRandomNodes(nil)
				batch = batch[:0] // Clear the batch
			case msg := <-s.newMessages:
				batch = append(batch, msg)
				if len(batch) >= BatchSize {
					s.gossipToRandomNodes(batch)
					batch = batch[:0] // Clear the batch
				}
			case <-batchTicker.C:
				if len(batch) > 0 {
					s.gossipToRandomNodes(batch)
					batch = batch[:0] // Clear the batch
				}
			}
		}
	}()
}

func (s *server) gossipToRandomNodes(newMsg []int) {
	s.mu.RLock()
	messages := append([]int(nil), s.received[max(0, len(s.received)-BatchSize):]...)
	lastSent := s.lastSent
	s.mu.RUnlock()

	if len(messages) == 0 && newMsg == nil {
		return
	}

	if newMsg != nil {
		messages = append(messages, newMsg...)
	}

	gossipMsg := map[string]any{
		"type":      BroadcastType,
		"messages":  messages,
		"last_sent": lastSent,
	}

	for _, nodeID := range s.getRandomNodes(2) {
		go func(nodeID string) {
			ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
			defer cancel()
			_, err := s.node.SyncRPC(ctx, nodeID, gossipMsg)
			if err != nil {
				log.Printf("Error gossiping to node %s: %v", nodeID, err)
			}
		}(nodeID)
	}

	s.mu.Lock()
	s.lastSent = len(s.received)
	s.mu.Unlock()
}

func (s *server) getRandomNodes(num int) []string {
	allNodes := s.node.NodeIDs()
	currentNodeID := s.node.ID()

	// Remove the current node from the list
	var otherNodes []string
	for _, nodeID := range allNodes {
		if nodeID != currentNodeID {
			otherNodes = append(otherNodes, nodeID)
		}
	}

	// Shuffle the slice of other nodes
	rand.Shuffle(len(otherNodes), func(i, j int) {
		otherNodes[i], otherNodes[j] = otherNodes[j], otherNodes[i]
	})

	// Return up to 'num' nodes, but no more than available
	if len(otherNodes) < num {
		return otherNodes
	}
	return otherNodes[:num]
}

func (s *server) storeMessage(msg int, index int) {
	s.mu.Lock()
	if _, exists := s.store[msg]; !exists {
		s.store[msg] = struct{}{}
		s.mu.Unlock()

		s.mu.Lock()
		if index >= 0 && index < len(s.received) {
			s.received = append(s.received[:index], append([]int{msg}, s.received[index:]...)...)
		} else {
			s.received = append(s.received, msg)
		}
		s.mu.Unlock()

		select {
		case s.newMessages <- msg:
		default:
			// channel full, msg will be propagated in next gossip round
		}
	} else {
		s.mu.Unlock()
	}
}

func main() {
	s := newServer(maelstrom.NewNode())

	s.node.Handle(EchoType, s.handleEcho)
	s.node.Handle(GenerateType, s.handleGenerate)
	s.node.Handle(BroadcastType, s.handleBroadcast)
	s.node.Handle(ReadType, s.handleRead)
	s.node.Handle(TopologyType, s.handleTopology)
	s.startGossip()

	if err := s.node.Run(); err != nil {
		log.Fatal(err)
	}
	log.Println("Program done")
}
