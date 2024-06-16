package main

import (
	"context"
	"encoding/json"
	"log"
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
)

const (
	DefaultRetries = 3
	DefaultTimeout = 1 * time.Second
)

type server struct {
	node     *maelstrom.Node
	topology []string
	store    map[int]struct{}
	received []int
	mu       sync.RWMutex
}

func newServer(node *maelstrom.Node) *server {
	return &server{node: node, topology: []string{}, store: make(map[int]struct{}), received: []int{}}
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

	body["type"] = BroadcastOkType
	receivedInt := int(body["message"].(float64))

	if s.isMessageReceived(receivedInt) {
		// early return if we have already received int, don't broadcast
		return s.replyBroadcastOk(msg, body)
	}

	s.storeMessage(receivedInt)
	go s.broadcastMsg(receivedInt, DefaultRetries, DefaultTimeout)
	return s.replyBroadcastOk(msg, body)
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

func (s *server) broadcastMsg(msg int, retries int, timeout time.Duration) {
	send_msg := map[string]any{
		"type":    BroadcastType,
		"message": msg,
	}

	var wg sync.WaitGroup
	for _, n := range s.topology {
		wg.Add(1)
		go func(n string) {
			defer wg.Done()
			for i := 0; i < retries; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				_, err := s.node.SyncRPC(ctx, n, send_msg)
				cancel()

				if err == nil {
					break
				}

				if i == retries-1 {
					log.Printf("failed to send message to %s after %d retries: %v", n, retries, err)
				}

				log.Printf("Retrying to send message to %s (%d/%d): %v", n, i+1, retries, err)
			}
		}(n)
	}
	wg.Wait()
}

func (s *server) storeMessage(msg int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.store[msg] = struct{}{}
	s.received = append(s.received, msg)
}

func (s *server) isMessageReceived(msg int) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, exists := s.store[msg]
	return exists
}

func (s *server) replyBroadcastOk(msg maelstrom.Message, body map[string]any) error {
	delete(body, "message")
	return s.node.Reply(msg, body)
}

func main() {
	s := newServer(maelstrom.NewNode())

	s.node.Handle(EchoType, s.handleEcho)
	s.node.Handle(GenerateType, s.handleGenerate)
	s.node.Handle(BroadcastType, s.handleBroadcast)
	s.node.Handle(ReadType, s.handleRead)
	s.node.Handle(TopologyType, s.handleTopology)

	if err := s.node.Run(); err != nil {
		log.Fatal(err)
	}
}
