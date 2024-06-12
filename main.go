package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
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

type server struct {
	node     *maelstrom.Node
	store    map[int]struct{}
	received []int
}

func newServer(node *maelstrom.Node) *server {
	return &server{node: node, store: make(map[int]struct{}), received: []int{}}
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
	recieved_int := int(body["message"].(float64))

	if s.isMessageReceived(recieved_int) {
		// early return if we have already received int, don't broadcast
		return s.replyBroadcastOk(msg, body)
	}

	s.storeMessage(recieved_int)
	go func(recieved_int int) {
		if err := s.broadcastMsg(recieved_int, 3, 2*time.Second); err != nil {
			log.Printf("Failed to broadcast message after retries: %v", err)
		}
	}(recieved_int)
	return s.replyBroadcastOk(msg, body)
}

func (s *server) handleRead(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	body["type"] = ReadOkType
	body["messages"] = s.received

	return s.node.Reply(msg, body)
}

func (s *server) handleTopology(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	body["type"] = TopologyOkType
	delete(body, TopologyType)
	return s.node.Reply(msg, body)
}

func (s *server) broadcastMsg(msg int, retries int, timeout time.Duration) error {
	send_msg := map[string]any{
		"type":    BroadcastType,
		"message": msg,
	}
	for _, n := range s.node.NodeIDs() {
		if n == s.node.ID() {
			continue
		}
		for i := 0; i < retries; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			_, err := s.node.SyncRPC(ctx, n, send_msg)
			if err == nil {
				break
			}

			if i == retries-1 {
				return fmt.Errorf("failed to send message to %s after %d retries: %v", n, retries, err)
			}

			log.Printf("Retrying to send message to %s (%d/%d): %v", n, i+1, retries, err)
		}
	}

	return nil
}

func (s *server) storeMessage(msg int) {
	s.store[msg] = struct{}{}
	s.received = append(s.received, msg)
}

func (s *server) isMessageReceived(msg int) bool {
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
