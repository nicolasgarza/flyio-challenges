package main

import (
	"encoding/json"
	"log"

	"github.com/google/uuid"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	id       string
	node     *maelstrom.Node
	store    map[int]struct{}
	received []int
}

func newServer(id string, node *maelstrom.Node) *server {
	return &server{id: id, node: node, store: make(map[int]struct{}), received: []int{}}
}

func (s *server) handleEcho(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	// Update the message type to return back
	body["type"] = "echo_ok"

	return s.node.Reply(msg, body)
}

func (s *server) handleGenerate(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	body["type"] = "generate_ok"
	body["id"] = uuid.New().String()

	return s.node.Reply(msg, body)
}

func (s *server) handleBroadcast(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	body["type"] = "broadcast_ok"
	recieved_int := int(body["message"].(float64))

	if _, exists := s.store[recieved_int]; exists {
		// early return if we have already received int, don't broadcast
		delete(body, "message")
		return s.node.Reply(msg, body)
	}

	s.store[recieved_int] = struct{}{}
	s.received = append(s.received, recieved_int)
	s.broadcastMsg(recieved_int)
	delete(body, "message")

	return s.node.Reply(msg, body)
}

func (s *server) handleRead(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	body["type"] = "read_ok"
	body["messages"] = s.received

	return s.node.Reply(msg, body)
}

func (s *server) handleTopology(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	body["type"] = "topology_ok"
	delete(body, "topology")
	return s.node.Reply(msg, body)
}

func (s *server) broadcastMsg(msg int) {
	send_msg := map[string]any{
		"type":    "broadcast",
		"message": msg,
	}
	for _, n := range s.node.NodeIDs() {
		s.node.RPC(n, send_msg, func(msg maelstrom.Message) error { return nil })
	}
}

func main() {
	s := newServer(uuid.New().String(), maelstrom.NewNode())

	s.node.Handle("echo", s.handleEcho)
	s.node.Handle("generate", s.handleGenerate)
	s.node.Handle("broadcast", s.handleBroadcast)
	s.node.Handle("read", s.handleRead)
	s.node.Handle("topology", s.handleTopology)

	if err := s.node.Run(); err != nil {
		log.Fatal(err)
	}
}
