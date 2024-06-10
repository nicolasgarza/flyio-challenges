package main

import (
	"encoding/json"
	"log"

	"github.com/google/uuid"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	id   string
	node *maelstrom.Node
}

func newServer(id string, node *maelstrom.Node) *server {
	return &server{id: id, node: node}
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

func main() {
	s := newServer(uuid.New().String(), maelstrom.NewNode())

	s.node.Handle("echo", s.handleEcho)
	s.node.Handle("generate", s.handleGenerate)

	if err := s.node.Run(); err != nil {
		log.Fatal(err)
	}
}
