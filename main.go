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
	s := server{id: id, node: node}
	return &s
}

func (s *server) handleEcho(body map[string]any, msg maelstrom.Message) (maelstrom.Message, map[string]any) {
	// Update the message type to return back
	body["type"] = "echo_ok"

	return msg, body
}

func (s *server) handleGenerate(body map[string]any, msg maelstrom.Message) (maelstrom.Message, map[string]any) {
	body["type"] = "generate_ok"
	body["id"] = s.id

	return msg, body
}

func main() {
	n := maelstrom.NewNode()
	s := newServer(uuid.New().String(), n)

	n.Handle("echo", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		newBody, newMsg := s.handleEcho(body, msg)
		return n.Reply(newBody, newMsg)
	})

	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		newBody, newMsg := s.handleGenerate(body, msg)
		return n.Reply(newBody, newMsg)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
