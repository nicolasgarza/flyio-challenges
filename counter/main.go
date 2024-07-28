package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const counterKey = "counter"

type Server struct {
	Node        *maelstrom.Node
	Store       *maelstrom.KV
	StoredDelta int
}

func NewServer(node *maelstrom.Node, kv_store *maelstrom.KV) *Server {
	return &Server{
		Node:  node,
		Store: kv_store,
	}
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)
	s := NewServer(n, kv)

	s.Node.Handle("init", func(msg maelstrom.Message) error {
		UserLog("Initializing counter to 0")
		_, err := s.Store.ReadInt(context.Background(), counterKey)
		if err != nil {
			if rpcErr, ok := err.(*maelstrom.RPCError); ok && rpcErr.Code == maelstrom.KeyDoesNotExist {
				UserLog("Counter does not exist, initializing to 0")
				err = s.Store.Write(context.Background(), counterKey, 0)
				if err != nil {
					UserLog("error initializing counter: " + err.Error())
				}
			} else {
				UserLog("Error reading counter: " + err.Error())
				return err
			}
		} else {
			UserLog("Counter already initialized")
		}
		return nil
	})

	s.Node.Handle("add", s.AddHandler)
	s.Node.Handle("read", s.ReadHandler)

	UserLog("Running node")
	// Run the node
	if err := s.Node.Run(); err != nil {
		log.Fatal(err)
	}
}

func (s *Server) AddHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	delta_float, ok := body["delta"].(float64)
	if !ok {
		return fmt.Errorf("invalid delta value")
	}
	delta := int(delta_float)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for {
		ctr, err := s.Store.ReadInt(ctx, counterKey)
		if err != nil {
			return err
		}

		newCtr := ctr + delta
		err = s.Store.CompareAndSwap(ctx, counterKey, ctr, newCtr, false)
		if err == nil {
			break
		}
	}

	return s.Node.Reply(msg, map[string]string{"type": "add_ok"})
}

func (s *Server) ReadHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	UserLog("Reading from store...")
	currCount, err := s.Store.ReadInt(context.Background(), counterKey)
	if err != nil {
		return err
	}

	response := make(map[string]any)
	response["type"] = "read_ok"
	response["value"] = currCount

	return s.Node.Reply(msg, response)
}

func UserLog(msg string) {
	log.Println("-----------------------")
	log.Println(msg)
	log.Println("-----------------------")
}
