package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Server struct {
	Node               *maelstrom.Node
	Log                map[string][]struct{ Offset, Value int }
	CurOffsets         map[string]int
	CommittedOffsets   map[string]int
	logMu              sync.RWMutex
	curOffsetsMu       sync.RWMutex
	committedOffsetsMu sync.RWMutex
}

func NewServer(n *maelstrom.Node) *Server {
	return &Server{
		Node:             n,
		Log:              make(map[string][]struct{ Offset, Value int }),
		CurOffsets:       make(map[string]int),
		CommittedOffsets: make(map[string]int),
	}
}

func main() {
	n := maelstrom.NewNode()
	s := NewServer(n)

	s.Node.Handle("send", s.HandleSend)
	s.Node.Handle("poll", s.HandlePoll)
	s.Node.Handle("commit_offsets", s.HandleCommitOffsets)
	s.Node.Handle("list_committed_offsets", s.HandleListCommittedOffsets)

	if err := s.Node.Run(); err != nil {
		log.Fatal(err)
	}
}

func (s *Server) HandleSend(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	key := body["key"].(string)
	value_float, ok := body["msg"].(float64)
	if !ok {
		return fmt.Errorf("invalid value: %s", body["msg"])
	}
	value := int(value_float)

	s.curOffsetsMu.Lock()
	offset := s.CurOffsets[key]
	s.CurOffsets[key] += 1
	s.curOffsetsMu.Unlock()

	s.logMu.Lock()
	newElement := struct{ Offset, Value int }{Offset: offset, Value: value}
	if _, exists := s.Log[key]; !exists {
		s.Log[key] = []struct{ Offset, Value int }{}
	}

	s.Log[key] = append(s.Log[key], newElement)
	s.logMu.Unlock()

	res := make(map[string]any)
	res["type"] = "send_ok"
	res["offset"] = offset
	return s.Node.Reply(msg, res)
}

func (s *Server) HandlePoll(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	offsetsRaw, ok := body["offsets"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("offsets field is not a map")
	}

	offsets := make(map[string]float64)
	for k, v := range offsetsRaw {
		offsets[k] = v.(float64)
	}
	log.Printf("Offsets: %+v", offsets)

	log.Printf("Current log: ")
	for key, values := range s.Log {
		log.Printf("Key: %s", key)
		for _, v := range values {
			log.Printf("  {Offset: %d, Value: %d}", v.Offset, v.Value)
		}
	}

	s.logMu.RLock()
	log_data := make(map[string][][]int)
	for key, offset := range offsets {
		int_offset := int(offset)
		log.Printf("int offset to get: %d", int_offset)
		for _, pair := range s.Log[key][int_offset:] {
			if _, exists := log_data[key]; !exists {
				log_data[key] = [][]int{}
			}
			log_data[key] = append(log_data[key], []int{pair.Offset, pair.Value})
		}
	}
	s.logMu.RUnlock()

	resp := make(map[string]any)
	resp["type"] = "poll_ok"
	resp["msgs"] = log_data

	return s.Node.Reply(msg, resp)
}

func (s *Server) HandleCommitOffsets(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	offsetsMap, _ := body["offsets"].(map[string]interface{})

	s.committedOffsetsMu.Lock()
	for key, offsetRaw := range offsetsMap {
		offset, _ := offsetRaw.(float64)
		s.CommittedOffsets[key] = int(offset)
	}
	s.committedOffsetsMu.Unlock()

	return s.Node.Reply(msg, map[string]string{"type": "commit_offsets_ok"})
}

func (s *Server) HandleListCommittedOffsets(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	keys, ok := body["keys"].([]interface{})
	if !ok {
		return fmt.Errorf("keys field is not an array")
	}

	s.committedOffsetsMu.RLock()
	committed_offsets := make(map[string]int)
	for _, keyRaw := range keys {
		key, _ := keyRaw.(string)
		committed_offsets[key] = s.CommittedOffsets[key]
	}
	s.committedOffsetsMu.RUnlock()

	resp := make(map[string]any)
	resp["type"] = "list_committed_offsets_ok"
	resp["offsets"] = committed_offsets
	return s.Node.Reply(msg, resp)
}
