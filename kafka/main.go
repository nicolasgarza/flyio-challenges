package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Server struct {
	Node *maelstrom.Node
	KV   *maelstrom.KV
}

func NewServer(n *maelstrom.Node, kv *maelstrom.KV) *Server {
	return &Server{
		Node: n,
		KV:   kv,
	}
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)
	s := NewServer(n, kv)

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

	offset, err := s.GetAndIncrementOffset(key)
	if err != nil {
		return err
	}

	// store the log entry
	logEntryKey := fmt.Sprintf("Log_%s_%d", key, offset)
	if err := s.KV.Write(context.Background(), logEntryKey, value); err != nil {
		return err
	}

	// update list of offsets
	offsetsKey := fmt.Sprintf("LogOffsets_%s", key)
	for {
		var offsets []int
		err := s.KV.ReadInto(context.Background(), offsetsKey, &offsets)
		if err != nil {
			if rpcErr, ok := err.(*maelstrom.RPCError); ok && rpcErr.Code == maelstrom.KeyDoesNotExist {
				err = s.KV.Write(context.Background(), offsetsKey, []int{offset})
				if err == nil {
					break
				}
			}
			return err
		}

		newOffsets := append(offsets, offset)
		err = s.KV.CompareAndSwap(context.Background(), offsetsKey, offsets, newOffsets, true)
		if err == nil {
			break
		}
		if rpcErr, ok := err.(*maelstrom.RPCError); ok && rpcErr.Code == maelstrom.PreconditionFailed {
			continue
		}
		return err
	}

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

	offsets := make(map[string]int)
	for k, v := range offsetsRaw {
		offsets[k] = int(v.(float64))
	}

	log_data := make(map[string][][]int)
	for key, offset := range offsets {
		offsetsKey := fmt.Sprintf("LogOffsets_%s", key)
		var allOffsets []int
		if err := s.KV.ReadInto(context.Background(), offsetsKey, &allOffsets); err != nil {
			return err
		}

		for _, currOffset := range allOffsets {
			if currOffset >= offset {
				logEntryKey := fmt.Sprintf("Log_%s_%d", key, currOffset)
				value, err := s.KV.ReadInt(context.Background(), logEntryKey)
				if err != nil {
					return err
				}
				if _, exists := log_data[key]; !exists {
					log_data[key] = [][]int{}
				}
				log_data[key] = append(log_data[key], []int{currOffset, value})
			}
		}
	}

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

	for key, offsetRaw := range offsetsMap {
		offset := int(offsetRaw.(float64))
		fullKey := "CommittedOffsets_" + key

		err := s.KV.Write(context.Background(), fullKey, offset)
		if err != nil {
			return err
		}
	}

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

	committed_offsets := make(map[string]int)
	for _, keyRaw := range keys {
		key := keyRaw.(string)
		fetch_key := "CommittedOffsets_" + key
		committed_offset, err := s.KV.ReadInt(context.Background(), fetch_key)
		if err != nil {
			if rpcErr, ok := err.(*maelstrom.RPCError); ok && rpcErr.Code == maelstrom.KeyDoesNotExist {
				continue
			}
			return err
		}
		committed_offsets[key] = committed_offset
	}

	resp := make(map[string]any)
	resp["type"] = "list_committed_offsets_ok"
	resp["offsets"] = committed_offsets
	return s.Node.Reply(msg, resp)
}

func (s *Server) GetAndIncrementOffset(key string) (int, error) {
	fullKey := "CurOffsets_" + key
	for {
		currentValue, err := s.KV.ReadInt(context.Background(), fullKey)
		if err != nil {
			if rpcErr, ok := err.(*maelstrom.RPCError); ok && rpcErr.Code == maelstrom.KeyDoesNotExist {
				err = s.KV.Write(context.Background(), fullKey, 1)
				if err == nil {
					return 1, nil
				}
				return 0, err
			}
			return 0, err
		}

		newValue := currentValue + 1

		err = s.KV.CompareAndSwap(context.Background(), fullKey, currentValue, newValue, false)
		if err == nil {
			return newValue, nil
		}

		if rpcErr, ok := err.(*maelstrom.RPCError); ok && rpcErr.Code == maelstrom.PreconditionFailed {
			continue
		}

		return 0, err
	}
}
