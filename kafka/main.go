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

type LogState struct {
	CurrentOffset   int
	CommittedOffset int
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
	value := int(body["msg"].(float64))

	for {
		var state LogState
		err := s.KV.ReadInto(context.Background(), key, &state)
		if err != nil {
			if rpcErr, ok := err.(*maelstrom.RPCError); ok && rpcErr.Code == maelstrom.KeyDoesNotExist {
				state = LogState{CurrentOffset: 0, CommittedOffset: 0}
			} else {
				return err
			}
		}

		newOffset := state.CurrentOffset + 1
		newState := LogState{
			CurrentOffset:   newOffset,
			CommittedOffset: state.CommittedOffset,
		}

		err = s.KV.CompareAndSwap(context.Background(), key, state, newState, true)
		if err == nil {
			err = s.KV.Write(context.Background(), fmt.Sprintf("Log_%s_%d", key, newOffset), value)
			if err != nil {
				return err
			}

			return s.Node.Reply(msg, map[string]any{
				"type":   "send_ok",
				"offset": newOffset,
			})
		}
		if rpcErr, ok := err.(*maelstrom.RPCError); ok && rpcErr.Code == maelstrom.PreconditionFailed {
			continue
		}

		return err
	}
}

func (s *Server) HandlePoll(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	offsets := body["offsets"].(map[string]interface{})
	msgs := make(map[string][][]int)

	for key, offset := range offsets {
		startOffset := int(offset.(float64))
		var state LogState
		err := s.KV.ReadInto(context.Background(), key, &state)
		if err != nil {
			continue
		}

		for i := startOffset; i <= state.CurrentOffset; i++ {
			value, err := s.KV.ReadInt(context.Background(), fmt.Sprintf("Log_%s_%d", key, i))
			if err == nil {
				msgs[key] = append(msgs[key], []int{i, value})
			}
		}
	}

	return s.Node.Reply(msg, map[string]any{
		"type": "poll_ok",
		"msgs": msgs,
	})
}

func (s *Server) HandleCommitOffsets(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	offsets := body["offsets"].(map[string]interface{})

	for key, offset := range offsets {
		committedOffset := int(offset.(float64))
		for {
			var state LogState
			err := s.KV.ReadInto(context.Background(), key, &state)
			if err != nil {
				break // skip key if it doesnt exist
			}

			newState := LogState{
				CurrentOffset:   state.CurrentOffset,
				CommittedOffset: committedOffset,
			}

			err = s.KV.CompareAndSwap(context.Background(), key, state, newState, true)
			if err == nil {
				break
			}

			if rpcErr, ok := err.(*maelstrom.RPCError); ok && rpcErr.Code == maelstrom.PreconditionFailed {
				continue // someone else updated state, try again
			}

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

	keys := body["keys"].([]interface{})
	offsets := make(map[string]int)

	for _, key := range keys {
		var state LogState
		err := s.KV.ReadInto(context.Background(), key.(string), &state)
		if err == nil {
			offsets[key.(string)] = state.CommittedOffset
		}
	}

	return s.Node.Reply(msg, map[string]any{
		"type":    "list_committed_offsets_ok",
		"offsets": offsets,
	})
}
