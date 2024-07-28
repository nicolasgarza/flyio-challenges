package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"path/filepath"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

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
	// logFile := setupLogger()
	// defer logFile.Close()

	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)
	s := NewServer(n, kv)

	s.Node.Handle("init", func(msg maelstrom.Message) error {
		UserLog("Initializing counter to 0")
		_, err := s.Store.ReadInt(context.Background(), "counter")
		if err != nil {
			if rpcErr, ok := err.(*maelstrom.RPCError); ok && rpcErr.Code == maelstrom.KeyDoesNotExist {
				UserLog("Counter does not exist, initializing to 0")
				err = s.Store.Write(context.Background(), "counter", 0)
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

	delta_float, _ := body["delta"].(float64)
	delta := int(delta_float)
	for {
		ctr, err := s.Store.ReadInt(context.Background(), "counter")
		if err != nil {
			return err
		}

		newCtr := ctr + delta
		err = s.Store.CompareAndSwap(context.Background(), "counter", ctr, newCtr, false)
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
	currCount, err := s.Store.ReadInt(context.Background(), "counter")
	if err != nil {
		return err
	}

	response := make(map[string]any)
	response["type"] = "read_ok"
	response["value"] = currCount

	return s.Node.Reply(msg, response)
}

func getValAndReplace(s *Server, delta int) (error, bool) {
	UserLog("doing compare and swap")
	ctr, err := s.Store.ReadInt(context.Background(), "counter")
	if err != nil {
		return err, false
	}

	newCtr := ctr + delta
	err = s.Store.CompareAndSwap(context.Background(), "counter", ctr, newCtr, true)
	return err, true
}

func setupLogger() *os.File {
	logDir := filepath.Join(os.Getenv("HOME"), "code", "maelstrom-echo", "counter")

	err := os.MkdirAll(logDir, os.ModePerm)
	if err != nil {
		log.Fatal("Error creating logs directory:", err)
	}

	logFilePath := filepath.Join(logDir, "output.log")
	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o666)
	if err != nil {
		log.Fatal("Error opening log file:", err)
	}

	log.SetOutput(logFile)

	return logFile
}

func UserLog(msg string) {
	log.Println("-----------------------")
	log.Println(msg)
	log.Println("-----------------------")
}
