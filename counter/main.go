package counter

import (
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Server struct {
	Node *maelstrom.Node
}

func NewServer(node *maelstrom.Node) *Server {
	return &Server{
		Node: node,
	}
}

func main() {
	n := maelstrom.NewNode()
	s := NewServer(n)

	s.Node.Handle("add", s.AddHandler)
	s.Node.Handle("read", s.ReadHandler)

	if err := s.Node.Run(); err != nil {
		log.Fatal(err)
	}
}

func (s *Server) AddHandler(msg maelstrom.Message) error

func (s *Server) ReadHandler(_ maelstrom.Message) error
