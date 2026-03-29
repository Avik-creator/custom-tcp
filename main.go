package main

import (
	"log"
	"net"
)

type Message struct {
	from    string
	payload []byte
}

type Server struct {
	listenAddr string
	ln         net.Listener
	quitch     chan struct{}
	msgch      chan Message
}

func NewServer(listenAdd string) *Server {
	return &Server{
		listenAddr: listenAdd,
		quitch:     make(chan struct{}),
		msgch:      make(chan Message, 100),
	}
}

func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.listenAddr)
	if err != nil {
		return err
	}
	s.ln = ln

	go s.acceptLoop()

	<-s.quitch

	ln.Close()
	close(s.msgch)

	log.Println("Server Shutdown GraceFully")
	return nil
}
