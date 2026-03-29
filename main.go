package main

import (
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
