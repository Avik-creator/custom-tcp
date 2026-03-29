package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
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

func (s *Server) acceptLoop() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			log.Printf("Accept Error: %v", err)
			continue
		}

		log.Printf("New Connection from %s", conn.RemoteAddr())
		go s.readLoop(conn)
	}
}

func (s *Server) readLoop(conn net.Conn) {
	defer conn.Close()
	buf := make([]byte, 2048)

	for {
		n, err := conn.Read(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				log.Printf("Client disconnected: %s", conn.RemoteAddr())
			} else {
				log.Printf("Read error from %s: %v", conn.RemoteAddr(), err)
			}
			return // ← critical: exit the goroutine, don't loop
		}

		s.msgch <- Message{
			from:    conn.RemoteAddr().String(),
			payload: buf[:n],
		}

		if _, err := conn.Write([]byte("Message received\n")); err != nil {
			log.Printf("Write error to %s: %v", conn.RemoteAddr(), err)
			return
		}
	}
}
func main() {
	server := NewServer(":3000")

	// Consume messages in a separate goroutine
	go func() {
		for msg := range server.msgch {
			fmt.Printf("Received from %s: %s\n", msg.from, string(msg.payload))
		}
	}()

	// Graceful shutdown on Ctrl+C or SIGTERM
	go func() {
		sigch := make(chan os.Signal, 1)
		signal.Notify(sigch, syscall.SIGINT, syscall.SIGTERM)
		<-sigch
		log.Println("Shutdown signal received")
		close(server.quitch)
	}()

	log.Printf("Server listening on %s", server.listenAddr)
	if err := server.Start(); err != nil {
		log.Fatal(err)
	}
}
