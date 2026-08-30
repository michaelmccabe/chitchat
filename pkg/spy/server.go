package spy

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"chitchat/pkg/broker"
)

// CapturedMessage represents an intercepted message stored in the ring buffer.
type CapturedMessage struct {
	Timestamp  time.Time              `json:"timestamp"`
	Queue      string                 `json:"queue"`
	RoutingKey string                 `json:"routing_key"`
	Body       interface{}            `json:"body"` // Decoded JSON payload or string
	Headers    map[string]interface{} `json:"headers"`
}

// RingBuffer is a thread-safe, bounded sliding-window buffer of captured messages.
type RingBuffer struct {
	mu       sync.RWMutex
	messages []CapturedMessage
	maxSize  int
}

// NewRingBuffer initializes a new RingBuffer with the given maximum capacity.
func NewRingBuffer(maxSize int) *RingBuffer {
	if maxSize <= 0 {
		maxSize = 1000
	}
	return &RingBuffer{
		messages: make([]CapturedMessage, 0, maxSize),
		maxSize:  maxSize,
	}
}

// Record intercepts and appends a broker message to the ring buffer.
func (rb *RingBuffer) Record(msg broker.Message) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	var decodedBody interface{}
	if len(msg.Body) > 0 {
		if err := json.Unmarshal(msg.Body, &decodedBody); err != nil {
			decodedBody = string(msg.Body)
		}
	} else {
		decodedBody = ""
	}

	captured := CapturedMessage{
		Timestamp:  time.Now().UTC(),
		Queue:      msg.Queue,
		RoutingKey: msg.RoutingKey,
		Body:       decodedBody,
		Headers:    msg.Headers,
	}

	rb.messages = append(rb.messages, captured)
	if len(rb.messages) > rb.maxSize {
		rb.messages = rb.messages[len(rb.messages)-rb.maxSize:]
	}
}

// GetMessages returns a filtered copy of captured messages.
func (rb *RingBuffer) GetMessages(queueFilter, routingKeyFilter string) []CapturedMessage {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	result := make([]CapturedMessage, 0, len(rb.messages))
	for _, m := range rb.messages {
		if queueFilter != "" && m.Queue != queueFilter {
			continue
		}
		if routingKeyFilter != "" && m.RoutingKey != routingKeyFilter {
			continue
		}
		result = append(result, m)
	}
	return result
}

// Clear flushes all captured messages from the ring buffer.
func (rb *RingBuffer) Clear() {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.messages = make([]CapturedMessage, 0, rb.maxSize)
}

// Count returns the number of captured messages currently stored.
func (rb *RingBuffer) Count() int {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return len(rb.messages)
}

// Server exposes the REST spy API endpoints over HTTP.
type Server struct {
	buffer     *RingBuffer
	port       int
	httpServer *http.Server
	logger     *slog.Logger
}

// NewServer creates a new Spy REST API Server.
func NewServer(port int, buffer *RingBuffer) *Server {
	if port <= 0 {
		port = 8082
	}
	if buffer == nil {
		buffer = NewRingBuffer(1000)
	}

	s := &Server{
		buffer: buffer,
		port:   port,
		logger: slog.Default(),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/_chitchat/messages", s.handleMessages)
	mux.HandleFunc("/_chitchat/clear", s.handleClear)
	mux.HandleFunc("/_chitchat/health", s.handleHealth)

	s.httpServer = &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	return s
}

// Start launches the HTTP spy server in the background.
func (s *Server) Start() error {
	s.logger.Info("starting spy rest server", "port", s.port)

	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Error("spy server listener error", "error", err)
		}
	}()

	return nil
}

// Stop gracefully shuts down the HTTP spy server.
func (s *Server) Stop(ctx context.Context) error {
	s.logger.Info("stopping spy rest server", "port", s.port)
	return s.httpServer.Shutdown(ctx)
}

// Buffer returns the underlying ring buffer.
func (s *Server) Buffer() *RingBuffer {
	return s.buffer
}

func (s *Server) handleMessages(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	queueFilter := r.URL.Query().Get("queue")
	routingKeyFilter := r.URL.Query().Get("routing_key")

	messages := s.buffer.GetMessages(queueFilter, routingKeyFilter)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(messages); err != nil {
		s.logger.Error("failed to encode messages response", "error", err)
	}
}

func (s *Server) handleClear(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	s.buffer.Clear()
	s.logger.Info("cleared spy message ring buffer")

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "cleared"})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":   "ok",
		"messages": s.buffer.Count(),
	})
}
