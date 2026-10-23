package spy

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"chitchat/pkg/broker"
	"github.com/tidwall/gjson"
)

const (
	// defaultWaitTimeout is used by wait_for when no timeout parameter is given.
	defaultWaitTimeout = 5 * time.Second
	// maxWaitTimeout caps how long a single wait_for request may block.
	maxWaitTimeout = 60 * time.Second
)

// CapturedMessage represents an intercepted or published message stored in the ring buffer.
type CapturedMessage struct {
	// Seq is a monotonically increasing sequence number (never reset, not even by clear).
	// Pass it as ?since=<seq> to fetch only newer messages.
	Seq       uint64    `json:"seq"`
	Timestamp time.Time `json:"timestamp"`
	// Direction is "received" (consumed by chitchat) or "published" (mock response sent by chitchat).
	Direction string `json:"direction"`
	// Queue is the queue a received message came from; empty for published messages.
	Queue string `json:"queue"`
	// Exchange is the destination exchange of a published message.
	Exchange   string                 `json:"exchange,omitempty"`
	RoutingKey string                 `json:"routing_key"`
	Rule       string                 `json:"rule,omitempty"`
	MessageID  string                 `json:"message_id,omitempty"`
	Body       interface{}            `json:"body"` // Decoded JSON payload or string
	Headers    map[string]interface{} `json:"headers"`

	raw []byte // original body bytes, used for JSONPath body filters
}

// Filter selects captured messages. Zero values mean "don't filter on this field".
type Filter struct {
	Queue      string
	Exchange   string
	RoutingKey string
	Direction  string
	Rule       string
	// Headers requires each named header to exist and, if the value is non-nil, to equal it.
	Headers map[string]*string
	// Body requires each gjson path to exist in the JSON body and, if the value is non-nil, to equal it.
	Body []BodyFilter
	// SinceSeq returns only messages with Seq greater than this value.
	SinceSeq uint64
	// Limit keeps only the most recent N matching messages (0 = no limit).
	Limit int
}

// BodyFilter is a gjson path with an optional expected value.
type BodyFilter struct {
	Path  string
	Value *string
}

func (f Filter) matches(m *CapturedMessage) bool {
	if f.Queue != "" && m.Queue != f.Queue {
		return false
	}
	if f.Exchange != "" && m.Exchange != f.Exchange {
		return false
	}
	if f.RoutingKey != "" && m.RoutingKey != f.RoutingKey {
		return false
	}
	if f.Direction != "" && m.Direction != f.Direction {
		return false
	}
	if f.Rule != "" && m.Rule != f.Rule {
		return false
	}
	if f.SinceSeq > 0 && m.Seq <= f.SinceSeq {
		return false
	}
	for name, want := range f.Headers {
		val, ok := m.Headers[name]
		if !ok {
			return false
		}
		if want != nil && fmt.Sprint(val) != *want {
			return false
		}
	}
	for _, bf := range f.Body {
		if len(m.raw) == 0 {
			return false
		}
		res := gjson.GetBytes(m.raw, bf.Path)
		if !res.Exists() {
			return false
		}
		if bf.Value != nil && res.String() != *bf.Value {
			return false
		}
	}
	return true
}

// RingBuffer is a thread-safe, bounded sliding-window buffer of captured messages.
type RingBuffer struct {
	mu       sync.RWMutex
	messages []CapturedMessage
	maxSize  int
	nextSeq  uint64
	changed  chan struct{} // closed and replaced whenever the buffer changes
}

// NewRingBuffer initializes a new RingBuffer with the given maximum capacity.
func NewRingBuffer(maxSize int) *RingBuffer {
	if maxSize <= 0 {
		maxSize = 1000
	}
	return &RingBuffer{
		messages: make([]CapturedMessage, 0, maxSize),
		maxSize:  maxSize,
		nextSeq:  1,
		changed:  make(chan struct{}),
	}
}

// notifyLocked wakes everyone waiting on Changed. Callers must hold the write lock.
func (rb *RingBuffer) notifyLocked() {
	close(rb.changed)
	rb.changed = make(chan struct{})
}

// Changed returns a channel that is closed the next time the buffer is modified.
func (rb *RingBuffer) Changed() <-chan struct{} {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return rb.changed
}

// Record appends a broker message (received or published) to the ring buffer.
func (rb *RingBuffer) Record(msg broker.Message) {
	var decodedBody interface{}
	if len(msg.Body) > 0 {
		if err := json.Unmarshal(msg.Body, &decodedBody); err != nil {
			decodedBody = string(msg.Body)
		}
	} else {
		decodedBody = ""
	}

	direction := msg.Direction
	if direction == "" {
		direction = broker.DirectionReceived
	}

	rb.mu.Lock()
	defer rb.mu.Unlock()

	captured := CapturedMessage{
		Seq:        rb.nextSeq,
		Timestamp:  time.Now().UTC(),
		Direction:  direction,
		Queue:      msg.Queue,
		Exchange:   msg.Exchange,
		RoutingKey: msg.RoutingKey,
		Rule:       msg.Rule,
		MessageID:  msg.MessageID,
		Body:       decodedBody,
		Headers:    normalizeHeaders(msg.Headers),
		raw:        msg.Body,
	}
	rb.nextSeq++

	rb.messages = append(rb.messages, captured)
	if len(rb.messages) > rb.maxSize {
		rb.messages = rb.messages[len(rb.messages)-rb.maxSize:]
	}
	rb.notifyLocked()
}

// normalizeHeaders copies headers and converts values that don't render well as JSON
// (notably []byte, which would be base64 encoded) into readable forms.
func normalizeHeaders(h map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(h))
	for k, v := range h {
		out[k] = normalizeHeaderValue(v)
	}
	return out
}

func normalizeHeaderValue(v interface{}) interface{} {
	switch t := v.(type) {
	case nil:
		return nil
	case []byte:
		return string(t)
	case []interface{}:
		out := make([]interface{}, len(t))
		for i, e := range t {
			out[i] = normalizeHeaderValue(e)
		}
		return out
	}
	// Handle map types such as amqp.Table (a named map[string]interface{}).
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Map && rv.Type().Key().Kind() == reflect.String {
		out := make(map[string]interface{}, rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			out[iter.Key().String()] = normalizeHeaderValue(iter.Value().Interface())
		}
		return out
	}
	return v
}

// Query returns a copy of the captured messages that satisfy the filter, oldest first.
func (rb *RingBuffer) Query(f Filter) []CapturedMessage {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	result := make([]CapturedMessage, 0, len(rb.messages))
	for i := range rb.messages {
		if f.matches(&rb.messages[i]) {
			result = append(result, rb.messages[i])
		}
	}
	if f.Limit > 0 && len(result) > f.Limit {
		result = result[len(result)-f.Limit:]
	}
	return result
}

// GetMessages returns a filtered copy of captured messages.
func (rb *RingBuffer) GetMessages(queueFilter, routingKeyFilter string) []CapturedMessage {
	return rb.Query(Filter{Queue: queueFilter, RoutingKey: routingKeyFilter})
}

// Clear flushes all captured messages from the ring buffer and returns how many were removed.
// Sequence numbers keep increasing so a ?since=<seq> cursor stays valid across clears.
func (rb *RingBuffer) Clear() int {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	n := len(rb.messages)
	rb.messages = make([]CapturedMessage, 0, rb.maxSize)
	rb.notifyLocked()
	return n
}

// Count returns the number of captured messages currently stored.
func (rb *RingBuffer) Count() int {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return len(rb.messages)
}

// RulesProvider returns a JSON-serialisable snapshot of the loaded rules.
type RulesProvider func() interface{}

// Reloader re-reads the configuration and applies it. It returns a JSON-serialisable summary.
type Reloader func() (interface{}, error)

// Server exposes the REST spy API endpoint over HTTP.
type Server struct {
	buffer     *RingBuffer
	host       string
	port       int
	httpServer *http.Server
	logger     *slog.Logger

	hooksMu  sync.RWMutex
	rules    RulesProvider
	reloader Reloader
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
	mux.HandleFunc("/_chitchat/rules", s.handleRules)
	mux.HandleFunc("/_chitchat/reload", s.handleReload)

	s.httpServer = &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	return s
}

// WithHost sets the interface to bind to (default: all interfaces). Call before Start.
func (s *Server) WithHost(host string) *Server {
	s.host = host
	return s
}

// SetRulesProvider registers the source for GET /_chitchat/rules.
func (s *Server) SetRulesProvider(p RulesProvider) {
	s.hooksMu.Lock()
	defer s.hooksMu.Unlock()
	s.rules = p
}

// SetReloader registers the handler for POST /_chitchat/reload.
func (s *Server) SetReloader(r Reloader) {
	s.hooksMu.Lock()
	defer s.hooksMu.Unlock()
	s.reloader = r
}

// Start launches the HTTP spy server in the background.
func (s *Server) Start() error {
	addr := net.JoinHostPort(s.host, strconv.Itoa(s.port))
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to bind spy server to %s: %w", addr, err)
	}

	s.port = ln.Addr().(*net.TCPAddr).Port
	s.logger.Info("starting spy rest server", "host", s.host, "port", s.port)

	go func() {
		if err := s.httpServer.Serve(ln); err != nil && err != http.ErrServerClosed {
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

func (s *Server) Port() int {
	return s.port
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// splitKV splits "name=value" on the first '='; a bare "name" yields a nil value.
func splitKV(s string) (string, *string) {
	if i := strings.Index(s, "="); i >= 0 {
		v := s[i+1:]
		return s[:i], &v
	}
	return s, nil
}

// parseFilter builds a Filter from query parameters:
// queue, exchange, routing_key, direction, rule, since, limit, header=name[=value], body=path[=value].
func parseFilter(q url.Values) (Filter, error) {
	f := Filter{
		Queue:      q.Get("queue"),
		Exchange:   q.Get("exchange"),
		RoutingKey: q.Get("routing_key"),
		Direction:  q.Get("direction"),
		Rule:       q.Get("rule"),
	}
	if f.Direction != "" && f.Direction != broker.DirectionReceived && f.Direction != broker.DirectionPublished {
		return f, fmt.Errorf("invalid direction %q (expected %q or %q)", f.Direction, broker.DirectionReceived, broker.DirectionPublished)
	}
	if v := q.Get("since"); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return f, fmt.Errorf("invalid since %q: must be a non-negative integer", v)
		}
		f.SinceSeq = n
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return f, fmt.Errorf("invalid limit %q: must be a non-negative integer", v)
		}
		f.Limit = n
	}
	for _, h := range q["header"] {
		name, val := splitKV(h)
		if name == "" {
			return f, fmt.Errorf("invalid header filter %q", h)
		}
		if f.Headers == nil {
			f.Headers = make(map[string]*string)
		}
		f.Headers[name] = val
	}
	for _, b := range q["body"] {
		path, val := splitKV(b)
		if path == "" {
			return f, fmt.Errorf("invalid body filter %q", b)
		}
		f.Body = append(f.Body, BodyFilter{Path: path, Value: val})
	}
	return f, nil
}

func parseTimeout(v string) (time.Duration, error) {
	if v == "" {
		return defaultWaitTimeout, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		secs, serr := strconv.ParseFloat(v, 64)
		if serr != nil {
			return 0, fmt.Errorf("invalid timeout %q (use e.g. 5s, 500ms or a number of seconds)", v)
		}
		d = time.Duration(secs * float64(time.Second))
	}
	if d < 0 {
		return 0, fmt.Errorf("invalid timeout %q: must not be negative", v)
	}
	if d > maxWaitTimeout {
		d = maxWaitTimeout
	}
	return d, nil
}

// waitForMessages blocks until at least n messages match f, the timeout expires or ctx is cancelled.
func (s *Server) waitForMessages(ctx context.Context, f Filter, n int, timeout time.Duration) ([]CapturedMessage, bool) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		// Grab the change channel before querying so no update can slip between the two.
		changed := s.buffer.Changed()
		msgs := s.buffer.Query(f)
		if len(msgs) >= n {
			return msgs, true
		}
		select {
		case <-changed:
		case <-timer.C:
			return s.buffer.Query(f), false
		case <-ctx.Done():
			return msgs, false
		}
	}
}

func (s *Server) handleMessages(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
	case http.MethodDelete:
		s.handleClear2(w)
		return
	default:
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	query := r.URL.Query()
	filter, err := parseFilter(query)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	var (
		messages []CapturedMessage
		status   = http.StatusOK
	)
	if wf := query.Get("wait_for"); wf != "" {
		n, err := strconv.Atoi(wf)
		if err != nil || n < 0 {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid wait_for %q: must be a non-negative integer", wf))
			return
		}
		timeout, err := parseTimeout(query.Get("timeout"))
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		// Wait on the unlimited match set; limit is applied to the result afterwards.
		waitFilter := filter
		waitFilter.Limit = 0
		var ok bool
		messages, ok = s.waitForMessages(r.Context(), waitFilter, n, timeout)
		if !ok {
			if r.Context().Err() != nil {
				return // client went away
			}
			status = http.StatusRequestTimeout
		}
		if filter.Limit > 0 && len(messages) > filter.Limit {
			messages = messages[len(messages)-filter.Limit:]
		}
	} else {
		messages = s.buffer.Query(filter)
	}

	w.Header().Set("X-Chitchat-Count", strconv.Itoa(len(messages)))
	writeJSON(w, status, messages)
}

func (s *Server) handleClear(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	s.handleClear2(w)
}

func (s *Server) handleClear2(w http.ResponseWriter) {
	n := s.buffer.Clear()
	s.logger.Info("cleared spy message ring buffer", "cleared", n)
	writeJSON(w, http.StatusOK, map[string]interface{}{"status": "cleared", "cleared": n})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":   "ok",
		"messages": s.buffer.Count(),
	})
}

func (s *Server) handleRules(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	s.hooksMu.RLock()
	provider := s.rules
	s.hooksMu.RUnlock()

	if provider == nil {
		writeJSON(w, http.StatusOK, []interface{}{})
		return
	}
	writeJSON(w, http.StatusOK, provider())
}

func (s *Server) handleReload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	s.hooksMu.RLock()
	reload := s.reloader
	s.hooksMu.RUnlock()

	if reload == nil {
		writeError(w, http.StatusNotImplemented, "hot reload is not available")
		return
	}
	summary, err := reload()
	if err != nil {
		s.logger.Error("config reload failed", "error", err)
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"status": "reloaded", "result": summary})
}
