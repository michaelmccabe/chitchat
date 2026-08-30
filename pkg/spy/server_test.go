package spy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"chitchat/pkg/broker"
)

func TestRingBuffer_BoundedSlidingWindow(t *testing.T) {
	rb := NewRingBuffer(3)

	for i := 1; i <= 5; i++ {
		rb.Record(broker.Message{
			Queue:      "q1",
			RoutingKey: "rk1",
			Body:       []byte(`{"index": ` + string(rune('0'+i)) + `}`),
		})
	}

	if rb.Count() != 3 {
		t.Fatalf("expected 3 items in bounded buffer, got %d", rb.Count())
	}

	msgs := rb.GetMessages("", "")
	if len(msgs) != 3 {
		t.Fatalf("expected 3 items, got %d", len(msgs))
	}

	// Oldest items (1, 2) should have been evicted; remaining items: 3, 4, 5
	lastMsg := msgs[2].Body.(map[string]interface{})
	if lastMsg["index"].(float64) != 5 {
		t.Errorf("expected last item index to be 5, got %v", lastMsg["index"])
	}
}

func TestRingBuffer_Filtering(t *testing.T) {
	rb := NewRingBuffer(10)

	rb.Record(broker.Message{Queue: "orders", RoutingKey: "order.created", Body: []byte(`{"id": 1}`)})
	rb.Record(broker.Message{Queue: "orders", RoutingKey: "order.cancelled", Body: []byte(`{"id": 2}`)})
	rb.Record(broker.Message{Queue: "payments", RoutingKey: "payment.processed", Body: []byte(`{"id": 3}`)})

	// Filter by queue
	orderMsgs := rb.GetMessages("orders", "")
	if len(orderMsgs) != 2 {
		t.Errorf("expected 2 messages for queue 'orders', got %d", len(orderMsgs))
	}

	// Filter by routing key
	createdMsgs := rb.GetMessages("", "order.created")
	if len(createdMsgs) != 1 {
		t.Errorf("expected 1 message for routing_key 'order.created', got %d", len(createdMsgs))
	}

	// Filter by both
	paymentMsgs := rb.GetMessages("payments", "payment.processed")
	if len(paymentMsgs) != 1 {
		t.Errorf("expected 1 message for payments & payment.processed, got %d", len(paymentMsgs))
	}

	// Clear
	rb.Clear()
	if rb.Count() != 0 {
		t.Errorf("expected 0 messages after Clear, got %d", rb.Count())
	}
}

func TestHTTPSpyEndpoints(t *testing.T) {
	rb := NewRingBuffer(10)
	rb.Record(broker.Message{
		Queue:      "order.created.trigger",
		RoutingKey: "order.event.created",
		Body:       []byte(`{"order": {"id": "ord_100"}}`),
		Headers:    map[string]interface{}{"x-correlation-id": "12345"},
	})

	server := NewServer(8082, rb)

	// 1. Test GET /_chitchat/messages
	req := httptest.NewRequest(http.MethodGet, "/_chitchat/messages", nil)
	rec := httptest.NewRecorder()
	server.handleMessages(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var captured []CapturedMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &captured); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(captured) != 1 {
		t.Fatalf("expected 1 captured message, got %d", len(captured))
	}
	if captured[0].Queue != "order.created.trigger" {
		t.Errorf("unexpected queue: %s", captured[0].Queue)
	}

	// 2. Test GET with query filter
	reqFilter := httptest.NewRequest(http.MethodGet, "/_chitchat/messages?queue=non_existent", nil)
	recFilter := httptest.NewRecorder()
	server.handleMessages(recFilter, reqFilter)

	var filtered []CapturedMessage
	_ = json.Unmarshal(recFilter.Body.Bytes(), &filtered)
	if len(filtered) != 0 {
		t.Errorf("expected 0 messages for non_existent filter, got %d", len(filtered))
	}

	// 3. Test POST /_chitchat/clear
	reqClear := httptest.NewRequest(http.MethodPost, "/_chitchat/clear", nil)
	recClear := httptest.NewRecorder()
	server.handleClear(recClear, reqClear)

	if recClear.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", recClear.Code)
	}
	if rb.Count() != 0 {
		t.Errorf("expected ring buffer to be cleared, got %d", rb.Count())
	}

	// 4. Test Method Not Allowed
	reqBad := httptest.NewRequest(http.MethodPost, "/_chitchat/messages", nil)
	recBad := httptest.NewRecorder()
	server.handleMessages(recBad, reqBad)
	if recBad.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status 405, got %d", recBad.Code)
	}
}
