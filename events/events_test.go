package events

import (
	"context"
	"testing"
	"time"
)

func TestAsyncPublishDeliversEvent(t *testing.T) {
	bus := NewBus(WithAsync(true), WithWorkerCount(1), WithBufferSize(1))

	delivered := make(chan *Event, 1)
	if _, err := bus.Subscribe("topic", func(ctx context.Context, event *Event) error {
		delivered <- event
		return nil
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	if err := bus.Publish(context.Background(), "topic", "payload"); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case event := <-delivered:
		if event.Topic != "topic" || event.Data != "payload" {
			t.Fatalf("unexpected event: %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for async delivery")
	}
}

func TestPublishAfterCloseReturnsError(t *testing.T) {
	bus := NewBus(WithAsync(true))
	if err := bus.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := bus.Publish(context.Background(), "topic", "payload"); err != ErrBusClosed {
		t.Fatalf("expected ErrBusClosed, got %v", err)
	}
}
