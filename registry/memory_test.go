package registry

import (
	"testing"
	"time"
)

func TestMemoryWatcherReceivesCreateAndDelete(t *testing.T) {
	reg := NewRegistry(TypeMemory)
	if err := reg.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer reg.Close()

	watcher, err := reg.Watch()
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	defer watcher.Stop()

	service := &Service{
		ID:      "svc-1",
		Name:    "svc",
		Address: "127.0.0.1:9000",
	}
	if err := reg.Register(service); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := reg.Deregister(service); err != nil {
		t.Fatalf("deregister: %v", err)
	}

	expectEvent := func(expectedType EventType) {
		t.Helper()
		done := make(chan *Event, 1)
		go func() {
			event, _ := watcher.Next()
			done <- event
		}()

		select {
		case event := <-done:
			if event == nil || event.Type != expectedType {
				t.Fatalf("expected %s event, got %+v", expectedType, event)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for %s event", expectedType)
		}
	}

	expectEvent(Create)
	expectEvent(Delete)
}
