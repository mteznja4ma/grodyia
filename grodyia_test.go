package grodyia

import (
	"errors"
	"testing"
)

type flakyTransport struct {
	fail bool
}

func (t *flakyTransport) ID() string                  { return "flaky" }
func (t *flakyTransport) Name() string                { return "flaky" }
func (t *flakyTransport) Version() string             { return "1.0.0" }
func (t *flakyTransport) Metadata() map[string]string { return nil }
func (t *flakyTransport) Addr() string                { return "127.0.0.1:0" }
func (t *flakyTransport) Stop() error                 { return nil }

func (t *flakyTransport) Start() error {
	if t.fail {
		return errors.New("start failed")
	}
	return nil
}

func TestStartFailureDoesNotLeaveAppRunning(t *testing.T) {
	transport := &flakyTransport{fail: true}
	app := New()
	app.Bind(transport)

	if err := app.Start(); err == nil {
		t.Fatal("expected start error")
	}
	if app.IsRunning() {
		t.Fatal("app should not be running after failed start")
	}

	transport.fail = false
	if err := app.Start(); err != nil {
		t.Fatalf("second start should succeed: %v", err)
	}
	if !app.IsRunning() {
		t.Fatal("app should be running after successful retry")
	}
	if err := app.Stop(); err != nil {
		t.Fatalf("stop app: %v", err)
	}
}
