package task

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestExecuteWithTimeoutSignalsReload(t *testing.T) {
	old := stopGrace
	stopGrace = 10 * time.Millisecond
	defer func() { stopGrace = old }()

	reload := make(chan struct{}, 1)
	task := &Task{
		Name:     "timeout",
		Interval: 10 * time.Millisecond,
		ReloadCh: reload,
		Execute: func(ctx context.Context) error {
			time.Sleep(200 * time.Millisecond)
			return nil
		},
	}

	err := task.ExecuteWithTimeout()
	if !errors.Is(err, errTaskTimeoutStuck) {
		t.Fatalf("ExecuteWithTimeout() = %v, want errTaskTimeoutStuck", err)
	}
	select {
	case <-reload:
	default:
		t.Fatal("expected reload signal")
	}
}

func TestExecuteWithTimeoutResponsive(t *testing.T) {
	old := stopGrace
	stopGrace = 200 * time.Millisecond
	defer func() { stopGrace = old }()

	task := &Task{
		Name:     "responsive-timeout",
		Interval: 10 * time.Millisecond,
		Execute: func(ctx context.Context) error {
			<-ctx.Done()
			time.Sleep(10 * time.Millisecond)
			return nil
		},
	}

	if err := task.ExecuteWithTimeout(); !errors.Is(err, errTaskTimeout) {
		t.Fatalf("ExecuteWithTimeout() = %v, want errTaskTimeout", err)
	}
}

func TestExecuteWithTimeoutReturnsError(t *testing.T) {
	want := errors.New("boom")
	task := &Task{
		Interval: time.Second,
		Execute: func(ctx context.Context) error {
			return want
		},
	}
	if err := task.ExecuteWithTimeout(); !errors.Is(err, want) {
		t.Fatalf("ExecuteWithTimeout() = %v, want %v", err, want)
	}
}
