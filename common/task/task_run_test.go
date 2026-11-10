package task

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
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

func TestExecuteWithTimeoutRecoversPanic(t *testing.T) {
	task := &Task{
		Name:     "panic-once",
		Interval: time.Second,
		Execute: func(context.Context) error {
			panic("boom")
		},
	}

	err := task.ExecuteWithTimeout()
	if err == nil || errors.Is(err, errTaskPanicLimit) {
		t.Fatalf("ExecuteWithTimeout() = %v, want recovered panic", err)
	}
	if !strings.Contains(err.Error(), "panicked") {
		t.Fatalf("ExecuteWithTimeout() = %v, want panic context", err)
	}
	if !strings.Contains(err.Error(), "goroutine") {
		t.Fatalf("ExecuteWithTimeout() = %v, want stack trace", err)
	}
}

func TestExecuteWithTimeoutStopsAfterRepeatedPanics(t *testing.T) {
	task := &Task{
		Name:     "panic-always",
		Interval: time.Second,
		Execute: func(context.Context) error {
			panic("boom")
		},
	}

	for i := 1; i < maxTaskPanics; i++ {
		err := task.ExecuteWithTimeout()
		if err == nil || errors.Is(err, errTaskPanicLimit) {
			t.Fatalf("execution %d error = %v, want recovered panic", i, err)
		}
	}
	if err := task.ExecuteWithTimeout(); !errors.Is(err, errTaskPanicLimit) {
		t.Fatalf("ExecuteWithTimeout() = %v, want errTaskPanicLimit", err)
	}
}

func TestPanicCountResetsAfterSuccessfulRun(t *testing.T) {
	var calls atomic.Int32
	task := &Task{
		Name:     "panic-then-ok",
		Interval: time.Second,
		Execute: func(context.Context) error {
			if calls.Add(1) == 1 {
				panic("boom")
			}
			return nil
		},
	}

	if err := task.ExecuteWithTimeout(); err == nil {
		t.Fatal("expected a recovered panic")
	}
	if err := task.ExecuteWithTimeout(); err != nil {
		t.Fatalf("second execution = %v, want nil", err)
	}
	if task.panicCount != 0 {
		t.Fatalf("panicCount = %d, want 0", task.panicCount)
	}
}

func TestStartContinuesAfterRecoveredPanic(t *testing.T) {
	var calls atomic.Int32
	second := make(chan struct{})
	task := &Task{
		Name:     "panic-once-start",
		Interval: 10 * time.Millisecond,
		Execute: func(context.Context) error {
			if calls.Add(1) == 1 {
				panic("boom")
			}
			select {
			case <-second:
			default:
				close(second)
			}
			return nil
		},
	}

	if err := task.Start(false); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer task.Close()

	select {
	case <-second:
	case <-time.After(2 * time.Second):
		t.Fatal("task did not continue after a recovered panic")
	}
}
