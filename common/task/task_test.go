package task

import (
	"testing"
	"time"
)

func TestNextIntervalNoJitter(t *testing.T) {
	task := &Task{Interval: 10 * time.Second}
	if got := task.nextInterval(); got != 10*time.Second {
		t.Fatalf("nextInterval() = %v, want 10s", got)
	}
}

func TestNextIntervalWithJitter(t *testing.T) {
	base := 10 * time.Second
	task := &Task{Interval: base, Jitter: 0.1}
	for i := 0; i < 1000; i++ {
		got := task.nextInterval()
		if got < 9*time.Second || got > 11*time.Second {
			t.Fatalf("nextInterval() = %v, want in [9s, 11s]", got)
		}
	}
}

func TestNextIntervalMinimum(t *testing.T) {
	task := &Task{Interval: 2 * time.Second, Jitter: 1}
	for i := 0; i < 1000; i++ {
		got := task.nextInterval()
		if got < minTaskInterval {
			t.Fatalf("nextInterval() = %v, want >= %v", got, minTaskInterval)
		}
	}
}

func TestNextIntervalZero(t *testing.T) {
	task := &Task{}
	if got := task.nextInterval(); got != minTaskInterval {
		t.Fatalf("nextInterval() = %v, want %v", got, minTaskInterval)
	}
}
