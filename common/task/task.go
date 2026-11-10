package task

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"runtime/debug"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

const minTaskInterval = time.Second

// maxTaskPanics bounds how many consecutive panics a task tolerates before it
// stops itself. The node keeps forwarding traffic; the task resumes after a
// reload or restart.
const maxTaskPanics = 3

// stopGrace bounds how long a timed-out execution or Close waits for the
// execution goroutine to return after cancellation. It is a variable so tests
// can shorten it.
var stopGrace = 5 * time.Second

var (
	errTaskTimeout      = errors.New("task execution timed out")
	errTaskTimeoutStuck = errors.New("task execution did not stop after cancellation")
	errTaskPanicLimit   = errors.New("task panicked repeatedly")
)

type Task struct {
	Name       string
	Interval   time.Duration
	Jitter     float64 // fraction of Interval, e.g. 0.1 means ±10%; 0 disables jitter
	Execute    func(context.Context) error
	Access     sync.RWMutex
	Running    bool
	ReloadCh   chan struct{}
	Stop       chan struct{}
	done       chan struct{}
	execCancel context.CancelFunc
	panicCount int
}

func (t *Task) nextInterval() time.Duration {
	interval := t.Interval
	if interval <= 0 {
		return minTaskInterval
	}
	if t.Jitter <= 0 {
		return interval
	}
	delta := float64(interval) * t.Jitter
	next := interval + time.Duration((rand.Float64()*2-1)*delta)
	if next < minTaskInterval {
		return minTaskInterval
	}
	return next
}

func (t *Task) Start(first bool) error {
	t.Access.Lock()
	if t.Running {
		t.Access.Unlock()
		return nil
	}
	t.Running = true
	t.Stop = make(chan struct{})
	t.done = make(chan struct{})
	stop := t.Stop
	done := t.done
	t.Access.Unlock()

	go func() {
		defer close(done)
		defer func() {
			t.Access.Lock()
			t.Running = false
			t.Access.Unlock()
		}()

		run := func() bool {
			err := t.ExecuteWithTimeout()
			if err == nil {
				return true
			}
			if errors.Is(err, errTaskTimeoutStuck) {
				// The execution goroutine did not respond to cancellation.
				// Do not start another execution on top of it.
				log.Errorf("Task %s stopped after timeout", t.Name)
				return false
			}
			if errors.Is(err, errTaskPanicLimit) {
				// A panic is a code bug, not a transient error. Keep the node
				// running but stop repeating the same failure every interval.
				log.Errorf("Task %s stopped after repeated panics", t.Name)
				return false
			}
			if errors.Is(err, errTaskTimeout) {
				// Reload is queued; keep the task alive so panel outages do
				// not permanently stop synchronization.
				log.Errorf("Task %s timed out, will retry", t.Name)
				return true
			}
			log.Errorf("Task %s execution error: %v", t.Name, err)
			// Transient errors should not permanently stop the task.
			return true
		}

		if first && !run() {
			return
		}

		timer := time.NewTimer(t.nextInterval())
		defer timer.Stop()
		for {
			select {
			case <-timer.C:
			case <-stop:
				return
			}
			if !run() {
				return
			}
			select {
			case <-stop:
				return
			default:
			}
			timer.Reset(t.nextInterval())
		}
	}()

	return nil
}

func (t *Task) isStopping() bool {
	t.Access.RLock()
	defer t.Access.RUnlock()
	if t.Stop == nil {
		return false
	}
	select {
	case <-t.Stop:
		return true
	default:
		return false
	}
}

func (t *Task) ExecuteWithTimeout() error {
	ctx, cancel := context.WithTimeout(context.Background(), min(5*t.Interval, 5*time.Minute))
	defer cancel()

	t.Access.Lock()
	t.execCancel = cancel
	t.Access.Unlock()
	defer func() {
		t.Access.Lock()
		t.execCancel = nil
		t.Access.Unlock()
	}()

	done := make(chan error, 1)

	go func() {
		// A panic in Execute must not take the whole process down: it would
		// drop every established connection and, for a deterministic input,
		// turn the next restart into a crash loop.
		defer func() {
			if r := recover(); r != nil {
				t.Access.Lock()
				t.panicCount++
				panics := t.panicCount
				t.Access.Unlock()

				if panics >= maxTaskPanics {
					done <- fmt.Errorf("%w (%d consecutive): %v\n%s", errTaskPanicLimit, panics, r, debug.Stack())
					return
				}
				done <- fmt.Errorf("task %s panicked (%d/%d): %v\n%s", t.Name, panics, maxTaskPanics, r, debug.Stack())
			}
		}()
		err := t.Execute(ctx)
		t.Access.Lock()
		t.panicCount = 0
		t.Access.Unlock()
		done <- err
	}()

	select {
	case err := <-done:
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil
		}
		return err
	case <-ctx.Done():
		if t.isStopping() {
			return nil
		}
		log.Errorf("Task %s execution timed out, reloading", t.Name)
		if t.ReloadCh != nil {
			select {
			case t.ReloadCh <- struct{}{}:
			default:
			}
		} else {
			log.Errorf("Task %s has no reload channel", t.Name)
		}
		// Give the execution goroutine a short chance to observe cancellation.
		select {
		case <-done:
			return errTaskTimeout
		case <-time.After(stopGrace):
			log.Errorf("Task %s execution did not stop after cancellation", t.Name)
			return errTaskTimeoutStuck
		}
	}
}

func (t *Task) safeStop() {
	t.Access.Lock()
	if t.Running {
		t.Running = false
		close(t.Stop)
	}
	cancel := t.execCancel
	done := t.done
	t.Access.Unlock()
	if cancel != nil {
		cancel()
	}
	if done == nil {
		return
	}
	select {
	case <-done:
	case <-time.After(stopGrace):
		log.Warningf("Task %s did not stop within %s", t.Name, stopGrace)
	}
}

func (t *Task) Close() {
	t.safeStop()
	log.Warningf("Task %s stopped", t.Name)
}
