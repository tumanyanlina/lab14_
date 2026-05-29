package main

import (
	"math/rand"
	"time"
)

// ServiceType represents a type of service provided at MFC.
type ServiceType string

const (
	ServicePassport      ServiceType = "passport"
	ServiceRegistration  ServiceType = "registration"
	ServiceSocialBenefit ServiceType = "social_benefit"
	ServiceTax           ServiceType = "tax"
	ServiceProperty      ServiceType = "property"
)

var allServices = []ServiceType{
	ServicePassport,
	ServiceRegistration,
	ServiceSocialBenefit,
	ServiceTax,
	ServiceProperty,
}

// QueueRecord is a single raw observation from one MFC window sensor.
type QueueRecord struct {
	WindowID    int         `json:"window_id"`
	ServiceType ServiceType `json:"service_type"`
	QueueLength int         `json:"queue_length"`
	WaitTimeSec int         `json:"wait_time_sec"`
	Timestamp   time.Time   `json:"timestamp"`
}

// WindowState holds the current simulated state for one MFC window.
type WindowState struct {
	WindowID    int
	ServiceType ServiceType
	queueLen    int
}

// Emulator simulates MFC queue sensors for multiple windows.
type Emulator struct {
	windows  []*WindowState
	interval time.Duration
	rng      *rand.Rand
}

// NewEmulator creates an Emulator for numWindows windows with the given tick interval.
func NewEmulator(numWindows int, interval time.Duration) *Emulator {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	windows := make([]*WindowState, numWindows)
	for i := range windows {
		windows[i] = &WindowState{
			WindowID:    i + 1,
			ServiceType: allServices[i%len(allServices)],
			queueLen:    rng.Intn(5),
		}
	}
	return &Emulator{windows: windows, interval: interval, rng: rng}
}

// Run starts emitting QueueRecords into ch until done is closed.
func (e *Emulator) Run(ch chan<- QueueRecord, done <-chan struct{}) {
	ticker := time.NewTicker(e.interval)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case t := <-ticker.C:
			for _, w := range e.windows {
				e.updateState(w)
				ch <- QueueRecord{
					WindowID:    w.WindowID,
					ServiceType: w.ServiceType,
					QueueLength: w.queueLen,
					WaitTimeSec: e.estimateWait(w.queueLen),
					Timestamp:   t,
				}
			}
		}
	}
}

// updateState applies a random walk to the queue length.
func (e *Emulator) updateState(w *WindowState) {
	delta := e.rng.Intn(5) - 2 // -2..+2
	w.queueLen += delta
	if w.queueLen < 0 {
		w.queueLen = 0
	}
	if w.queueLen > 30 {
		w.queueLen = 30
	}
}

// estimateWait returns wait time estimate based on queue length (avg 4 min per person).
func (e *Emulator) estimateWait(queueLen int) int {
	baseSeconds := queueLen * 240
	jitter := e.rng.Intn(60) - 30
	result := baseSeconds + jitter
	if result < 0 {
		result = 0
	}
	return result
}
