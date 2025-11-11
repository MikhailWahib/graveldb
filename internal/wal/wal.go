// Package wal implements Write-Ahead Logging for durability
package wal

import (
	"bufio"
	"errors"
	"io"
	"os"
	"sync"
	"time"

	"github.com/MikhailWahib/graveldb/internal/storage"
)

// WAL manages the write-ahead log file
type WAL struct {
	mu sync.Mutex

	path string
	file *os.File
	buf  []byte

	flushTimer  *time.Timer
	flushNotify chan struct{}
	closeChan   chan struct{}
	closed      bool

	flushThreshold int
	flushInterval  time.Duration
}

// NewWAL creates a new WAL
func NewWAL(path string, flushThreshold int, flushInterval time.Duration) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	wal := &WAL{
		path:           path,
		file:           file,
		buf:            make([]byte, 0, flushThreshold),
		flushNotify:    make(chan struct{}, 1),
		closeChan:      make(chan struct{}),
		flushThreshold: flushThreshold,
		flushInterval:  flushInterval,
	}
	wal.flushTimer = time.AfterFunc(flushInterval, wal.asyncFlush)
	go wal.backgroundFlusher()
	return wal, nil
}

// writeEntry appends a serialized entry to the WAL buffer and triggers flush if needed
func (w *WAL) writeEntry(e storage.Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return errors.New("WAL is closed")
	}

	data := storage.SerializeEntry(e)
	w.buf = append(w.buf, data...)

	if len(w.buf) >= w.flushThreshold {
		select {
		case w.flushNotify <- struct{}{}:
		default:
		}
	}

	return nil
}

// AppendPut appends a put operation to the WAL
func (w *WAL) AppendPut(key, value []byte) error {
	return w.writeEntry(storage.Entry{
		Type:  storage.PutEntry,
		Key:   key,
		Value: value,
	})
}

// AppendDelete appends a delete operation to the WAL
func (w *WAL) AppendDelete(key []byte) error {
	return w.writeEntry(storage.Entry{
		Type: storage.DeleteEntry,
		Key:  key,
	})
}

// backgroundFlusher handles periodic and threshold-based flushing
func (w *WAL) backgroundFlusher() {
	for {
		select {
		case <-w.flushNotify:
			w.flushBuffer()
		case <-w.flushTimer.C:
			w.flushBuffer()
			w.resetTimer()
		case <-w.closeChan:
			return
		}
	}
}

// resetTimer schedules the next background flush
func (w *WAL) resetTimer() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.closed && w.flushTimer != nil {
		if !w.flushTimer.Stop() {
			select {
			case <-w.flushTimer.C:
			default:
			}
		}
		w.flushTimer.Reset(w.flushInterval)
	}
}

// asyncFlush triggers a flush from the timer context
func (w *WAL) asyncFlush() {
	select {
	case w.flushNotify <- struct{}{}:
	default:
	}
}

// flushBuffer writes buffered data to disk and syncs
func (w *WAL) flushBuffer() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed || len(w.buf) == 0 {
		return nil
	}

	if _, err := w.file.Write(w.buf); err != nil {
		return err
	}
	w.buf = w.buf[:0]

	if err := w.file.Sync(); err == nil {
		return err
	}

	return nil
}

// Replay reads entries from the beginning of the WAL file
func (w *WAL) Replay() ([]storage.Entry, error) {
	readFile, err := os.Open(w.path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = readFile.Close() }()

	var entries []storage.Entry
	reader := bufio.NewReader(readFile)
	for {
		entry, err := storage.ReadEntryFromReader(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// Close flushes all data and closes the WAL
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}

	close(w.closeChan)
	if w.flushTimer != nil {
		w.flushTimer.Stop()
	}

	if len(w.buf) > 0 {
		if _, err := w.file.Write(w.buf); err != nil {
			return err
		}
		_ = w.file.Sync()
	}

	w.closed = true
	return w.file.Close()
}
