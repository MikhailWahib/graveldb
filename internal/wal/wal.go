// Package wal implements Write-Ahead Logging for durability
package wal

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"os"
	"sync"
	"time"

	"github.com/MikhailWahib/graveldb/internal/shared"
)

const (
	flushThreshold = 64 * 1024 // 64KB
	flushInterval  = 10 * time.Millisecond
)

// WAL manages the write-ahead log file
type WAL struct {
	mu   sync.Mutex
	once sync.Once

	path        string
	file        *os.File
	writeOffset int64

	buffer      bytes.Buffer
	flushTimer  *time.Timer
	flushNotify chan struct{}
	closeChan   chan struct{}
	closed      bool
}

// NewWAL creates a new WAL
func NewWAL(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	wal := &WAL{
		path:        path,
		file:        file,
		writeOffset: fileInfo.Size(),
		flushNotify: make(chan struct{}, 1),
		closeChan:   make(chan struct{}),
	}
	wal.flushTimer = time.AfterFunc(flushInterval, wal.asyncFlush)
	go wal.backgroundFlusher()
	return wal, nil
}

// writeEntry writes an entry to the buffer and schedules a flush
func (w *WAL) writeEntry(e shared.Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return errors.New("WAL is closed")
	}

	// Serialize entry to buffer
	data := shared.SerializeEntry(e)
	if _, err := w.buffer.Write(data); err != nil {
		return err
	}

	// Trigger flush if buffer exceeds threshold
	if w.buffer.Len() >= flushThreshold {
		w.signalFlush()
	}
	return nil
}

// AppendPut appends a put operation to the WAL
func (w *WAL) AppendPut(key, value []byte) error {
	return w.writeEntry(shared.Entry{
		Type:  shared.PutEntry,
		Key:   key,
		Value: value,
	})
}

// AppendDelete appends a delete operation to the WAL
func (w *WAL) AppendDelete(key []byte) error {
	return w.writeEntry(shared.Entry{
		Type:  shared.DeleteEntry,
		Key:   key,
		Value: []byte{},
	})
}

// signalFlush triggers an immediate flush
func (w *WAL) signalFlush() {
	select {
	case w.flushNotify <- struct{}{}:
	default: // Already pending
	}
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
			<-w.flushTimer.C // drain if needed
		}
		w.flushTimer.Reset(flushInterval)
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
func (w *WAL) flushBuffer() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.buffer.Len() == 0 || w.closed {
		return
	}

	// Write buffered data to disk
	n, err := w.file.WriteAt(w.buffer.Bytes(), w.writeOffset)
	if err != nil {
		return
	}

	// Update write offset
	w.writeOffset += int64(n)

	// Sync to ensure durability
	if err := w.file.Sync(); err != nil {
		return
	}

	// Reset buffer for new writes
	w.buffer.Reset()
}

// Replay reads entries from the beginning, returning 0 offset at EOF
func (w *WAL) Replay() ([]shared.Entry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	_, err := w.file.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}
	reader := bufio.NewReader(w.file)

	var walEntries []shared.Entry
	for {
		entry, err := shared.ReadEntryFromReader(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		walEntries = append(walEntries, entry)
	}
	return walEntries, nil
}

// Close closes the WAL file
func (w *WAL) Close() error {
	if w.closed {
		return nil
	}

	var err error
	w.once.Do(func() {
		close(w.closeChan)
		w.flushTimer.Stop()
		w.flushBuffer()
		w.closed = true
		err = w.file.Close()
	})

	return err
}
