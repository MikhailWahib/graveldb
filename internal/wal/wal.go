// Package wal implements Write-Ahead Logging for durability
package wal

import (
	"bufio"
	"errors"
	"io"
	"os"
	"sync"
	"time"

	"github.com/MikhailWahib/graveldb/internal/config"
	"github.com/MikhailWahib/graveldb/internal/record"
)

// WAL manages the write-ahead log file
type WAL struct {
	mu   sync.Mutex
	once sync.Once

	path   string
	file   *os.File
	writer *bufio.Writer

	flushTimer  *time.Timer
	flushNotify chan struct{}
	closeChan   chan struct{}
	closed      bool

	config *config.Config
}

// NewWAL creates a new WAL
func NewWAL(path string, config *config.Config) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	wal := &WAL{
		path:        path,
		file:        file,
		writer:      bufio.NewWriterSize(file, config.WALFlushThreshold),
		flushNotify: make(chan struct{}, 1),
		closeChan:   make(chan struct{}),
		config:      config,
	}
	wal.flushTimer = time.AfterFunc(config.WALFlushInterval, wal.asyncFlush)
	go wal.backgroundFlusher()
	return wal, nil
}

// writeEntry serializes and writes an entry to the buffer
func (w *WAL) writeEntry(e record.Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return errors.New("WAL is closed")
	}

	data := record.SerializeEntry(e)
	if _, err := w.writer.Write(data); err != nil {
		return err
	}

	if w.writer.Buffered() >= w.config.WALFlushThreshold {
		w.signalFlush()
	}
	return nil
}

// AppendPut appends a put operation to the WAL
func (w *WAL) AppendPut(key, value []byte) error {
	return w.writeEntry(record.Entry{
		Type:  record.PutEntry,
		Key:   key,
		Value: value,
	})
}

// AppendDelete appends a delete operation to the WAL
func (w *WAL) AppendDelete(key []byte) error {
	return w.writeEntry(record.Entry{
		Type:  record.DeleteEntry,
		Key:   key,
		Value: []byte{},
	})
}

// signalFlush triggers an immediate flush
func (w *WAL) signalFlush() {
	select {
	case w.flushNotify <- struct{}{}:
	default:
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
			select {
			case <-w.flushTimer.C:
			default:
			}
		}
		w.flushTimer.Reset(w.config.WALFlushInterval)
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

	if w.closed || w.writer.Buffered() == 0 {
		return
	}

	if err := w.writer.Flush(); err != nil {
		return
	}

	if err := w.file.Sync(); err != nil {
		return
	}
}

// Replay reads entries from the beginning of the WAL file
func (w *WAL) Replay() ([]record.Entry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	readFile, err := os.Open(w.path)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = readFile.Close()
	}()

	reader := bufio.NewReader(readFile)
	var entries []record.Entry
	for {
		entry, err := record.ReadEntryFromReader(reader)
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
