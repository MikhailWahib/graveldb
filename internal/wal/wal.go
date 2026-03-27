// Package wal implements Write-Ahead Logging for durability
package wal

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
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

	flushTicker *time.Ticker
	closeChan   chan struct{}
	closed      bool
	err         error

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
		closeChan:      make(chan struct{}),
		flushThreshold: flushThreshold,
		flushInterval:  flushInterval,
	}
	wal.flushTicker = time.NewTicker(flushInterval)
	go wal.backgroundFlusher()
	return wal, nil
}

// writeEntry appends a serialized entry to the WAL buffer and triggers flush if needed
func (w *WAL) writeEntry(e storage.Entry) error {
	w.mu.Lock()
	if w.closed {
		err := w.err
		w.mu.Unlock()
		if err != nil {
			return fmt.Errorf("WAL is closed: %w", err)
		}
		return fmt.Errorf("WAL is closed")
	}

	data := storage.SerializeEntry(e)
	w.buf = append(w.buf, data...)

	if len(w.buf) >= w.flushThreshold {
		err := w.flushBuffer()
		w.mu.Unlock()
		if err != nil {
			w.fail(err)
			return err
		}
		return nil
	}

	w.mu.Unlock()
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
		case <-w.flushTicker.C:
			w.mu.Lock()
			if err := w.flushBuffer(); err != nil {
				w.failLocked(err)
				w.mu.Unlock()
				return
			}
			w.flushTicker.Reset(w.flushInterval)
			w.mu.Unlock()
		case <-w.closeChan:
			return
		}
	}
}

// flushBuffer writes buffered data to disk and syncs
func (w *WAL) flushBuffer() error {
	if w.closed || len(w.buf) == 0 {
		return nil
	}

	if _, err := w.file.Write(w.buf); err != nil {
		return err
	}

	if err := w.file.Sync(); err != nil {
		return err
	}

	w.buf = w.buf[:0]

	return nil
}

// Seal flushes the active WAL, renames it to archivePath, and starts a fresh
// active WAL file at the original path.
func (w *WAL) Seal(archivePath string) (string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		if w.err != nil {
			return "", fmt.Errorf("WAL is closed: %w", w.err)
		}
		return "", fmt.Errorf("WAL is closed")
	}

	if err := w.flushBuffer(); err != nil {
		return "", err
	}

	if err := w.file.Close(); err != nil {
		return "", err
	}

	if err := os.Rename(w.path, archivePath); err != nil {
		file, openErr := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if openErr != nil {
			return "", fmt.Errorf("failed to rotate WAL: rename error: %v, reopen error: %w", err, openErr)
		}
		w.file = file
		return "", err
	}

	file, err := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC|os.O_APPEND, 0644)
	if err != nil {
		return "", err
	}
	w.file = file
	return archivePath, nil
}

// Replay reads entries from the active WAL file.
func (w *WAL) Replay() ([]storage.Entry, error) {
	return replayFile(w.path)
}

// ReplayDir reads entries from all WAL files in a directory.
func ReplayDir(dir string) ([]storage.Entry, error) {
	patterns := []string{
		filepath.Join(dir, "wal-*.log"),
		filepath.Join(dir, "wal.log"),
	}

	pathSet := make(map[string]struct{})
	for _, pattern := range patterns {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			return nil, err
		}
		for _, match := range matches {
			pathSet[match] = struct{}{}
		}
	}

	paths := make([]string, 0, len(pathSet))
	for path := range pathSet {
		paths = append(paths, path)
	}
	sort.Strings(paths)

	var entries []storage.Entry
	for _, path := range paths {
		segmentEntries, err := replayFile(path)
		if err != nil {
			return nil, err
		}
		entries = append(entries, segmentEntries...)
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
	if w.flushTicker != nil {
		w.flushTicker.Stop()
	}

	if err := w.flushBuffer(); err != nil {
		w.closed = true
		_ = w.file.Close()
		return err
	}

	w.closed = true
	_ = w.file.Close()
	return nil
}

func (w *WAL) fail(err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.failLocked(err)
}

func (w *WAL) failLocked(err error) {
	if w.closed {
		return
	}
	w.err = err
	w.closed = true
	if w.flushTicker != nil {
		w.flushTicker.Stop()
	}
	select {
	case <-w.closeChan:
	default:
		close(w.closeChan)
	}
	_ = w.file.Close()
}

func replayFile(path string) ([]storage.Entry, error) {
	readFile, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = readFile.Close() }()

	var entries []storage.Entry
	reader := bufio.NewReader(readFile)
	for {
		entry, err := storage.ReadEntryFromReader(reader)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}
