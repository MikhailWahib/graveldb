package sstable

import (
	"fmt"
	"os"
	"sync"
)

// SSTable provides a unified interface for SSTable operations,
// internally delegating to SSTReader and SSTWriter
type SSTable struct {
	mu sync.RWMutex

	path      string
	reader    *sstReader
	writer    *sstWriter
	isReading bool
	isWriting bool
}

// NewSSTable creates a new SSTable instance
func NewSSTable(path string) *SSTable {
	return &SSTable{
		path:      path,
		isReading: false,
		isWriting: false,
	}
}

// OpenForRead opens an existing SSTable file in read mode
func (sst *SSTable) OpenForRead() error {
	sst.mu.Lock()
	defer sst.mu.Unlock()

	if sst.isReading || sst.isWriting {
		return fmt.Errorf("SSTable is already open for reading or writing")
	}

	reader := newSSTReader()
	err := reader.Open(sst.path)
	if err != nil {
		return err
	}

	sst.reader = reader
	sst.isReading = true
	return nil
}

// OpenForWrite creates a new SSTable file in write mode
func (sst *SSTable) OpenForWrite() error {
	sst.mu.Lock()
	defer sst.mu.Unlock()

	if sst.isReading || sst.isWriting {
		return fmt.Errorf("SSTable is already open for reading or writing")
	}

	writer := newSSTWriter()
	err := writer.Open(sst.path)
	if err != nil {
		return err
	}

	sst.writer = writer
	sst.isWriting = true
	return nil
}

// Lookup performs a lookup in read mode
func (sst *SSTable) Lookup(key []byte) ([]byte, error) {
	sst.mu.RLock()
	defer sst.mu.RUnlock()

	if !sst.isReading {
		return nil, fmt.Errorf("SSTable is not open for reading")
	}
	return sst.reader.Lookup(key)
}

// AppendPut adds a key-value pair in write mode
func (sst *SSTable) AppendPut(key, value []byte) error {
	sst.mu.Lock()
	defer sst.mu.Unlock()

	if !sst.isWriting {
		return fmt.Errorf("SSTable is not open for writing")
	}
	return sst.writer.AppendPut(key, value)
}

// AppendDelete adds a deletion marker in write mode
func (sst *SSTable) AppendDelete(key []byte) error {
	sst.mu.Lock()
	defer sst.mu.Unlock()

	if !sst.isWriting {
		return fmt.Errorf("SSTable is not open for writing")
	}
	return sst.writer.AppendDelete(key)
}

// NewIterator creates a new iterator for the SSTable in read mode
func (sst *SSTable) NewIterator() (*Iterator, error) {
	sst.mu.RLock()
	defer sst.mu.RUnlock()

	if !sst.isReading {
		return nil, fmt.Errorf("SSTable is not open for reading")
	}
	return sst.reader.newIterator(), nil
}

// Finish finalizes the SSTable in write mode
func (sst *SSTable) Finish() error {
	sst.mu.Lock()
	defer sst.mu.Unlock()

	if !sst.isWriting {
		return fmt.Errorf("SSTable is not open for writing")
	}

	err := sst.writer.Finish()
	if err != nil {
		return err
	}

	sst.isWriting = false
	sst.writer = nil
	return nil
}

// Close closes the SSTable
func (sst *SSTable) Close() error {
	sst.mu.Lock()
	defer sst.mu.Unlock()

	var err error
	if sst.isReading {
		err = sst.reader.Close()
		sst.isReading = false
		sst.reader = nil
	}
	if sst.isWriting {
		// If we're closing without finishing, we'll need to clean up
		// but this might leave an incomplete file
		err = sst.writer.file.Close()
		sst.isWriting = false
		sst.writer = nil
	}
	return err
}

// Delete removes the SSTable file from disk.
func (sst *SSTable) Delete() error {
	sst.mu.RLock()
	path := sst.path
	sst.mu.RUnlock()

	return os.Remove(path)
}

// GetPath returns the file path of the SSTable.
func (sst *SSTable) GetPath() string {
	sst.mu.RLock()
	defer sst.mu.RUnlock()
	return sst.path
}
