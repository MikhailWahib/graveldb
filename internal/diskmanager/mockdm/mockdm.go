// Package mockdm provides a mock implementation of the disk manager for testing
package mockdm

import (
	"io"
	"os"
	"strings"
	"time"

	"github.com/MikhailWahib/graveldb/internal/diskmanager"
)

// MockFile implements diskmanager.FileHandle for testing purposes
type MockFile struct {
	data []byte
	name string
}

// WriteAt writes len(b) bytes to the file starting at byte offset off
func (m *MockFile) WriteAt(b []byte, off int64) (int, error) {
	// Extend the slice if needed
	requiredLen := int(off) + len(b)
	if requiredLen > len(m.data) {
		newData := make([]byte, requiredLen)
		copy(newData, m.data)
		m.data = newData
	}
	return copy(m.data[off:], b), nil
}

// ReadAt reads len(b) bytes from the file starting at byte offset off
func (m *MockFile) ReadAt(b []byte, off int64) (int, error) {
	if off >= int64(len(m.data)) {
		return 0, io.EOF
	}
	n := copy(b, m.data[off:])
	if n < len(b) {
		return n, io.EOF
	}
	return n, nil
}

// Close closes the mock file
func (m *MockFile) Close() error {
	return nil
}

// Sync simulates syncing file contents to disk
func (m *MockFile) Sync() error {
	return nil
}

// Stat returns file information
func (m *MockFile) Stat() (os.FileInfo, error) {
	return &testFileInfo{size: int64(len(m.data)), name: m.name}, nil
}

type testFileInfo struct {
	size int64
	name string
}

func (m *testFileInfo) Name() string       { return m.name }
func (m *testFileInfo) Size() int64        { return m.size }
func (m *testFileInfo) Mode() os.FileMode  { return 0644 }
func (m *testFileInfo) ModTime() time.Time { return time.Now() }
func (m *testFileInfo) IsDir() bool        { return false }
func (m *testFileInfo) Sys() any           { return nil }

// MockDiskManager implements diskmanager.DiskManager interface for testing
type MockDiskManager struct {
	files map[string]*MockFile
}

// NewMockDiskManager creates a new MockDiskManager instance
func NewMockDiskManager() *MockDiskManager {
	return &MockDiskManager{
		files: make(map[string]*MockFile),
	}
}

// Open creates or opens a mock file
func (dm *MockDiskManager) Open(path string, _ int, _ os.FileMode) (diskmanager.FileHandle, error) {
	if file, exists := dm.files[path]; exists {
		return file, nil
	}

	file := &MockFile{
		data: []byte{},
		name: path,
	}
	dm.files[path] = file
	return file, nil
}

// Delete removes a mock file
func (dm *MockDiskManager) Delete(path string) error {
	delete(dm.files, path)
	return nil
}

// List returns mock files matching the filter
func (dm *MockDiskManager) List(_ string, filter string) ([]string, error) {
	var files []string
	for name := range dm.files {
		if filter == "" || strings.Contains(name, filter) {
			files = append(files, name)
		}
	}
	return files, nil
}

// Close closes a mock file
func (dm *MockDiskManager) Close(_ string) error {
	return nil
}
