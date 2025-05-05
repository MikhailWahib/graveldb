package mockdm

import (
	"os"
	"strings"
	"time"

	"github.com/MikhailWahib/graveldb/internal/diskmanager"
)

type MockFile struct {
	data []byte
	name string
}

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

func (m *MockFile) ReadAt(b []byte, off int64) (int, error) {
	if off >= int64(len(m.data)) {
		return 0, os.ErrInvalid
	}
	return copy(b, m.data[off:]), nil
}

func (m *MockFile) Close() error {
	return nil
}

func (m *MockFile) Sync() error {
	return nil
}

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

func NewMockDiskManager() *MockDiskManager {
	return &MockDiskManager{
		files: make(map[string]*MockFile),
	}
}

func (dm *MockDiskManager) Open(path string, flag int, perm os.FileMode) (diskmanager.FileHandle, error) {
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

func (dm *MockDiskManager) Delete(path string) error {
	delete(dm.files, path)
	return nil
}

func (dm *MockDiskManager) List(dir string, filter string) ([]string, error) {
	var files []string
	for name := range dm.files {
		if filter == "" || strings.Contains(name, filter) {
			files = append(files, name)
		}
	}
	return files, nil
}

func (dm *MockDiskManager) Close(path string) error {
	delete(dm.files, path)
	return nil
}
