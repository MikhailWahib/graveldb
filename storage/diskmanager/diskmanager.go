// File: diskmanager/diskmanager.go
package diskmanager

import (
	"os"
	"strings"
)

// FileHandle abstracts a file with support for random access and seeking.
type FileHandle interface {
	ReadAt(b []byte, off int64) (int, error)
	WriteAt(b []byte, off int64) (int, error)
	Close() error
	Sync() error
	Seek(offset int64, whence int) (int64, error)
}

type fileHandle struct {
	file *os.File
}

func NewFileHandle(file *os.File) FileHandle { return &fileHandle{file: file} }

func (fh *fileHandle) ReadAt(b []byte, off int64) (int, error) { return fh.file.ReadAt(b, off) }

func (fh *fileHandle) WriteAt(b []byte, off int64) (int, error) { return fh.file.WriteAt(b, off) }

func (fh *fileHandle) Close() error { return fh.file.Close() }

func (fh *fileHandle) Sync() error { return fh.file.Sync() }

func (fh *fileHandle) Seek(offset int64, whence int) (int64, error) {
	return fh.file.Seek(offset, whence)
}

// DiskManager defines methods for file operations.
type DiskManager interface {
	Open(path string, flags int, perm os.FileMode) (FileHandle, error)
	Delete(path string) error
	List(dir string, filter string) ([]string, error)
	ReadAt(path string, b []byte, off int64) (int, error)
	WriteAt(path string, b []byte, off int64) (int, error)
	Sync(path string) error
	Close(path string) error
}

type diskManager struct {
	fileHandles map[string]FileHandle
}

// NewDiskManager creates a new DiskManager instance.
func NewDiskManager() DiskManager {
	return &diskManager{
		fileHandles: make(map[string]FileHandle),
	}
}

// Open opens a file with the given flags and permissions.
// It caches the file handle keyed by path.
func (dm *diskManager) Open(path string, flags int, perm os.FileMode) (FileHandle, error) {
	if handle, exists := dm.fileHandles[path]; exists {
		return handle, nil
	}
	file, err := os.OpenFile(path, flags, perm)
	if err != nil {
		return nil, err
	}
	handle := NewFileHandle(file)
	dm.fileHandles[path] = handle
	return handle, nil
}

func (dm *diskManager) Delete(path string) error {
	if handle, exists := dm.fileHandles[path]; exists {
		_ = handle.Close()
		delete(dm.fileHandles, path)
	}
	return os.Remove(path)
}

func (dm *diskManager) List(dir string, filter string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var files []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if filter == "" || strings.Contains(entry.Name(), filter) {
			files = append(files, entry.Name())
		}
	}
	return files, nil
}

func (dm *diskManager) ReadAt(path string, b []byte, off int64) (int, error) {
	handle, err := dm.Open(path, os.O_RDONLY, 0644)
	if err != nil {
		return 0, err
	}
	return handle.ReadAt(b, off)
}

func (dm *diskManager) WriteAt(path string, b []byte, off int64) (int, error) {
	handle, err := dm.Open(path, os.O_RDWR, 0644)
	if err != nil {
		return 0, err
	}
	return handle.WriteAt(b, off)
}

func (dm *diskManager) Sync(path string) error {
	handle, err := dm.Open(path, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	return handle.Sync()
}

func (dm *diskManager) Close(path string) error {
	handle, exists := dm.fileHandles[path]
	if !exists {
		return nil
	}
	err := handle.Close()
	if err != nil {
		return err
	}
	delete(dm.fileHandles, path)
	return nil
}
