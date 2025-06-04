// Package diskmanager provides interfaces and implementations for managing disk-based file operations.
// It handles file reading, writing, and other disk-related operations required by the database.
package diskmanager

import (
	"os"
	"strings"
)

// FileHandle abstracts file operations with random access, syncing, and seeking.
type FileHandle interface {
	// ReadAt reads len(b) bytes from the file starting at byte offset off.
	// It returns the number of bytes read and any error encountered.
	ReadAt(b []byte, off int64) (int, error)
	// WriteAt writes len(b) bytes to the file starting at byte offset off.
	// It returns the number of bytes written and any error encountered.
	WriteAt(b []byte, off int64) (int, error)
	// Close closes the file handle, rendering it unusable for I/O.
	Close() error
	// Sync commits the current contents of the file to stable storage.
	Sync() error
	// Stat returns the file stat
	Stat() (os.FileInfo, error)
}

type fileHandle struct {
	file *os.File
}

// NewFileHandle wraps an *os.File into a FileHandle implementation.
func NewFileHandle(file *os.File) FileHandle { return &fileHandle{file: file} }

func (fh *fileHandle) ReadAt(b []byte, off int64) (int, error) { return fh.file.ReadAt(b, off) }

func (fh *fileHandle) WriteAt(b []byte, off int64) (int, error) { return fh.file.WriteAt(b, off) }

func (fh *fileHandle) Close() error { return fh.file.Close() }

func (fh *fileHandle) Sync() error { return fh.file.Sync() }

func (fh *fileHandle) Stat() (os.FileInfo, error) { return fh.file.Stat() }

// DiskManager defines methods for file operations.
type DiskManager interface {
	// Open opens a file with specified path, flags and permissions.
	// If the file is already open, returns the existing handle.
	Open(path string, flags int, perm os.FileMode) (FileHandle, error)
	// Delete removes the named file and closes its handle if open.
	Delete(path string) error
	// List returns a slice of filenames in the specified directory
	// that contain the filter string. Empty filter matches all files.
	List(dir string, filter string) ([]string, error)
	// Close closes the file handle for the file at path if it exists.
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
