package diskmanager_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/MikhailWahib/graveldb/storage/diskmanager"
)

func TestDiskManager_Open(t *testing.T) {
	dm := diskmanager.NewDiskManager()
	filePath := "testfile1.txt"

	// Cleanup before and after test
	_ = os.Remove(filePath)
	defer func() {
		_ = dm.Close(filePath)
		_ = os.Remove(filePath)
	}()

	// Test creating a new file
	handle, err := dm.Open(filePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Expected no error on file creation, got %v", err)
	}
	if handle == nil {
		t.Fatal("Expected valid file handle, got nil")
	}

	// Test reopening existing file
	err = dm.Close(filePath)
	if err != nil {
		t.Fatalf("Expected no error on close, got %v", err)
	}

	handle, err = dm.Open(filePath, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Expected no error opening existing file, got %v", err)
	}
	if handle == nil {
		t.Fatal("Expected valid file handle on reopening, got nil")
	}

	// Test read-only opening
	err = dm.Close(filePath)
	if err != nil {
		t.Fatalf("Expected no error on close, got %v", err)
	}

	handle, err = dm.Open(filePath, os.O_RDONLY, 0644)
	if err != nil {
		t.Fatalf("Expected no error opening file in read-only mode, got %v", err)
	}
	if handle == nil {
		t.Fatal("Expected valid file handle on read-only opening, got nil")
	}

	// Test opening non-existent file without create flag
	nonExistentPath := "nonexistent.txt"
	_, err = dm.Open(nonExistentPath, os.O_RDWR, 0644)
	if err == nil {
		t.Fatal("Expected error opening non-existent file without create flag")
		_ = dm.Close(nonExistentPath)
		_ = os.Remove(nonExistentPath)
	}
	if !os.IsNotExist(err) {
		t.Fatalf("Expected 'file not exist' error, got %v", err)
	}
}

func TestFileHandle_ReadWriteOperations(t *testing.T) {
	dm := diskmanager.NewDiskManager()
	filePath := "testfile2.txt"

	// Cleanup before and after test
	_ = os.Remove(filePath)
	defer func() {
		_ = dm.Close(filePath)
		_ = os.Remove(filePath)
	}()

	// Create file first with proper flags
	handle, err := dm.Open(filePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	data := []byte("Hello, world!")
	n, err := handle.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("Expected no error on WriteAt, got %v", err)
	}
	if n != len(data) {
		t.Fatalf("Expected to write %d bytes, wrote %d", len(data), n)
	}

	// Sync to ensure data is written to disk
	err = handle.Sync()
	if err != nil {
		t.Fatalf("Expected no error on Sync, got %v", err)
	}

	readData := make([]byte, len(data))
	n, err = handle.ReadAt(readData, 0)
	if err != nil {
		t.Fatalf("Expected no error on ReadAt, got %v", err)
	}
	if n != len(data) {
		t.Fatalf("Expected to read %d bytes, read %d", len(data), n)
	}
	if string(readData) != string(data) {
		t.Fatalf("Expected %s, got %s", string(data), string(readData))
	}

	// Test appending data
	offset := int64(len(data))
	newData := []byte("\nHiii!")
	n, err = handle.WriteAt(newData, offset)
	if err != nil {
		t.Fatalf("Expected no error on WriteAt, got %v", err)
	}
	if n != len(newData) {
		t.Fatalf("Expected to write %d bytes, wrote %d", len(newData), n)
	}

	// Always sync after write
	err = handle.Sync()
	if err != nil {
		t.Fatalf("Expected no error on Sync, got %v", err)
	}

	// Read combined data
	readData = make([]byte, len(data)+len(newData))
	n, err = handle.ReadAt(readData, 0)
	if err != nil {
		t.Fatalf("Expected no error on ReadAt, got %v", err)
	}
	if n != len(readData) {
		t.Fatalf("Expected to read %d bytes, read %d", len(readData), n)
	}
	expectedData := "Hello, world!\nHiii!"
	if string(readData) != expectedData {
		t.Fatalf("Expected %s, got %s", expectedData, string(readData))
	}

	// Test opening and writing to non-existent file
	nonExistentPath := "nonexistent.txt"
	_, err = dm.Open(nonExistentPath, os.O_RDWR, 0644)
	if err == nil {
		t.Fatal("Expected error opening non-existent file without create flag")
		_ = dm.Close(nonExistentPath)
		_ = os.Remove(nonExistentPath)
	}
}

func TestDiskManager_Delete(t *testing.T) {
	dm := diskmanager.NewDiskManager()
	filePath := "testfile3.txt"

	// Cleanup before test
	_ = os.Remove(filePath)

	// Create file with proper flags
	handle, err := dm.Open(filePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Expected no error on Open, got %v", err)
	}

	// Write some data to ensure the file exists
	data := []byte("Test data")
	_, err = handle.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("Expected no error on WriteAt, got %v", err)
	}
	err = handle.Sync()
	if err != nil {
		t.Fatalf("Expected no error on Sync, got %v", err)
	}

	// Close before delete
	err = dm.Close(filePath)
	if err != nil {
		t.Fatalf("Expected no error on Close, got %v", err)
	}

	// Delete and verify
	err = dm.Delete(filePath)
	if err != nil {
		t.Fatalf("Expected no error on Delete, got %v", err)
	}

	_, err = os.Stat(filePath)
	if !os.IsNotExist(err) {
		t.Fatalf("Expected file %s to be deleted, but it exists", filePath)
	}

	// Test deleting non-existent file
	err = dm.Delete("nonexistent.txt")
	if err == nil {
		t.Fatal("Expected error when deleting non-existent file")
	}
	if !os.IsNotExist(err) {
		t.Fatalf("Expected 'file not exist' error, got %v", err)
	}
}

func TestFileHandle_Sync(t *testing.T) {
	dm := diskmanager.NewDiskManager()
	filePath := "testfile4.txt"

	// Cleanup before and after test
	_ = os.Remove(filePath)
	defer func() {
		_ = dm.Close(filePath)
		_ = os.Remove(filePath)
	}()

	// Create file with proper flags
	handle, err := dm.Open(filePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	data := []byte("Data to sync")
	_, err = handle.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("Expected no error on WriteAt, got %v", err)
	}

	err = handle.Sync()
	if err != nil {
		t.Fatalf("Expected no error on Sync, got %v", err)
	}

	// Verify data was synced to disk by closing and reopening
	err = dm.Close(filePath)
	if err != nil {
		t.Fatalf("Expected no error on Close, got %v", err)
	}

	handle, err = dm.Open(filePath, os.O_RDONLY, 0644)
	if err != nil {
		t.Fatalf("Expected no error on reopening file, got %v", err)
	}

	readData := make([]byte, len(data))
	n, err := handle.ReadAt(readData, 0)
	if err != nil {
		t.Fatalf("Expected no error on ReadAt after Sync, got %v", err)
	}
	if n != len(data) {
		t.Fatalf("Expected to read %d bytes, read %d", len(data), n)
	}
	if string(readData) != string(data) {
		t.Fatalf("Expected %s after Sync, got %s", string(data), string(readData))
	}
}

func TestDiskManager_List(t *testing.T) {
	dm := diskmanager.NewDiskManager()
	testDir := "test_list_dir"

	// Create test directory
	err := os.MkdirAll(testDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}
	defer os.RemoveAll(testDir)

	// Create test files
	testFiles := []string{
		"file1.txt",
		"file2.log",
		"data.txt",
	}

	for _, f := range testFiles {
		path := filepath.Join(testDir, f)
		handle, err := dm.Open(path, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			t.Fatalf("Failed to create test file %s: %v", f, err)
		}
		defer func(p string) {
			_ = dm.Close(p)
		}(path)
		_ = handle.Close() // Close handle after creating file
	}

	// Test listing all files
	files, err := dm.List(testDir, "")
	if err != nil {
		t.Fatalf("Expected no error listing files, got %v", err)
	}
	if len(files) != len(testFiles) {
		t.Fatalf("Expected %d files, got %d", len(testFiles), len(files))
	}

	// Test filtering files
	txtFiles, err := dm.List(testDir, ".txt")
	if err != nil {
		t.Fatalf("Expected no error listing .txt files, got %v", err)
	}
	if len(txtFiles) != 2 {
		t.Fatalf("Expected 2 .txt files, got %d", len(txtFiles))
	}

	// Test listing non-existent directory
	_, err = dm.List("nonexistent_dir", "")
	if err == nil {
		t.Fatal("Expected error listing non-existent directory")
	}
}

func TestFileHandle_EdgeCases(t *testing.T) {
	dm := diskmanager.NewDiskManager()
	filePath := "testfile5.txt"

	// Cleanup before and after test
	_ = os.Remove(filePath)
	defer func() {
		_ = dm.Close(filePath)
		_ = os.Remove(filePath)
	}()

	// Create file with proper flags
	handle, err := dm.Open(filePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Test writing empty data
	n, err := handle.WriteAt([]byte{}, 0)
	if err != nil {
		t.Fatalf("Expected no error writing empty data, got %v", err)
	}
	if n != 0 {
		t.Fatalf("Expected to write 0 bytes, wrote %d", n)
	}

	data := []byte("Hello")
	_, err = handle.WriteAt(data, 10) // Write at offset 10
	if err != nil {
		t.Fatalf("Expected no error writing at offset, got %v", err)
	}

	err = handle.Sync()
	if err != nil {
		t.Fatalf("Expected no error on Sync, got %v", err)
	}

	// Test reading across sparse regions
	fullData := make([]byte, 15) // 10 bytes of zeros + 5 bytes of "Hello"
	_, err = handle.ReadAt(fullData, 0)
	if err != nil {
		t.Fatalf("Expected no error reading full data, got %v", err)
	}

	// First 10 bytes should be zeros
	for i := 0; i < 10; i++ {
		if fullData[i] != 0 {
			t.Fatalf("Expected byte %d to be 0, got %d", i, fullData[i])
		}
	}

	// Last 5 bytes should be "Hello"
	if string(fullData[10:15]) != "Hello" {
		t.Fatalf("Expected 'Hello' at offset 10, got '%s'", string(fullData[10:15]))
	}

	// Test reading beyond file size
	beyondData := make([]byte, 5)
	_, err = handle.ReadAt(beyondData, 20)
	if err == nil {
		t.Fatal("Expected error reading beyond file size")
	}
}
