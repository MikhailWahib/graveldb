package diskmanager_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/MikhailWahib/graveldb/internal/diskmanager"
	"github.com/stretchr/testify/require"
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
	require.NoError(t, err, "Expected no error on file creation")
	require.NotNil(t, handle, "Expected valid file handle, got nil")

	// Test reopening existing file
	err = dm.Close(filePath)
	require.NoError(t, err, "Expected no error on close")

	handle, err = dm.Open(filePath, os.O_RDWR, 0644)
	require.NoError(t, err, "Expected no error opening existing file")
	require.NotNil(t, handle, "Expected valid file handle on reopening")

	// Test read-only opening
	err = dm.Close(filePath)
	require.NoError(t, err, "Expected no error on close")

	handle, err = dm.Open(filePath, os.O_RDONLY, 0644)
	require.NoError(t, err, "Expected no error opening file in read-only mode")
	require.NotNil(t, handle, "Expected valid file handle on read-only opening")

	// Test opening non-existent file without create flag
	nonExistentPath := "nonexistent.txt"
	_, err = dm.Open(nonExistentPath, os.O_RDWR, 0644)
	require.Error(t, err, "Expected error opening non-existent file without create flag")
	require.True(t, os.IsNotExist(err), "Expected 'file not exist' error")
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
	require.NoError(t, err, "Expected no error, got %v", err)

	data := []byte("Hello, world!")
	n, err := handle.WriteAt(data, 0)
	require.NoError(t, err, "Expected no error on WriteAt")
	require.Equal(t, len(data), n, "Expected to write %d bytes, wrote %d", len(data), n)

	// Sync to ensure data is written to disk
	err = handle.Sync()
	require.NoError(t, err, "Expected no error on Sync")

	readData := make([]byte, len(data))
	n, err = handle.ReadAt(readData, 0)
	require.NoError(t, err, "Expected no error on ReadAt")
	require.Equal(t, len(data), n, "Expected to read %d bytes, read %d", len(data), n)
	require.Equal(t, string(data), string(readData), "Expected %s, got %s", string(data), string(readData))

	// Test appending data
	offset := int64(len(data))
	newData := []byte("\nHiii!")
	n, err = handle.WriteAt(newData, offset)
	require.NoError(t, err, "Expected no error on WriteAt")
	require.Equal(t, len(newData), n, "Expected to write %d bytes, wrote %d", len(newData), n)

	// Always sync after write
	err = handle.Sync()
	require.NoError(t, err, "Expected no error on Sync")

	// Read combined data
	readData = make([]byte, len(data)+len(newData))
	n, err = handle.ReadAt(readData, 0)
	require.NoError(t, err, "Expected no error on ReadAt")
	require.Equal(t, len(readData), n, "Expected to read %d bytes, read %d", len(readData), n)
	expectedData := "Hello, world!\nHiii!"
	require.Equal(t, expectedData, string(readData), "Expected %s, got %s", expectedData, string(readData))

	// Test opening and writing to non-existent file
	nonExistentPath := "nonexistent.txt"
	_, err = dm.Open(nonExistentPath, os.O_RDWR, 0644)
	require.Error(t, err, "Expected error opening non-existent file without create flag")
}

func TestDiskManager_Delete(t *testing.T) {
	dm := diskmanager.NewDiskManager()
	filePath := "testfile3.txt"

	// Cleanup before test
	_ = os.Remove(filePath)

	// Create file with proper flags
	handle, err := dm.Open(filePath, os.O_CREATE|os.O_RDWR, 0644)
	require.NoError(t, err, "Expected no error on Open")

	// Write some data to ensure the file exists
	data := []byte("Test data")
	_, err = handle.WriteAt(data, 0)
	require.NoError(t, err, "Expected no error on WriteAt")
	err = handle.Sync()
	require.NoError(t, err, "Expected no error on Sync")

	// Close before delete
	err = dm.Close(filePath)
	require.NoError(t, err, "Expected no error on Close")

	// Delete and verify
	err = dm.Delete(filePath)
	require.NoError(t, err, "Expected no error on Delete")

	_, err = os.Stat(filePath)
	require.True(t, os.IsNotExist(err), "Expected file %s to be deleted, but it exists", filePath)

	// Test deleting non-existent file
	err = dm.Delete("nonexistent.txt")
	require.Error(t, err, "Expected error when deleting non-existent file")
	require.True(t, os.IsNotExist(err), "Expected 'file not exist' error")
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
	require.NoError(t, err, "Expected no error, got %v", err)

	data := []byte("Data to sync")
	_, err = handle.WriteAt(data, 0)
	require.NoError(t, err, "Expected no error on WriteAt")

	err = handle.Sync()
	require.NoError(t, err, "Expected no error on Sync")

	// Verify data was synced to disk by closing and reopening
	err = dm.Close(filePath)
	require.NoError(t, err, "Expected no error on Close")

	handle, err = dm.Open(filePath, os.O_RDONLY, 0644)
	require.NoError(t, err, "Expected no error on reopening file")

	readData := make([]byte, len(data))
	n, err := handle.ReadAt(readData, 0)
	require.NoError(t, err, "Expected no error on ReadAt after Sync")
	require.Equal(t, len(data), n, "Expected to read %d bytes, read %d", len(data), n)
	require.Equal(t, string(data), string(readData), "Expected %s after Sync, got %s", string(data), string(readData))
}

func TestDiskManager_List(t *testing.T) {
	dm := diskmanager.NewDiskManager()
	testDir := "test_list_dir"

	// Create test directory
	err := os.MkdirAll(testDir, 0755)
	require.NoError(t, err, "Failed to create test directory")

	defer func() {
		if err := os.RemoveAll(testDir); err != nil {
			t.Errorf("failed to cleanup test directory: %v", err)
		}
	}()

	// Create test files
	testFiles := []string{
		"file1.txt",
		"file2.log",
		"data.txt",
	}

	for _, f := range testFiles {
		path := filepath.Join(testDir, f)
		handle, err := dm.Open(path, os.O_CREATE|os.O_RDWR, 0644)
		require.NoError(t, err, "Failed to create test file %s", f)
		defer func(p string) {
			_ = dm.Close(p)
		}(path)
		_ = handle.Close() // Close handle after creating file
	}

	// Test listing all files
	files, err := dm.List(testDir, "")
	require.NoError(t, err, "Expected no error listing files")
	require.Len(t, files, len(testFiles), "Expected %d files, got %d", len(testFiles), len(files))

	// Test filtering files
	txtFiles, err := dm.List(testDir, ".txt")
	require.NoError(t, err, "Expected no error listing .txt files")
	require.Len(t, txtFiles, 2, "Expected 2 .txt files, got %d", len(txtFiles))

	// Test listing non-existent directory
	_, err = dm.List("nonexistent_dir", "")
	require.Error(t, err, "Expected error listing non-existent directory")
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
	require.NoError(t, err, "Expected no error, got %v", err)

	// Test writing empty data
	n, err := handle.WriteAt([]byte{}, 0)
	require.NoError(t, err, "Expected no error writing empty data")
	require.Zero(t, n, "Expected to write 0 bytes, wrote %d", n)

	data := []byte("Hello")
	_, err = handle.WriteAt(data, 10) // Write at offset 10
	require.NoError(t, err, "Expected no error writing at offset")

	err = handle.Sync()
	require.NoError(t, err, "Expected no error on Sync")

	// Test reading across sparse regions
	fullData := make([]byte, 15) // 10 bytes of zeros + 5 bytes of "Hello"
	_, err = handle.ReadAt(fullData, 0)
	require.NoError(t, err, "Expected no error reading full data")

	// First 10 bytes should be zeros
	for i := range 10 {
		require.Zero(t, fullData[i], "Expected byte %d to be 0", i)
	}

	// Last 5 bytes should be "Hello"
	require.Equal(t, "Hello", string(fullData[10:15]), "Expected 'Hello' at offset 10")
}
