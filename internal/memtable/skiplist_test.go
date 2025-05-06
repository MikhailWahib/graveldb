package memtable_test

import (
	"testing"

	skiplist "github.com/MikhailWahib/graveldb/internal/memtable"
	"github.com/stretchr/testify/assert"
)

func TestSkipListPutAndGet(t *testing.T) {
	sl := skiplist.NewSkipList()

	sl.Put("apple", "red")
	sl.Put("banana", "yellow")
	sl.Put("cherry", "dark red")
	sl.Put("Hello", "World")
	sl.Put("hello", "world")
	sl.Put("123", "456")
	sl.Put("abc", "def")

	tests := []struct {
		key, expectedValue string
		expectedFound      bool
	}{
		{"apple", "red", true},
		{"banana", "yellow", true},
		{"cherry", "dark red", true},
		{"grape", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			actualValue, found := sl.Get(tt.key)
			assert.Equal(t, tt.expectedFound, found, "unexpected found value for key %v", tt.key)
			assert.Equal(t, tt.expectedValue, actualValue, "unexpected value for key %v", tt.key)
		})
	}
}

func TestSkipListUpdate(t *testing.T) {
	sl := skiplist.NewSkipList()

	sl.Put("apple", "red")
	sl.Put("apple", "green")

	actualValue, found := sl.Get("apple")
	assert.True(t, found, "expected apple to be found")
	assert.Equal(t, "green", actualValue, "expected 'green' for 'apple'")
}

func TestSkipListDelete(t *testing.T) {
	sl := skiplist.NewSkipList()

	sl.Put("apple", "red")
	sl.Put("banana", "yellow")
	sl.Put("cherry", "dark red")

	sl.Delete("banana")

	_, found := sl.Get("banana")
	assert.False(t, found, "expected 'banana' to be deleted")

	// Test that other keys are still there
	val, found := sl.Get("apple")
	assert.True(t, found, "expected 'apple' to be found")
	assert.Equal(t, "red", val, "expected 'apple' to be 'red'")

	val, found = sl.Get("cherry")
	assert.True(t, found, "expected 'cherry' to be found")
	assert.Equal(t, "dark red", val, "expected 'cherry' to be 'dark red'")
}

func TestSkipListEdgeCases(t *testing.T) {
	sl := skiplist.NewSkipList()

	// Test inserting and deleting the same key
	sl.Put("apple", "red")
	sl.Delete("apple")

	_, found := sl.Get("apple")
	assert.False(t, found, "expected 'apple' to be deleted")

	// Test inserting after deletion
	sl.Put("apple", "green")

	// "apple" should now return the updated value
	val, found := sl.Get("apple")
	assert.True(t, found, "expected 'apple' to be found after re-insertion")
	assert.Equal(t, "green", val, "expected 'green' for 'apple'")
}

func TestSkipListEmpty(t *testing.T) {
	sl := skiplist.NewSkipList()

	// Test that the list is empty
	_, found := sl.Get("apple")
	assert.False(t, found, "expected 'apple' to not be found in empty skip list")

	// Test deletion on empty skip list
	sl.Delete("apple") // Should not cause issues
	_, found = sl.Get("apple")
	assert.False(t, found, "expected 'apple' to not be found after deletion")
}
