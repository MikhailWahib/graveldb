package memtable_test

import (
	"testing"

	skiplist "github.com/MikhailWahib/graveldb/internal/memtable"
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

	sl.Print()

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
			if found != tt.expectedFound || actualValue != tt.expectedValue {
				t.Errorf("For key %v: expected (%v, %v), got (%v, %v)", tt.key, tt.expectedValue, tt.expectedFound, actualValue, found)
			}
		})
	}
}

func TestSkipListUpdate(t *testing.T) {
	sl := skiplist.NewSkipList()

	sl.Put("apple", "red")

	sl.Put("apple", "green")

	actualValue, found := sl.Get("apple")
	if !found || actualValue != "green" {
		t.Errorf("Expected 'green' for 'apple', got %v", actualValue)
	}
}

func TestSkipListDelete(t *testing.T) {
	sl := skiplist.NewSkipList()

	sl.Put("apple", "red")
	sl.Put("banana", "yellow")
	sl.Put("cherry", "dark red")

	sl.Delete("banana")

	_, found := sl.Get("banana")
	if found {
		t.Errorf("Expected 'banana' to be deleted, but it was found")
	}

	// Test that other keys are still there
	if val, found := sl.Get("apple"); !found || val != "red" {
		t.Errorf("Expected 'apple' to be 'red', got %v", val)
	}
	if val, found := sl.Get("cherry"); !found || val != "dark red" {
		t.Errorf("Expected 'cherry' to be 'dark red', got %v", val)
	}
}

func TestSkipListEdgeCases(t *testing.T) {
	sl := skiplist.NewSkipList()

	// Test inserting and deleting the same key
	sl.Put("apple", "red")
	sl.Delete("apple")

	if _, found := sl.Get("apple"); found {
		t.Errorf("Expected 'apple' to be deleted, but it was found")
	}

	// Test inserting after deletion
	sl.Put("apple", "green")

	// "apple" should now return the updated value
	val, found := sl.Get("apple")
	if !found || val != "green" {
		t.Errorf("Expected 'green' for 'apple', got %v", val)
	}
}

func TestSkipListEmpty(t *testing.T) {
	sl := skiplist.NewSkipList()

	// Test that the list is empty
	if _, found := sl.Get("apple"); found {
		t.Errorf("Expected 'apple' to not be found in empty skip list")
	}

	// Test deletion on empty skip list
	sl.Delete("apple") // Should not cause issues
	if _, found := sl.Get("apple"); found {
		t.Errorf("Expected 'apple' to not be found after deletion")
	}
}
