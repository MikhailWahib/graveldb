package memtable

import (
	"bytes"
	"math/rand"
	"time"

	"github.com/MikhailWahib/graveldb/internal/record"
)

const (
	maxLevel    = 16
	probability = 0.5
)

// SkipListNode represents a node in the skip list data structure
type SkipListNode struct {
	key   []byte
	entry record.Entry
	next  []*SkipListNode
}

// SkipList is a probabilistic data structure that allows for
// efficient search, insertion, and deletion operations
type SkipList struct {
	head     *SkipListNode
	level    int
	maxLevel int
	size     int
	rng      *rand.Rand
}

// NewSkipListNode creates a new SkipListNode with the given key, value, and level.
// It initializes the 'next' slice to the correct length for the node's level.
func NewSkipListNode(key []byte, entry record.Entry, level int) *SkipListNode {
	return &SkipListNode{
		key:   key,
		entry: entry,
		next:  make([]*SkipListNode, level),
	}
}

// NewSkipList initializes and returns a new empty SkipList.
// The list is seeded with a pseudo-random generator and a head node with maxLevel pointers.
func NewSkipList() *SkipList {
	return &SkipList{
		head:     NewSkipListNode([]byte{}, record.Entry{}, maxLevel),
		level:    1,
		maxLevel: maxLevel,
		size:     0,
		rng:      rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Entries return all entries in the skiplist
func (sl *SkipList) Entries() []record.Entry {
	var result []record.Entry
	current := sl.head.next[0]

	for current != nil {
		result = append(result, record.Entry{
			Key:   current.key,
			Value: current.entry.Value,
		})
		current = current.next[0]
	}

	return result
}

// randomLevel determines the level for a new node using a probabilistic model.
func (sl *SkipList) randomLevel() int {
	level := 1
	for sl.rng.Float64() < probability && level < sl.maxLevel {
		level++
	}
	return level
}

// Put inserts a new key-value pair into the SkipList or updates the value if the key already exists.
func (sl *SkipList) Put(entry record.Entry) {
	key := entry.Key
	update := make([]*SkipListNode, sl.maxLevel)
	current := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		for current.next[i] != nil && bytes.Compare(current.next[i].key, key) < 0 {
			current = current.next[i]
		}
		update[i] = current
	}

	current = current.next[0]
	if current != nil && bytes.Equal(current.key, key) {
		current.entry = entry
		return
	}

	newLevel := sl.randomLevel()
	if newLevel > sl.level {
		for i := sl.level; i < newLevel; i++ {
			update[i] = sl.head
		}
		sl.level = newLevel
	}

	newNode := &SkipListNode{
		key:   key,
		entry: entry,
		next:  make([]*SkipListNode, newLevel),
	}
	for i := range newLevel {
		newNode.next[i] = update[i].next[i]
		update[i].next[i] = newNode
	}

	sl.size += len(entry.Key) + len(entry.Value)
}

// Get retrieves the value associated with a given key.
// Returns the value and true if found, otherwise returns nil and false.
func (sl *SkipList) Get(key []byte) (record.Entry, bool) {
	current := sl.head

	// Start from the highest level and work down
	for i := sl.level - 1; i >= 0; i-- {
		for current.next[i] != nil && bytes.Compare(current.next[i].key, key) < 0 {
			current = current.next[i]
		}
	}

	// Check the node at level 0
	current = current.next[0]
	if current != nil && bytes.Equal(current.key, key) {
		return current.entry, true
	}
	return record.Entry{}, false
}

// Delete marks a key as deleted in the skiplist by setting its value to TOMBSTONE.
func (sl *SkipList) Delete(key []byte) error {
	entry, _ := sl.Get(key)

	// Ignore the case where the key is already deleted
	if entry.Type == record.DeleteEntry {
		return nil
	}

	sl.Put(record.Entry{Type: record.DeleteEntry, Key: key, Value: nil})

	sl.size -= len(entry.Value)
	return nil
}

// Range returns a slice of keys in the range [start, end] (inclusive).
// Traverses the SkipList starting from 'start' and collects keys up to 'end'.
func (sl *SkipList) Range(start, end []byte) [][]byte {
	var result [][]byte
	current := sl.head

	// Find the first node >= start
	for i := sl.level - 1; i >= 0; i-- {
		for current.next[i] != nil && bytes.Compare(current.next[i].key, start) < 0 {
			current = current.next[i]
		}
	}

	// Move to the first node in range
	current = current.next[0]

	// Collect all nodes in range
	for current != nil && bytes.Compare(current.key, end) <= 0 {
		result = append(result, current.key)
		current = current.next[0]
	}

	return result
}

// Size returns the size of key-value pairs currently stored in the SkipList in bytes.
func (sl *SkipList) Size() int {
	return sl.size
}

// Clear resets the SkipList to an empty state, retaining only the head node.
func (sl *SkipList) Clear() {
	for i := range sl.head.next {
		sl.head.next[i] = nil
	}
	sl.level = 1
	sl.size = 0
}

// IsEmpty returns true if the SkipList contains no elements.
func (sl *SkipList) IsEmpty() bool {
	return sl.size == 0
}

// Contains checks whether a given key exists in the SkipList.
// Returns true if the key is present, false otherwise.
func (sl *SkipList) Contains(key []byte) bool {
	_, found := sl.Get(key)
	return found
}

// Print outputs the structure of the SkipList level by level.
// Used primarily for debugging or visualization in development.
func (sl *SkipList) Print() {
	for i := sl.level - 1; i >= 0; i-- {
		print("Level ", i, ": ")
		current := sl.head.next[i]
		for current != nil {
			print(current.key, " ")
			current = current.next[i]
		}
		println()
	}
}
