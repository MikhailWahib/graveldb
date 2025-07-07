package memtable

import (
	"math/rand"
	"time"
)

const (
	maxLevel    = 16
	probability = 0.5
)

// SkipListNode represents a node in the skip list data structure,
// containing the key-value pair and links to other nodes
type SkipListNode struct {
	key   string
	value string
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
func NewSkipListNode(key, value string, level int) *SkipListNode {
	return &SkipListNode{
		key:   key,
		value: value,
		next:  make([]*SkipListNode, level),
	}
}

// NewSkipList initializes and returns a new empty SkipList.
// The list is seeded with a pseudo-random generator and a head node with maxLevel pointers.
func NewSkipList() *SkipList {
	return &SkipList{
		head:     NewSkipListNode("", "", maxLevel),
		level:    1,
		maxLevel: maxLevel,
		size:     0,
		rng:      rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

type Entry struct {
	Key   string
	Value string
}

// Entries return all entries in the skiplist
func (sl *SkipList) Entries() []Entry {
	var result []Entry
	current := sl.head.next[0]

	for current != nil {
		result = append(result, Entry{
			Key:   current.key,
			Value: current.value,
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
func (sl *SkipList) Put(key, value string) {
	// Create update array to store path
	update := make([]*SkipListNode, sl.maxLevel)
	current := sl.head

	// Find all predecessor nodes at each level
	for i := sl.level - 1; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].key < key {
			current = current.next[i]
		}
		update[i] = current
	}

	// Get lowest level pointer to the node
	current = current.next[0]

	// If key exists, update the value
	if current != nil && current.key == key {
		current.value = value
		return
	}

	// Create new node with random level
	newLevel := sl.randomLevel()
	if newLevel > sl.level {
		for i := sl.level; i < newLevel; i++ {
			update[i] = sl.head
		}
		sl.level = newLevel
	}

	newNode := NewSkipListNode(key, value, newLevel)

	// Insert the node at each level
	for i := range newLevel {
		newNode.next[i] = update[i].next[i]
		update[i].next[i] = newNode
	}

	sl.size += len(key) + len(value)
}

// Get retrieves the value associated with a given key.
// Returns the value and true if found, otherwise returns an empty string and false.
func (sl *SkipList) Get(key string) (string, bool) {
	current := sl.head

	// Start from the highest level and work down
	for i := sl.level - 1; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].key < key {
			current = current.next[i]
		}
	}

	// Check the node at level 0
	current = current.next[0]
	if current != nil && current.key == key {
		return current.value, true
	}

	return "", false
}
func (sl *SkipList) Delete(key string) error {
	val, _ := sl.Get(key)

	// Ignore the case where the key is already deleted
	if val == TOMBSTONE {
		return nil
	}

	sl.Put(key, TOMBSTONE)

	sl.size -= len(val)
	sl.size += len(TOMBSTONE)
	return nil
}

// Range returns a slice of keys in the range [start, end] (inclusive).
// Traverses the SkipList starting from 'start' and collects keys up to 'end'.
func (sl *SkipList) Range(start, end string) []string {
	var result []string
	current := sl.head

	// Find the first node >= start
	for i := sl.level - 1; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].key < start {
			current = current.next[i]
		}
	}

	// Move to the first node in range
	current = current.next[0]

	// Collect all nodes in range
	for current != nil && current.key <= end {
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
func (sl *SkipList) Contains(key string) bool {
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
