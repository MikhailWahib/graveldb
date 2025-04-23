package memtable

import (
	"math/rand"
	"time"
)

const (
	maxLevel    = 16
	probability = 0.5
)

type SkipListNode struct {
	key   string
	value string
	next  []*SkipListNode
}

type SkipList struct {
	head     *SkipListNode
	level    int
	maxLevel int
	size     int
	rng      *rand.Rand
}

func NewSkipListNode(key, value string, level int) *SkipListNode {
	return &SkipListNode{
		key:   key,
		value: value,
		next:  make([]*SkipListNode, level),
	}
}

func NewSkipList() *SkipList {
	return &SkipList{
		head:     NewSkipListNode("", "", maxLevel),
		level:    1,
		maxLevel: maxLevel,
		size:     0,
		rng:      rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (sl *SkipList) randomLevel() int {
	level := 1
	for sl.rng.Float64() < probability && level < sl.maxLevel {
		level++
	}
	return level
}

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

	sl.size++
}

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

func (sl *SkipList) Delete(key string) bool {
	update := make([]*SkipListNode, sl.maxLevel)
	current := sl.head

	// Find predecessors at each level
	for i := sl.level - 1; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].key < key {
			current = current.next[i]
		}
		update[i] = current
	}

	// Get the node at level 0
	current = current.next[0]

	if current == nil || current.key != key {
		return false
	}

	// Remove the node from each level
	for i := range current.next {
		if update[i].next[i] != current {
			break
		}
		update[i].next[i] = current.next[i]
	}

	// Update the list's level if needed
	for sl.level > 1 && sl.head.next[sl.level-1] == nil {
		sl.level--
	}

	sl.size--
	return true
}

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

func (sl *SkipList) Size() int {
	return sl.size
}

func (sl *SkipList) Clear() {
	sl.head = NewSkipListNode("", "", sl.maxLevel)
	sl.level = 1
	sl.size = 0
}

func (sl *SkipList) IsEmpty() bool {
	return sl.size == 0
}

func (sl *SkipList) Contains(key string) bool {
	_, found := sl.Get(key)
	return found
}

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
