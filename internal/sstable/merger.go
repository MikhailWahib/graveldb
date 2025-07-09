package sstable

import (
	"bytes"
	"container/heap"
	"fmt"
)

// Merger combines multiple SSTables into a single SSTable
type Merger struct {
	sources []*SSTable
	output  *SSTable
}

// NewMerger creates a new SSTable merger
func NewMerger() *Merger {
	return &Merger{
		sources: make([]*SSTable, 0),
	}
}

// AddSource adds a source SSTable to be merged
func (m *Merger) AddSource(sst *SSTable) error {
	m.sources = append(m.sources, sst)
	return nil
}

// SetOutput sets the output SSTable for the merge result
func (m *Merger) SetOutput(sst *SSTable) {
	m.output = sst
}

type iteratorItem struct {
	key      []byte
	value    []byte
	iter     *Iterator
	deleted  bool
	priority int // lower = newer
}

type iteratorHeap []*iteratorItem

func (h iteratorHeap) Len() int { return len(h) }

func (h iteratorHeap) Less(i, j int) bool {
	keyCmp := bytes.Compare(h[i].key, h[j].key)
	if keyCmp != 0 {
		return keyCmp < 0
	}
	// When keys match, pick item from newer SSTable (lower priority value wins)
	return h[i].priority < h[j].priority
}

func (h iteratorHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *iteratorHeap) Push(x any) {
	*h = append(*h, x.(*iteratorItem))
}

func (h *iteratorHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// Merge performs the actual merge operation and writes the output SST to disk
func (m *Merger) Merge() error {
	if m.output == nil {
		return fmt.Errorf("merger: output SSTable not set")
	}

	ih := &iteratorHeap{}
	heap.Init(ih)

	// Reverse assign priority: 0 = newest, N = oldest
	for i := len(m.sources) - 1; i >= 0; i-- {
		source := m.sources[i]
		iter, err := source.NewIterator()
		if err != nil {
			return err
		}
		if iter.Next() {
			heap.Push(ih, &iteratorItem{
				key:      iter.Key(),
				value:    iter.Value(),
				iter:     iter,
				deleted:  iter.IsDeleted(),
				priority: len(m.sources) - 1 - i, // lower = newer
			})
		}
	}

	var lastKey []byte

	for ih.Len() > 0 {
		item := heap.Pop(ih).(*iteratorItem)

		if lastKey != nil && bytes.Equal(item.key, lastKey) {
			if item.iter.Next() {
				heap.Push(ih, &iteratorItem{
					key:      item.iter.Key(),
					value:    item.iter.Value(),
					iter:     item.iter,
					deleted:  item.iter.IsDeleted(),
					priority: item.priority,
				})
			}
			// Skip duplicates
			continue
		}

		// Write current key to output
		if item.deleted {
			if err := m.output.AppendDelete(item.key); err != nil {
				return err
			}
		} else {
			if err := m.output.AppendPut(item.key, item.value); err != nil {
				return err
			}
		}

		lastKey = item.key

		// Push next from this iterator
		if item.iter.Next() {
			heap.Push(ih, &iteratorItem{
				key:      item.iter.Key(),
				value:    item.iter.Value(),
				iter:     item.iter,
				deleted:  item.iter.IsDeleted(),
				priority: item.priority,
			})
		}
	}

	return m.output.Finish()
}

// Reset clears the merger
func (m *Merger) Reset() {
	m.sources = make([]*SSTable, 0)
	m.output = nil
}
