package sstable

import (
	"encoding/binary"
	"fmt"
	"os"
	"sort"

	"github.com/MikhailWahib/graveldb/internal/common"
	"github.com/MikhailWahib/graveldb/internal/diskmanager"
	"github.com/MikhailWahib/graveldb/internal/utils"
)

type SSTReader struct {
	dm        diskmanager.DiskManager
	file      diskmanager.FileHandle
	index     []IndexEntry
	indexBase int64
}

func NewSSTReader(dm diskmanager.DiskManager) *SSTReader {
	return &SSTReader{dm: dm}
}

func (r *SSTReader) Open(filename string) error {
	file, err := r.dm.Open(filename, os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("failed to open SST file: %w", err)
	}
	r.file = file

	// Load footer: last 16 bytes
	stat, err := r.file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat SST file: %w", err)
	}

	if stat.Size() < 16 {
		return fmt.Errorf("file too small to be valid SSTable")
	}

	footerOffset := stat.Size() - 16
	buf := make([]byte, 16)
	_, err = r.file.ReadAt(buf, footerOffset)
	if err != nil {
		return fmt.Errorf("failed to read footer: %w", err)
	}

	indexOffset := int64(binary.BigEndian.Uint64(buf[:8]))
	indexSize := int64(binary.BigEndian.Uint64(buf[8:16]))
	r.indexBase = indexOffset

	// Parse sparse index
	index := []IndexEntry{}
	offset := indexOffset
	end := indexOffset + indexSize

	for offset < end {
		e := utils.ReadEntryWithPrefix(r.file, offset)
		if e.Err != nil {
			return fmt.Errorf("failed to read index entry: %w", err)
		}

		// Read offset immediately after key
		offsetBuf := make([]byte, 8)
		_, err = r.file.ReadAt(offsetBuf, e.NewOffset)
		if err != nil {
			return fmt.Errorf("failed to read index offset: %w", err)
		}
		dataOffset := int64(binary.BigEndian.Uint64(offsetBuf))
		index = append(index, IndexEntry{Key: e.Key, Offset: dataOffset})

		offset = e.NewOffset + 8
	}

	r.index = index
	return nil
}

func (r *SSTReader) Close() error {
	return r.file.Close()
}

// Lookup performs a binary search over the sparse index and returns the entry if found.
func (r *SSTReader) Lookup(key []byte) ([]byte, error) {
	pos := sort.Search(len(r.index), func(i int) bool {
		return utils.CompareKeys(r.index[i].Key, key) >= 0
	})

	if pos == len(r.index) {
		// Key is beyond last indexed key — not found
		return nil, fmt.Errorf("key not found")
	}

	// Linear scan from found position
	offset := r.index[pos].Offset
	for {
		e := utils.ReadEntryWithPrefix(r.file, offset)
		if e.Err != nil {
			return nil, fmt.Errorf("failed to read index entry: %w", e.Err)
		}

		cmp := utils.CompareKeys(e.Key, key)
		if cmp == 0 {
			// Key found, with a tombstone check, return nil (not found)
			if common.EntryType(e.Type) == common.DeleteEntry {
				return nil, fmt.Errorf("key not found")
			}

			// Key found, return the value
			return e.Value, nil
		}

		if cmp > 0 {
			break
		}

		offset = e.NewOffset
	}

	return nil, fmt.Errorf("key not found")
}
