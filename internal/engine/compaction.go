package engine

import "github.com/MikhailWahib/graveldb/internal/sstable"

type CompactionManager struct {
	levels map[int][]*sstable.SSTable
}

func NewCompactionManager(levels map[int][]*sstable.SSTable) *CompactionManager {
	return &CompactionManager{levels: levels}
}
