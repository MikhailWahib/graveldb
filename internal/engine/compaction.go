package engine

import (
	"fmt"
	"os"
	"sync/atomic"

	"github.com/MikhailWahib/graveldb/internal/sstable"
)

type CompactionManager struct {
	tiers      *[][]*sstable.SSTable
	merger     sstable.Merger
	sstCounter *atomic.Uint64
	dataDir    string
}

func NewCompactionManager(dataDir string, tiers *[][]*sstable.SSTable, sstCounter *atomic.Uint64) *CompactionManager {
	return &CompactionManager{dataDir: dataDir, tiers: tiers, merger: *sstable.NewMerger(), sstCounter: sstCounter}
}

func (cm *CompactionManager) shouldCompactTier(tier int) bool {
	return len((*cm.tiers)[tier]) > MAX_TABLES_PER_TIER
}

func (cm *CompactionManager) generateOutputPath(tier int) string {
	outputDir := fmt.Sprintf("%s/sstables/T%d", cm.dataDir, tier)
	err := os.MkdirAll(outputDir, 0755)
	if err != nil {
		return ""
	}

	return fmt.Sprintf("%s/%06d.sst", outputDir, cm.sstCounter.Load())
}

func (cm *CompactionManager) compactTierRecursive(start int) error {
	for tier := start; tier < len(*cm.tiers); tier++ {
		if !cm.shouldCompactTier(tier) {
			return nil
		}
		if err := cm.compactTier(tier); err != nil {
			return err
		}
	}
	return nil
}

func (cm *CompactionManager) compactTier(tier int) error {
	if !cm.shouldCompactTier(tier) {
		return nil
	}

	inputs := (*cm.tiers)[tier]
	outputFile := cm.generateOutputPath(tier + 1) // e.g., T1/000123.sst
	output := sstable.NewSSTable(outputFile)

	// Ensure tiers slice is long enough
	for len(*cm.tiers) <= tier+1 {
		*cm.tiers = append(*cm.tiers, nil)
	}

	// Add sources to merger
	for _, sst := range inputs {
		cm.merger.AddSource(sst)
		if err := sst.OpenForRead(); err != nil {
			return err
		}
	}
	cm.merger.SetOutput(output)
	if err := output.OpenForWrite(); err != nil {
		return err
	}

	if err := cm.merger.Merge(); err != nil {
		return err
	}

	if err := output.Close(); err != nil {
		return err
	}
	// Cleanup
	for _, sst := range inputs {
		_ = sst.Delete() // delete file from disk
	}
	(*cm.tiers)[tier] = []*sstable.SSTable{}
	(*cm.tiers)[tier+1] = append((*cm.tiers)[tier+1], output)

	cm.merger.Clear()
	return nil
}
