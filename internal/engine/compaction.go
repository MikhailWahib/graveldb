// Package engine implements the core storage engine, including compaction and tier management.
package engine

import (
	"fmt"
	"os"

	"github.com/MikhailWahib/graveldb/internal/sstable"
)

// CompactionManager manages the compaction process for SSTable tiers.
type CompactionManager struct {
	engine *Engine
	merger *sstable.Merger
}

// NewCompactionManager creates a new CompactionManager for the given data directory and tiers.
func NewCompactionManager(e *Engine) *CompactionManager {
	return &CompactionManager{
		engine: e,
		merger: sstable.NewMerger(),
	}
}

func (cm *CompactionManager) shouldCompactTier(tier int) bool {
	return len(cm.engine.tiers[tier]) > cm.engine.maxTablesPerTier
}

func (cm *CompactionManager) generateOutputPath(tier int) string {
	outputDir := fmt.Sprintf("%s/sstables/T%d", cm.engine.dataDir, tier)
	err := os.MkdirAll(outputDir, 0755)
	if err != nil {
		return ""
	}

	return fmt.Sprintf("%s/%06d.sst", outputDir, cm.engine.sstCounter.Load())
}

func (cm *CompactionManager) compactTierRecursive(start int) error {
	for tier := start; tier < len(cm.engine.tiers); tier++ {
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

	inputs := cm.engine.tiers[tier]
	outputFile := cm.generateOutputPath(tier + 1) // e.g., T1/000123.sst
	output := sstable.NewSSTable(outputFile)

	// Ensure tiers slice is long enough
	for len(cm.engine.tiers) <= tier+1 {
		cm.engine.tiers = append(cm.engine.tiers, nil)
	}

	// Add sources to merger
	for _, sst := range inputs {
		if err := cm.merger.AddSource(sst); err != nil {
			return err
		}
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
	cm.engine.tiers[tier] = []*sstable.SSTable{}
	cm.engine.tiers[tier+1] = append(cm.engine.tiers[tier+1], output)

	cm.merger.Reset()
	return nil
}
