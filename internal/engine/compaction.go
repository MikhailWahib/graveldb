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

// shouldCompactTier checks if a tier should be compacted.
// Must be called with engine mutex held (either read or write lock).
func (cm *CompactionManager) shouldCompactTier(tier int) bool {
	if tier >= len(cm.engine.tiers) {
		return false
	}
	return len(cm.engine.tiers[tier]) > cm.engine.maxTablesPerTier
}

// generateOutputPath generates a unique output path for compacted SSTable.
func (cm *CompactionManager) generateOutputPath(tier int) string {
	outputDir := fmt.Sprintf("%s/sstables/T%d", cm.engine.dataDir, tier)
	err := os.MkdirAll(outputDir, 0755)
	if err != nil {
		return ""
	}
	// Atomically increment counter to avoid filename conflicts
	return fmt.Sprintf("%s/%06d.sst", outputDir, cm.engine.sstCounter.Add(1))
}

// compactTiers compacts tiers starting from the given tier.
func (cm *CompactionManager) compactTiers(start int) error {
	for tier := start; tier < len(cm.engine.tiers); tier++ {
		// Check if compaction is needed
		cm.engine.mu.RLock()
		shouldCompact := cm.shouldCompactTier(tier)
		cm.engine.mu.RUnlock()

		if !shouldCompact {
			return nil
		}

		if err := cm.compact(tier); err != nil {
			return err
		}
	}
	return nil
}

// compact compacts a single tier by merging all SSTables in it.
func (cm *CompactionManager) compact(tier int) error {
	// Acquire write lock for the entire compaction operation
	cm.engine.mu.Lock()
	defer cm.engine.mu.Unlock()

	// Double-check if compaction is still needed
	if !cm.shouldCompactTier(tier) {
		return nil
	}

	inputs := cm.engine.tiers[tier]
	if len(inputs) == 0 {
		return nil
	}

	// Generate output path (counter is atomically incremented)
	outputFile := cm.generateOutputPath(tier + 1) // e.g., T1/000123.sst
	if outputFile == "" {
		return fmt.Errorf("failed to generate output path for tier %d", tier+1)
	}

	output := sstable.NewSSTable(outputFile)

	// Ensure tiers slice is long enough
	for len(cm.engine.tiers) <= tier+1 {
		cm.engine.tiers = append(cm.engine.tiers, nil)
	}

	// Add sources to merger and open them for reading
	for _, sst := range inputs {
		if err := cm.merger.AddSource(sst); err != nil {
			return fmt.Errorf("failed to add source to merger: %w", err)
		}
		if err := sst.OpenForRead(); err != nil {
			return fmt.Errorf("failed to open SST for reading: %w", err)
		}
	}

	// Set output and open for writing
	cm.merger.SetOutput(output)
	if err := output.OpenForWrite(); err != nil {
		// Clean up opened input SSTables
		for _, sst := range inputs {
			_ = sst.Close()
		}
		return fmt.Errorf("failed to open output SST for writing: %w", err)
	}

	// Perform the merge
	if err := cm.merger.Merge(); err != nil {
		// Clean up on error
		_ = output.Close()
		for _, sst := range inputs {
			_ = sst.Close()
		}
		return fmt.Errorf("failed to merge SSTables: %w", err)
	}

	if err := cm.finalizeAndCleanup(output, inputs); err != nil {
		return err
	}

	// Update tiers structure
	cm.engine.tiers[tier] = []*sstable.SSTable{}
	cm.engine.tiers[tier+1] = append(cm.engine.tiers[tier+1], output)

	// Reset merger for next use
	cm.merger.Reset()

	return nil
}

func (cm *CompactionManager) finalizeAndCleanup(output *sstable.SSTable, inputs []*sstable.SSTable) error {
	if err := output.Close(); err != nil {
		for _, sst := range inputs {
			_ = sst.Close()
		}
		return fmt.Errorf("failed to close output SST: %w", err)
	}

	for _, sst := range inputs {
		_ = sst.Close()
		_ = sst.Delete()
	}
	return nil
}
