// Package config provides configuration structures and defaults for GravelDB.
package config

import (
	"time"
)

const (
	defaultMaxMemtableSize   = 32 * 1024 * 1024
	defaultMaxTablesPerTier  = 4
	defaultIndexInterval     = 16
	defaultWALFlushThreshold = 64 * 1024
	defaultWALFlushInterval  = 10 * time.Millisecond
)

// Config holds all tunable parameters for GravelDB's performance and durability.
type Config struct {
	MaxMemtableSize   int
	MaxTablesPerTier  int
	IndexInterval     int
	WALFlushThreshold int
	WALFlushInterval  time.Duration
}

// DefaultConfig returns a Config struct populated with default values.
func DefaultConfig() *Config {
	return &Config{
		MaxMemtableSize:   defaultMaxMemtableSize,
		MaxTablesPerTier:  defaultMaxTablesPerTier,
		IndexInterval:     defaultIndexInterval,
		WALFlushThreshold: defaultWALFlushThreshold,
		WALFlushInterval:  defaultWALFlushInterval,
	}
}

// FillDefaults sets any zero-value fields in the Config to their default values.
func (c *Config) FillDefaults() {
	def := DefaultConfig()
	if c.MaxMemtableSize == 0 {
		c.MaxMemtableSize = def.MaxMemtableSize
	}
	if c.MaxTablesPerTier == 0 {
		c.MaxTablesPerTier = def.MaxTablesPerTier
	}
	if c.IndexInterval == 0 {
		c.IndexInterval = def.IndexInterval
	}
	if c.WALFlushThreshold == 0 {
		c.WALFlushThreshold = def.WALFlushThreshold
	}
	if c.WALFlushInterval == 0 {
		c.WALFlushInterval = def.WALFlushInterval
	}
}
