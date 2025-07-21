// Package config provides configuration structures and defaults for GravelDB.
package config

import (
	"time"
)

const (
	defaultMaxMemtableSize  = 4 * 1024 * 1024
	defaultMaxTablesPerTier = 4
	defaultIndexInterval    = 16
	defaultFlushThreshold   = 64 * 1024
	defaultFlushInterval    = 10 * time.Millisecond
)

// Config holds all tunable parameters for GravelDB's performance and durability.
type Config struct {
	MaxMemtableSize  int
	MaxTablesPerTier int
	IndexInterval    int
	FlushThreshold   int
	FlushInterval    time.Duration
}

// DefaultConfig returns a Config struct populated with default values.
func DefaultConfig() *Config {
	return &Config{
		MaxMemtableSize:  defaultMaxMemtableSize,
		MaxTablesPerTier: defaultMaxTablesPerTier,
		IndexInterval:    defaultIndexInterval,
		FlushThreshold:   defaultFlushThreshold,
		FlushInterval:    defaultFlushInterval,
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
	if c.FlushThreshold == 0 {
		c.FlushThreshold = def.FlushThreshold
	}
	if c.FlushInterval == 0 {
		c.FlushInterval = def.FlushInterval
	}
}
