package ondisk

import (
	"fmt"
	"io"

	"github.com/cristaloleg/didis/internal/core"

	"github.com/cockroachdb/pebble"
)

var _ core.Store = &Store{}

type Store struct {
	db *pebble.DB

	syncOpt *pebble.WriteOptions
}

type Config struct {
	// Dir where data will be located.
	Dir string `json:"dir" yaml:"dir"`

	NoSync bool

	// BytesPerSync is from [pebble.Options].
	BytesPerSync int

	// DisableWAL is from [pebble.Options].
	DisableWAL bool

	// ErrorIfExists is from [pebble.Options].
	ErrorIfExists bool

	// ErrorIfNotExists is from [pebble.Options].
	ErrorIfNotExists bool

	// MaxOpenFiles is from [pebble.Options].
	MaxOpenFiles int

	// MemTableSize is from [pebble.Options].
	MemTableSize uint64

	// ReadOnly is from [pebble.Options].
	ReadOnly bool

	// WALDir is from [pebble.Options].
	WALDir string
}

func Open(cfg Config) (*Store, error) {
	opts := &pebble.Options{
		BytesPerSync:     cfg.BytesPerSync,
		DisableWAL:       cfg.DisableWAL,
		ErrorIfExists:    cfg.ErrorIfExists,
		ErrorIfNotExists: cfg.ErrorIfNotExists,
		MaxOpenFiles:     cfg.MaxOpenFiles,
		MemTableSize:     cfg.MemTableSize,
		ReadOnly:         cfg.ReadOnly,
		WALDir:           cfg.WALDir,
	}

	db, err := pebble.Open(cfg.Dir, opts)
	if err != nil {
		return nil, fmt.Errorf("pebble open: %w", err)
	}

	s := &Store{
		db:      db,
		syncOpt: pebble.Sync,
	}

	if cfg.NoSync {
		s.syncOpt = pebble.NoSync
	}
	return s, nil
}

func tryClose(c io.Closer) {
	if c != nil {
		c.Close()
	}
}
