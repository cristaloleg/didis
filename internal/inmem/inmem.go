package inmem

import (
	"sync"

	"github.com/cristaloleg/didis/internal/core"
)

var _ core.Store = &Store{}

type Store struct {
	mu sync.RWMutex
	m  map[string][]byte
}

func New() *Store {
	return &Store{
		m: make(map[string][]byte, 1024),
	}
}
