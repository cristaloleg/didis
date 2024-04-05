package inmem

import (
	"strconv"

	"github.com/cristaloleg/didis/internal/core"
)

func (s *Store) setNum(key []byte, by int) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	val, ok := s.m[string(key)]
	if !ok {
		val = []byte("0")
	}

	num, err := strconv.ParseInt(string(val), 10, 64)
	if err != nil {
		return 0, core.ErrNotIntOrOutOfRange
	}
	num += int64(by)

	s.m[string(key)] = []byte(strconv.FormatInt(num, 10))
	return num, nil
}
