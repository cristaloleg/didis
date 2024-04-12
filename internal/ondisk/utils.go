package ondisk

import (
	"errors"
	"strconv"

	"github.com/cristaloleg/didis/internal/core"

	"github.com/cockroachdb/pebble"
)

func (s *Store) setNum(key []byte, by int) (int64, error) {
	b := s.db.NewIndexedBatch()

	val, closer, err := b.Get(key)
	if err != nil {
		if !errors.Is(err, pebble.ErrNotFound) {
			return 0, err
		}
		val = []byte("0")
	}
	defer tryClose(closer)

	num, err := strconv.ParseInt(string(val), 10, 64)
	if err != nil {
		return 0, core.ErrNotIntOrOutOfRange
	}
	num += int64(by)

	realVal := []byte(strconv.FormatInt(num, 10))

	if err := b.Set(key, realVal, s.syncOpt); err != nil {
		return 0, err
	}
	if err := b.Commit(s.syncOpt); err != nil {
		return 0, err
	}
	return num, nil
}
