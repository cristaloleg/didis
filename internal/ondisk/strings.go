package ondisk

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"

	"github.com/cristaloleg/didis/internal/core"

	"github.com/cockroachdb/pebble"
)

// Strings operations https://redis.io/commands/?group=string

func (s *Store) APPEND(key, value []byte) (int, error) {
	b := s.db.NewIndexedBatch()

	val, closer, _ := b.Get(key)
	defer tryClose(closer)

	realVal := append(bytes.Clone(val), value...)

	if err := b.Set(key, realVal, s.syncOpt); err != nil {
		return 0, err
	}
	if err := b.Commit(s.syncOpt); err != nil {
		return 0, err
	}
	return len(realVal), nil
}

func (s *Store) DECR(key []byte) (int64, error) {
	return s.setNum(key, -1)
}

func (s *Store) DECRBY(key []byte, by int) (int64, error) {
	return s.setNum(key, -by)
}

func (s *Store) GET(key []byte) ([]byte, error) {
	val, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, core.ErrKeyNotFound
		}
		return nil, err
	}
	defer tryClose(closer)

	return bytes.Clone(val), nil
}

func (s *Store) GETDEL(key []byte) ([]byte, error) {
	b := s.db.NewIndexedBatch()

	val, closer, _ := s.db.Get(key)
	defer tryClose(closer)

	_ = b.Delete(key, nil)
	if err := b.Commit(s.syncOpt); err != nil {
		return nil, err
	}
	return val, nil
}

// TODO: GETEX() error

func (s *Store) GETRANGE(key []byte, start, end int) ([]byte, error) {
	val, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, core.ErrKeyNotFound
		}
		return nil, err
	}
	defer tryClose(closer)

	if start == 0 && end == -1 {
		return bytes.Clone(val), nil
	}
	if start > len(val) {
		return []byte(""), nil
	}
	if start < 0 {
		start += len(val)
	}
	if end == -1 {
		end = len(val) - 1
	}
	end = min(end, len(val)-1)
	return bytes.Clone(val[start : end+1]), nil
}

func (s *Store) GETSET(key, value []byte) ([]byte, error) {
	b := s.db.NewIndexedBatch()

	oldValue, closer, _ := b.Get(key)
	defer tryClose(closer)

	if err := b.Set(key, value, nil); err != nil {
		return nil, err
	}
	if err := b.Commit(s.syncOpt); err != nil {
		return nil, err
	}
	return bytes.Clone(oldValue), nil
}

func (s *Store) INCR(key []byte) (int64, error) {
	return s.setNum(key, 1)
}

func (s *Store) INCRBY(key []byte, by int) (int64, error) {
	return s.setNum(key, by)
}

func (s *Store) INCRBYFLOAT(key []byte, by float64) (string, error) {
	b := s.db.NewIndexedBatch()

	val, closer, err := b.Get(key)
	if err != nil {
		if !errors.Is(err, pebble.ErrNotFound) {
			return "", err
		}
		val = []byte("0")
	}
	defer tryClose(closer)

	num, err := strconv.ParseFloat(string(val), 64)
	if err != nil {
		return "0", err
	}
	num += by

	value := []byte(strconv.FormatFloat(num, 'f', -1, 64))

	if err := b.Set(key, value, s.syncOpt); err != nil {
		return "", err
	}
	if err := b.Commit(s.syncOpt); err != nil {
		return "", err
	}
	return string(value), nil
}

// TODO: LCS() error

func (s *Store) MGET(keys ...[]byte) ([][]byte, error) {
	b := s.db.NewIndexedBatch()

	res := make([][]byte, 0, len(keys))
	for i := range keys {
		val, closer, _ := b.Get(keys[i])
		defer tryClose(closer)

		res = append(res, bytes.Clone(val))
	}

	if err := b.Close(); err != nil {
		return nil, err
	}
	return res, nil
}

func (s *Store) MSET(keyvals ...[]byte) error {
	if len(keyvals)%2 == 1 {
		return fmt.Errorf("wrong number of arguments for 'mset' command")
	}

	b := s.db.NewBatch()

	for i := 0; i < len(keyvals); i += 2 {
		err := b.Set(bytes.Clone(keyvals[i]), bytes.Clone(keyvals[i+1]), nil)
		if err != nil {
			return err
		}
	}

	if err := b.Commit(s.syncOpt); err != nil {
		return err
	}
	return nil
}

// TODO: MSETNX() error { return nil }
// TODO: PSETEX() error { return nil }

func (s *Store) SET(key, value []byte) error {
	err := s.db.Set(key, value, s.syncOpt)
	return err
}

// TODO: SETEX() error { return nil }
// TODO: SETNX() error { return nil }
// TODO: SETRANGE() error { return nil }

func (s *Store) STRLEN(key []byte) (int64, error) {
	val, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, nil
		}
		return 0, err
	}
	defer tryClose(closer)

	return int64(len(val)), nil
}

// TODO: SUBSTR(key []byte, start, end int) (string, error)
