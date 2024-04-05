package inmem

import (
	"bytes"
	"fmt"
	"strconv"

	"github.com/cristaloleg/didis/internal/core"
)

func (s *Store) APPEND(key, value []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	val, _ := s.m[string(key)]
	realVal := append(val, value...)

	s.m[string(key)] = realVal
	return len(realVal), nil
}

func (s *Store) DECR(key []byte) (int64, error) {
	return s.setNum(key, -1)
}

func (s *Store) DECRBY(key []byte, by int) (int64, error) {
	return s.setNum(key, -by)
}

func (s *Store) GET(key []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	val, ok := s.m[string(key)]
	if !ok {
		return nil, core.ErrKeyNotFound
	}
	return bytes.Clone(val), nil
}

func (s *Store) GETDEL(key []byte) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	val, ok := s.m[string(key)]
	if !ok {
		return nil, core.ErrKeyNotFound
	}
	delete(s.m, string(key))

	return bytes.Clone(val), nil
}

// TODO: GETEX()

func (s *Store) GETRANGE(key []byte, start, end int) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	val, ok := s.m[string(key)]
	if !ok {
		return nil, core.ErrKeyNotFound
	}

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
	s.mu.Lock()
	defer s.mu.Unlock()

	res := s.m[string(key)]
	s.m[string(key)] = bytes.Clone(value)

	return bytes.Clone(res), nil
}

func (s *Store) INCR(key []byte) (int64, error) {
	return s.setNum(key, 1)
}

func (s *Store) INCRBY(key []byte, by int) (int64, error) {
	return s.setNum(key, by)
}

func (s *Store) INCRBYFLOAT(key []byte, by float64) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	val, ok := s.m[string(key)]
	if !ok {
		val = []byte("0")
	}

	num, err := strconv.ParseFloat(string(val), 64)
	if err != nil {
		return "0", err
	}
	num += by

	value := []byte(strconv.FormatFloat(num, 'f', -1, 64))
	s.m[string(key)] = value
	return string(value), nil
}

// TODO LCS()

func (s *Store) MGET(keys ...[]byte) ([][]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	res := [][]byte{}
	for i := range keys {
		res = append(res, bytes.Clone(s.m[string(keys[i])]))
	}
	return res, nil
}

func (s *Store) MSET(keyvals ...[]byte) error {
	if len(keyvals)%2 == 1 {
		return fmt.Errorf("wrong number of arguments for 'mset' command")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for i := 0; i < len(keyvals); i += 2 {
		s.m[string(keyvals[i])] = bytes.Clone(keyvals[i+1])
	}
	return nil
}

// TODO: MSETNX()
// TODO: PSETEX()

func (s *Store) SET(key, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.m[string(key)] = bytes.Clone(value)
	return nil
}

// TODO: SETEX()
// TODO: SETNX()
// TODO: SETRANGE(key []byte, offset int, value []byte)

func (s *Store) STRLEN(key []byte) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return int64(len(s.m[string(key)])), nil
}

// TODO: SUBSTR(key []byte, start, end int)
