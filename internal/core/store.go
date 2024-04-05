package core

import "errors"

var (
	ErrKeyNotFound        = errors.New("key not found")
	ErrNotIntOrOutOfRange = errors.New("value is not an integer or out of range")
)

type Store interface {
	StringsStore
}

type StringsStore interface {
	APPEND(key, value []byte) (int, error)
	DECR(key []byte) (int64, error)
	DECRBY(key []byte, by int) (int64, error)
	GET(key []byte) ([]byte, error)
	GETDEL(key []byte) ([]byte, error)
	// TODO: GETEX()
	GETRANGE(key []byte, start, end int) ([]byte, error)
	GETSET(key, value []byte) ([]byte, error)
	INCR(key []byte) (int64, error)
	INCRBY(key []byte, by int) (int64, error)
	INCRBYFLOAT(key []byte, by float64) (string, error)
	// TODO: LCS()
	MGET(keys ...[]byte) ([][]byte, error)
	MSET(keyvals ...[]byte) error
	// TODO: MSETNX()
	// TODO: PSETEX()
	SET(key, value []byte) error
	// TODO: SETEX()
	// TODO: SETNX()
	// TODO: SETRANGE(key []byte, offset int, value []byte)
	STRLEN(key []byte) (int64, error)
	// TODO: SUBSTR(key []byte, start, end int)
}
