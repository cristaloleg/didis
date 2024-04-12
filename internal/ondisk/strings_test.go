package ondisk

import (
	"testing"

	"github.com/cristaloleg/didis/internal/core"

	"github.com/cristalhq/testt"
)

func TestAPPEND(t *testing.T) {
	/*
		redis> EXISTS mykey
		(integer) 0
		redis> APPEND mykey "Hello"
		(integer) 5
		redis> APPEND mykey " World"
		(integer) 11
		redis> GET mykey
		"Hello World"
		redis>
	*/

	mykey := []byte("mykey")

	s := newStore(t)
	size, err := s.APPEND(mykey, []byte("Hello"))
	testt.NoError(t, err)
	testt.MustEqual(t, size, 5)

	size, err = s.APPEND(mykey, []byte(" World"))
	testt.NoError(t, err)
	testt.MustEqual(t, size, 11)

	val, err := s.GET(mykey)
	testt.NoError(t, err)
	testt.MustEqual(t, string(val), "Hello World")
}

func TestDECR(t *testing.T) {
	/*
		redis> SET mykey "10"
		"OK"
		redis> DECR mykey
		(integer) 9
		redis> SET mykey "234293482390480948029348230948"
		"OK"
		redis> DECR mykey
		(error) value is not an integer or out of range
		redis>
	*/

	mykey := []byte("mykey")

	s := newStore(t)
	err := s.SET(mykey, []byte("10"))
	testt.NoError(t, err)

	val, err := s.DECR(mykey)
	testt.NoError(t, err)
	testt.MustEqual(t, val, int64(9))

	err = s.SET(mykey, []byte("234293482390480948029348230948"))
	testt.NoError(t, err)

	_, err = s.DECR(mykey)
	testt.WantError(t, err)
	testt.MustEqual(t, err.Error(), core.ErrNotIntOrOutOfRange.Error())
}

func TestDECRBY(t *testing.T) {
	/*
		redis> SET mykey "10"
		"OK"
		redis> DECRBY mykey 3
		(integer) 7
		redis>
	*/

	mykey := []byte("mykey")

	s := newStore(t)
	err := s.SET(mykey, []byte("10"))
	testt.NoError(t, err)

	val, err := s.DECRBY(mykey, 3)
	testt.NoError(t, err)
	testt.MustEqual(t, val, int64(7))
}

func TestGET(t *testing.T) {
	/*
		redis> GET nonexisting
		(nil)
		redis> SET mykey "Hello"
		"OK"
		redis> GET mykey
		"Hello"
		redis>
	*/

	mykey := []byte("mykey")

	s := newStore(t)
	val, err := s.GET([]byte("nonexisting"))
	testt.WantError(t, err)
	testt.MustEqual(t, err.Error(), core.ErrKeyNotFound.Error())

	err = s.SET(mykey, []byte("Hello"))

	val, err = s.GET(mykey)
	testt.NoError(t, err)
	testt.MustEqual(t, string(val), "Hello")
}

func TestGETDEL(t *testing.T) {
	/*
		redis> SET mykey "Hello"
		"OK"
		redis> GETDEL mykey
		"Hello"
		redis> GET mykey
		(nil)
		redis>
	*/

	mykey := []byte("mykey")

	s := newStore(t)
	err := s.SET(mykey, []byte("Hello"))
	testt.NoError(t, err)

	val, err := s.GETDEL(mykey)
	testt.NoError(t, err)
	testt.MustEqual(t, string(val), "Hello")

	val, err = s.GET(mykey)
	testt.WantError(t, err)
	testt.MustEqual(t, err.Error(), core.ErrKeyNotFound.Error())
}

func TestGETRANGE(t *testing.T) {
	/*
		redis> SET mykey "This is a string"
		"OK"
		redis> GETRANGE mykey 0 3
		"This"
		redis> GETRANGE mykey -3 -1
		"ing"
		redis> GETRANGE mykey 0 -1
		"This is a string"
		redis> GETRANGE mykey 10 100
		"string"
		redis>
	*/

	mykey := []byte("mykey")

	s := newStore(t)
	err := s.SET(mykey, []byte("This is a string"))
	testt.NoError(t, err)

	val, err := s.GETRANGE(mykey, 0, 3)
	testt.NoError(t, err)
	testt.MustEqual(t, string(val), "This")

	val, err = s.GETRANGE(mykey, -3, -1)
	testt.NoError(t, err)
	testt.MustEqual(t, string(val), "ing")

	val, err = s.GETRANGE(mykey, 0, -1)
	testt.NoError(t, err)
	testt.MustEqual(t, string(val), "This is a string")

	val, err = s.GETRANGE(mykey, 10, 100)
	testt.NoError(t, err)
	testt.MustEqual(t, string(val), "string")
}

func TestGETSET(t *testing.T) {
	/*
		redis> SET mykey "Hello"
		"OK"
		redis> GETSET mykey "World"
		"Hello"
		redis> GET mykey
		"World"
		redis>
	*/

	mykey := []byte("mykey")

	s := newStore(t)
	err := s.SET(mykey, []byte("Hello"))
	testt.NoError(t, err)

	val, err := s.GETSET(mykey, []byte("World"))
	testt.NoError(t, err)
	testt.MustEqual(t, string(val), "Hello")

	val, err = s.GET(mykey)
	testt.NoError(t, err)
	testt.MustEqual(t, string(val), "World")
}

func TestINCR(t *testing.T) {
	/*
		redis> SET mykey "10"
		"OK"
		redis> INCR mykey
		(integer) 11
		redis> GET mykey
		"11"
		redis>
	*/

	mykey := []byte("mykey")

	s := newStore(t)
	err := s.SET(mykey, []byte("10"))
	testt.NoError(t, err)

	val, err := s.INCR(mykey)
	testt.NoError(t, err)
	testt.MustEqual(t, val, int64(11))

	val2, err := s.GET(mykey)
	testt.NoError(t, err)
	testt.MustEqual(t, string(val2), "11")
}

func TestINCRBY(t *testing.T) {
	/*
		redis> SET mykey "10"
		"OK"
		redis> INCRBY mykey 5
		(integer) 15
		redis>
	*/

	mykey := []byte("mykey")

	s := newStore(t)
	err := s.SET(mykey, []byte("10"))
	testt.NoError(t, err)

	val, err := s.INCRBY(mykey, 5)
	testt.NoError(t, err)
	testt.MustEqual(t, val, int64(15))
}

func TestINCRBYFLOAT(t *testing.T) {
	/*
		redis> SET mykey 10.50
		"OK"
		redis> INCRBYFLOAT mykey 0.1
		"10.6"
		redis> INCRBYFLOAT mykey -5
		"5.6"
		redis> SET mykey 5.0e3
		"OK"
		redis> INCRBYFLOAT mykey 2.0e2
		"5200"
		redis>
	*/

	mykey := []byte("mykey")

	s := newStore(t)
	err := s.SET(mykey, []byte("10.50"))
	testt.NoError(t, err)

	val, err := s.INCRBYFLOAT(mykey, 0.1)
	testt.NoError(t, err)
	testt.MustEqual(t, string(val), "10.6")

	val, err = s.INCRBYFLOAT(mykey, -5)
	testt.NoError(t, err)
	testt.MustEqual(t, string(val), "5.6")

	err = s.SET(mykey, []byte("5.0e3"))
	testt.NoError(t, err)

	val, err = s.INCRBYFLOAT(mykey, 2.0e2)
	testt.NoError(t, err)
	testt.MustEqual(t, string(val), "5200")
}

func TestMGET(t *testing.T) {
	/*
		redis> SET key1 "Hello"
		"OK"
		redis> SET key2 "World"
		"OK"
		redis> MGET key1 key2 nonexisting
		1) "Hello"
		2) "World"
		3) (nil)
		redis>
	*/

	key1 := []byte("key1")
	key2 := []byte("key2")

	s := newStore(t)
	err := s.SET(key1, []byte("Hello"))
	testt.NoError(t, err)

	err = s.SET(key2, []byte("World"))
	testt.NoError(t, err)

	vals, err := s.MGET(key1, key2, []byte("nonexisting"))
	testt.NoError(t, err)
	testt.MustEqual(t, len(vals), 3)
	testt.MustEqual(t, string(vals[0]), "Hello")
	testt.MustEqual(t, string(vals[1]), "World")
	testt.MustEqual(t, vals[2], []byte(nil))
}

func TestMSET(t *testing.T) {
	/*
		redis> MSET key1 "Hello" key2 "World"
		"OK"
		redis> GET key1
		"Hello"
		redis> GET key2
		"World"
		redis>
	*/

	key1 := []byte("key1")
	key2 := []byte("key2")

	s := newStore(t)
	err := s.MSET(key1, []byte("Hello"), key2, []byte("World"))
	testt.NoError(t, err)

	val, err := s.GET(key1)
	testt.NoError(t, err)
	testt.MustEqual(t, string(val), "Hello")

	val, err = s.GET(key2)
	testt.NoError(t, err)
	testt.MustEqual(t, string(val), "World")
}

func TestSET(t *testing.T) {
	/*
		redis> SET mykey "Hello"
		"OK"
		redis> GET mykey
		"Hello"
	*/

	mykey := []byte("mykey")

	s := newStore(t)
	err := s.SET(mykey, []byte("Hello"))
	testt.NoError(t, err)

	val, err := s.GET(mykey)
	testt.NoError(t, err)
	testt.MustEqual(t, string(val), "Hello")
}

func TestSTRLEN(t *testing.T) {
	/*
		redis> SET mykey "Hello world"
		"OK"
		redis> STRLEN mykey
		(integer) 11
		redis> STRLEN nonexisting
		(integer) 0
		redis>
	*/

	mykey := []byte("mykey")

	s := newStore(t)
	err := s.SET(mykey, []byte("Hello world"))
	testt.NoError(t, err)

	size, err := s.STRLEN(mykey)
	testt.NoError(t, err)
	testt.MustEqual(t, size, int64(11))

	size, err = s.STRLEN([]byte("nonexisting"))
	testt.NoError(t, err)
	testt.MustEqual(t, size, int64(0))
}

func newStore(tb testing.TB) *Store {
	tb.Helper()

	dir := tb.TempDir()

	s, err := Open(Config{Dir: dir})
	if err != nil {
		tb.Fatal(err)
	}
	return s
}
