package server

import (
	"context"
	"testing"
	"time"

	"github.com/cristaloleg/didis/internal/core"
	"github.com/cristaloleg/didis/internal/inmem"

	"github.com/cristalhq/testt"
	"github.com/redis/go-redis/v9"
)

func TestAPPEND(t *testing.T) {
	/*
		redis> EXISTS "mykey"
		(integer) 0
		redis> APPEND "mykey" "Hello"
		(integer) 5
		redis> APPEND "mykey" " World"
		(integer) 11
		redis> GET "mykey"
		"Hello World"
		redis>
	*/

	ctx := context.Background()
	addr := testServer(t)
	client := testClient(t, addr)

	size, err := client.Append(ctx, "mykey", "Hello").Result()
	testt.NoError(t, err)
	testt.MustEqual(t, size, int64(5))

	size, err = client.Append(ctx, "mykey", " World").Result()
	testt.NoError(t, err)
	testt.MustEqual(t, size, int64(11))

	val, err := client.Get(ctx, "mykey").Result()
	testt.NoError(t, err)
	testt.MustEqual(t, string(val), "Hello World")
}

func TestDECR(t *testing.T) {
	/*
		redis> SET "mykey" "10"
		"OK"
		redis> DECR "mykey"
		(integer) 9
		redis> SET "mykey" "234293482390480948029348230948"
		"OK"
		redis> DECR "mykey"
		(error) value is not an integer or out of range
		redis>
	*/

	ctx := context.Background()
	addr := testServer(t)
	client := testClient(t, addr)

	err := client.Set(ctx, "mykey", []byte("10"), 0).Err()
	testt.NoError(t, err)

	val, err := client.Decr(ctx, "mykey").Result()
	testt.NoError(t, err)
	testt.MustEqual(t, val, int64(9))

	err = client.Set(ctx, "mykey", "234293482390480948029348230948", 0).Err()
	testt.NoError(t, err)

	_, err = client.Decr(ctx, "mykey").Result()
	testt.WantError(t, err)
	testt.MustEqual(t, err.Error(), core.ErrNotIntOrOutOfRange.Error())
}

func TestDECRBY(t *testing.T) {
	/*
		redis> SET "mykey" "10"
		"OK"
		redis> DECRBY "mykey" 3
		(integer) 7
		redis>
	*/

	ctx := context.Background()
	addr := testServer(t)
	client := testClient(t, addr)

	err := client.Set(ctx, "mykey", []byte("10"), 0).Err()
	testt.NoError(t, err)

	val, err := client.DecrBy(ctx, "mykey", 3).Result()
	testt.NoError(t, err)
	testt.MustEqual(t, val, int64(7))
}

func TestGET(t *testing.T) {
	/*
		redis> GET nonexisting
		(nil)
		redis> SET "mykey" "Hello"
		"OK"
		redis> GET "mykey"
		"Hello"
		redis>
	*/

	ctx := context.Background()
	addr := testServer(t)
	client := testClient(t, addr)

	val, err := client.Get(ctx, "nonexisting").Result()
	testt.WantError(t, err)
	testt.MustEqual(t, err.Error(), "key not found")

	err = client.Set(ctx, "mykey", "Hello", 0).Err()
	testt.NoError(t, err)

	val, err = client.Get(ctx, "mykey").Result()
	testt.NoError(t, err)
	testt.MustEqual(t, string(val), "Hello")
}

func TestGETDEL(t *testing.T) {
	/*
		redis> SET "mykey" "Hello"
		"OK"
		redis> GETDEL "mykey"
		"Hello"
		redis> GET "mykey"
		(nil)
		redis>
	*/

	ctx := context.Background()
	addr := testServer(t)
	client := testClient(t, addr)

	err := client.Set(ctx, "mykey", []byte("Hello"), 0).Err()
	testt.NoError(t, err)

	val, err := client.GetDel(ctx, "mykey").Result()
	testt.NoError(t, err)
	testt.MustEqual(t, string(val), "Hello")

	val, err = client.Get(ctx, "mykey").Result()
	testt.WantError(t, err)
	testt.MustEqual(t, err.Error(), "key not found")
}

func TestGETRANGE(t *testing.T) {
	/*
		redis> SET "mykey" "This is a string"
		"OK"
		redis> GETRANGE "mykey" 0 3
		"This"
		redis> GETRANGE "mykey" -3 -1
		"ing"
		redis> GETRANGE "mykey" 0 -1
		"This is a string"
		redis> GETRANGE "mykey" 10 100
		"string"
		redis>
	*/

	ctx := context.Background()
	addr := testServer(t)
	client := testClient(t, addr)

	err := client.Set(ctx, "mykey", []byte("This is a string"), 0).Err()
	testt.NoError(t, err)

	val, err := client.GetRange(ctx, "mykey", 0, 3).Result()
	testt.NoError(t, err)
	testt.MustEqual(t, string(val), "This")

	val, err = client.GetRange(ctx, "mykey", -3, -1).Result()
	testt.NoError(t, err)
	testt.MustEqual(t, string(val), "ing")

	val, err = client.GetRange(ctx, "mykey", 0, -1).Result()
	testt.NoError(t, err)
	testt.MustEqual(t, string(val), "This is a string")

	val, err = client.GetRange(ctx, "mykey", 10, 100).Result()
	testt.NoError(t, err)
	testt.MustEqual(t, string(val), "string")
}

func TestGETSET(t *testing.T) {
	/*
		redis> SET "mykey" "Hello"
		"OK"
		redis> GETSET "mykey" "World"
		"Hello"
		redis> GET "mykey"
		"World"
		redis>
	*/

	ctx := context.Background()
	addr := testServer(t)
	client := testClient(t, addr)

	err := client.Set(ctx, "mykey", "Hello", 0).Err()
	testt.NoError(t, err)

	val, err := client.GetSet(ctx, "mykey", "World").Result()
	testt.NoError(t, err)
	testt.MustEqual(t, string(val), "Hello")

	val, err = client.Get(ctx, "mykey").Result()
	testt.NoError(t, err)
	testt.MustEqual(t, string(val), "World")
}

func TestINCR(t *testing.T) {
	/*
		redis> SET "mykey" "10"
		"OK"
		redis> INCR "mykey"
		(integer) 11
		redis> GET "mykey"
		"11"
		redis>
	*/

	ctx := context.Background()
	addr := testServer(t)
	client := testClient(t, addr)

	err := client.Set(ctx, "mykey", []byte("10"), 0).Err()
	testt.NoError(t, err)

	val, err := client.Incr(ctx, "mykey").Result()
	testt.NoError(t, err)
	testt.MustEqual(t, val, int64(11))

	val2, err := client.Get(ctx, "mykey").Result()
	testt.NoError(t, err)
	testt.MustEqual(t, string(val2), "11")
}

func TestINCRBY(t *testing.T) {
	/*
		redis> SET "mykey" "10"
		"OK"
		redis> INCRBY "mykey" 5
		(integer) 15
		redis>
	*/

	ctx := context.Background()
	addr := testServer(t)
	client := testClient(t, addr)

	err := client.Set(ctx, "mykey", []byte("10"), 0).Err()
	testt.NoError(t, err)

	val, err := client.IncrBy(ctx, "mykey", 5).Result()
	testt.NoError(t, err)
	testt.MustEqual(t, val, int64(15))
}

func TestINCRBYFLOAT(t *testing.T) {
	/*
		redis> SET "mykey" 10.50
		"OK"
		redis> INCRBYFLOAT "mykey" 0.1
		"10.6"
		redis> INCRBYFLOAT "mykey" -5
		"5.6"
		redis> SET "mykey" 5.0e3
		"OK"
		redis> INCRBYFLOAT "mykey" 2.0e2
		"5200"
		redis>
	*/

	ctx := context.Background()
	addr := testServer(t)
	client := testClient(t, addr)

	err := client.Set(ctx, "mykey", []byte("10.50"), 0).Err()
	testt.NoError(t, err)

	val, err := client.IncrByFloat(ctx, "mykey", 0.1).Result()
	testt.NoError(t, err)
	testt.MustEqual(t, val, 10.6)

	val, err = client.IncrByFloat(ctx, "mykey", -5).Result()
	testt.NoError(t, err)
	testt.MustEqual(t, val, 5.6)

	err = client.Set(ctx, "mykey", []byte("5.0e3"), 0).Err()
	testt.NoError(t, err)

	val, err = client.IncrByFloat(ctx, "mykey", 2.0e2).Result()
	testt.NoError(t, err)
	testt.MustEqual(t, val, 5200.0)
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

	ctx := context.Background()

	addr := testServer(t)
	client := testClient(t, addr)

	err := client.Set(ctx, "key1", []byte("Hello"), 0).Err()
	testt.NoError(t, err)

	err = client.Set(ctx, "key2", []byte("World"), 0).Err()
	testt.NoError(t, err)

	vals, err := client.MGet(ctx, "key1", "key2", "nonexisting").Result()
	testt.NoError(t, err)
	testt.MustEqual(t, len(vals), 3)
	testt.MustEqual(t, vals[0].(string), "Hello")
	testt.MustEqual(t, vals[1].(string), "World")
	testt.MustEqual(t, vals[2].(string), "")
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

	ctx := context.Background()
	addr := testServer(t)
	client := testClient(t, addr)

	err := client.MSet(ctx, "key1", "Hello", "key2", "World").Err()
	testt.NoError(t, err)

	val, err := client.Get(ctx, "key1").Result()
	testt.NoError(t, err)
	testt.MustEqual(t, string(val), "Hello")

	val, err = client.Get(ctx, "key2").Result()
	testt.NoError(t, err)
	testt.MustEqual(t, string(val), "World")
}

func TestSET(t *testing.T) {
	/*
		redis> SET "mykey" "Hello"
		"OK"
		redis> GET "mykey"
		"Hello"
	*/

	ctx := context.Background()
	addr := testServer(t)
	client := testClient(t, addr)

	err := client.Set(ctx, "mykey", []byte("Hello"), 0).Err()
	testt.NoError(t, err)

	val, err := client.Get(ctx, "mykey").Result()
	testt.NoError(t, err)
	testt.MustEqual(t, string(val), "Hello")
}

func TestSTRLEN(t *testing.T) {
	/*
		redis> SET "mykey" "Hello world"
		"OK"
		redis> STRLEN "mykey"
		(integer) 11
		redis> STRLEN nonexisting
		(integer) 0
		redis>
	*/

	ctx := context.Background()
	addr := testServer(t)
	client := testClient(t, addr)

	err := client.Set(ctx, "mykey", []byte("Hello world"), 0).Err()
	testt.NoError(t, err)

	size, err := client.StrLen(ctx, "mykey").Result()
	testt.NoError(t, err)
	testt.MustEqual(t, size, int64(11))

	size, err = client.StrLen(ctx, "nonexisting").Result()
	testt.NoError(t, err)
	testt.MustEqual(t, size, int64(0))
}

func testServer(tb testing.TB) string {
	tb.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	tb.Cleanup(cancel)

	srv, err := New(Config{
		Addr:  "localhost:0",
		Store: inmem.New(),
	})
	testt.NoError(tb, err)

	go func() {
		srv.Run(ctx)
	}()

	// wait a bit for server to start in a goroutine.
	time.Sleep(100 * time.Millisecond)

	return srv.addr
}

func testClient(tb testing.TB, addr string) *redis.Client {
	tb.Helper()

	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	return client
}
