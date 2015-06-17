package ObjectPool

import (
	"io"
	"sync"
	"sync/atomic"
	"net"
	"testing"
    "time"
    "fmt"
    "log"
    "reflect"
    "math/rand"
)

var (
	uint_1024   = uint32(1024)
	uint_2048   = uint32(2048)
	uint_512    = uint32(512)
	idle_300s   time.Duration
	idle_2s     time.Duration
	idle_50ms   time.Duration
	server_addr = "127.0.0.1:10999"
	server_addr_down = "127.0.0.1:11000"
	server_timeout time.Duration
	server_active_conn_count = int32(0)
	network = "tcp"

	concurrency_get_fail = int32(0)
	concurrency_write_fail = int32(0)
	concurrency_read_fail = int32(0)
	concurrency_other_fail = int32(0)
)


func init() {
	idle_300s, _ = time.ParseDuration("300s")
	idle_2s, _ = time.ParseDuration("2s")
	idle_50ms, _ = time.ParseDuration("50ms")
	server_timeout, _ = time.ParseDuration("2s")

	// start simple echo server
	echo_server_start_succ := make(chan bool, 1)
	go echo_server(echo_server_start_succ)

	_, more := <- echo_server_start_succ
	if !more {
		log.Fatal("start server failed")
	}
}

func echo_server(start_succ_signal chan<- bool) {

	l, err := net.Listen(network, server_addr)
	if err != nil {
		log.Fatal("err")
	}
	defer l.Close()
	start_succ_signal <- true

	int32_positive_one := int32(1)
	int32_negtive_one := int32(-1)

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Fatalf("accept fail:%s", err)
		}

		go func() {
			atomic.AddInt32(&server_active_conn_count, int32_positive_one)
			defer atomic.AddInt32(&server_active_conn_count, int32_negtive_one)
			io.Copy(conn, conn)
		}()
	}

}


func conn_constructor() (interface{}, error) {
	return net.DialTimeout(network, server_addr, idle_2s)
}

func conn_down_constructor() (interface{}, error) {
	return net.DialTimeout(network, server_addr_down, idle_2s)
}

func conn_id_extractor(object interface{}) string {
	conn := object.(net.Conn)
	return fmt.Sprintf("%s_%s", conn.RemoteAddr(), conn.LocalAddr())
}

func conn_destructor(object interface{}) {
	conn := object.(net.Conn)
	conn.Close()
}

func TestNew_MinLowerToMax(t *testing.T) {
	_, err := NewObjectPool(uint_1024, uint_512, idle_2s, 
					conn_constructor, conn_destructor, conn_id_extractor)
	if err == nil {
		t.Fatalf("New() should checking min is lower to max")
	}
}

func TestNew_ConsNil(t *testing.T) {
	_, err := NewObjectPool(uint_512, uint_1024, idle_300s,
					nil, conn_destructor, conn_id_extractor)
	if err == nil {
		t.Fatalf("New() should checking constructor is not nil")
	}
}

func TestNew_DeconsNil(t *testing.T) {
    _, err := NewObjectPool(uint_512, uint_1024, idle_300s,
				conn_constructor, nil, conn_id_extractor)
	if err == nil {
		t.Fatalf("New() should checking destructor is not nil")
	}
}

func TestNew_IdExtractorNil(t *testing.T) {
	_,  err := NewObjectPool(uint_512, uint_1024, idle_300s,
				conn_constructor, conn_destructor, nil)
	if err == nil {
		t.Fatalf("New() should checking idExtractor is not nil")
	}
}

func TestNew_OK(t *testing.T) {
	pool, err := NewObjectPool(uint_512, uint_1024, idle_300s,
					conn_constructor, conn_destructor, conn_id_extractor)
	if err != nil {
		t.Fatalf("New() create object failed, err:%v", err)
	}
    if pool == nil {
        t.Fatal("Invalid return")
    }

	pool.Close()
}

func TestGet_OK(t *testing.T) {
	pool, err := NewObjectPool(uint_512, uint_1024, idle_300s,
					conn_constructor, conn_destructor, conn_id_extractor)
	if err != nil {
		t.Fatalf("New() create object failed, err:%v", err)
	}
	defer pool.Close()

	object_holder, err := pool.GetObject()
	if err != nil {
		t.Fatalf("GetObject() failed, err:%s", err)
	}

	object := object_holder.ExtractObject()
	_, succ := object.(net.Conn)
	if !succ {
		object_type_info := reflect.TypeOf(object)
		t.Fatalf("ExtractObject() failed, object_type:%s, object:%s, need:net.Conn",
					object_type_info, object)
	}

	if server_active_conn_count != 1 {
		t.Logf("active-connection expect:%d, get:%d", 1, server_active_conn_count)
	}
	t.Logf("INFO active-connection expect:%d, get:%d", 1, server_active_conn_count)
    sleep_time, _ := time.ParseDuration("1s")
    time.Sleep(sleep_time)
	t.Logf("INFO active-connection expect:%d, get:%d", 1, server_active_conn_count)
	if server_active_conn_count != 1 {
		t.Logf("active-connection expect:%d, get:%d", 1, server_active_conn_count)
	}

	if pool.GetObjectCount() != 1 {
		t.Errorf("GetObjectCount() expect:%d, get:%d", 1, pool.GetObjectCount())
	}

	pool.ReturnObject(object_holder)
}


func TestGet_FAIL(t *testing.T) {
	pool, err := NewObjectPool(uint_512, uint_1024, idle_300s,
					conn_down_constructor, conn_destructor, conn_id_extractor)
	if err != nil {
		t.Fatalf("New() create object failed, err:%v", err)
	}
	defer pool.Close()

	object_holder, err := pool.GetObject()
	if err == nil {
		t.Fatalf("GetObject() should failed, object:%v", object_holder)
	}
	fatal_is_not_nil(t, object_holder)

	if server_active_conn_count != 0 {
		t.Errorf("active-connection expect:%d, get:%d", 1, server_active_conn_count)
	}

	if pool.GetObjectCount() != 0 {
		t.Errorf("GetObjectCount() expect:%d, get:%d", 0, pool.GetObjectCount())
	}
}

func TestReadWrite_OK(t *testing.T) {
	pool, err := NewObjectPool(uint_512, uint_1024, idle_300s,
					conn_constructor, conn_destructor, conn_id_extractor)
	if err != nil {
		t.Fatalf("New() create object failed, err:%v", err)
	}
	defer pool.Close()

	object_holder, err := pool.GetObject()
	if err != nil {
		t.Fatalf("GetObject() failed, err:%s", err)
	}

	conn := object_holder.ExtractObject().(net.Conn)
	data_write := "test from client"
	err = conn.SetWriteDeadline(time.Now().Add(idle_50ms))
	fatal_error(t, err)
	write_len, err := io.WriteString(conn, data_write)
	if err != nil || write_len != len(data_write) {
		t.Fatalf("write data failed, expect:%d, ret:%d, err:%s", len(data_write), write_len, err)
	}

	data_read := make([]byte, len(data_write))
	err = conn.SetReadDeadline(time.Now().Add(idle_50ms))
	fatal_error(t, err)
	read_len, err := io.ReadFull(conn, data_read)
	if err != nil || read_len != len(data_read) {
		t.Fatalf("read data failed, expect:%d, ret:%d, err:%s", len(data_read), read_len, err)
	}

	if data_write != string(data_read) {
		t.Fatalf("invalid retur data, expect:%s, ret:%s", data_write, string(data_read))
	}

	pool.ReturnObject(object_holder)
}

func TestReturn_OK(t *testing.T) {
	pool, err := NewObjectPool(uint_512, uint_1024, idle_300s,
					conn_constructor, conn_destructor, conn_id_extractor)
	if err != nil {
		t.Fatalf("New() create object failed, err:%v", err)
	}
	defer pool.Close()

	object_holder, err := pool.GetObject()
	if err != nil {
		t.Fatalf("GetObject() failed, err:%s", err)
	}

	err = pool.ReturnObject(object_holder)
	if err != nil {
		t.Fatalf("Return Object failed, err:%v", err)
	}

	if pool.GetIdleObjectCount() != 1 {
		t.Fatalf("Return Object Failed, expect:%d, get:%d", 1, pool.GetIdleObjectCount())
	}
}

func TestReturn_ReturnUnusable(t *testing.T) {
	pool, err := NewObjectPool(uint_512, uint_1024, idle_300s,
					conn_constructor, conn_destructor, conn_id_extractor)
	if err != nil {
		t.Fatalf("New() create object failed, err:%v", err)
	}
	defer pool.Close()

	object_holder, err := pool.GetObject()
	if err != nil {
		t.Fatalf("GetObject() failed, err:%s", err)
	}

	object_holder.MarkUnusable()
	err = pool.ReturnObject(object_holder)
	if err != nil {
		t.Fatalf("Return Object failed, err:%v", err)
	}

	if pool.GetIdleObjectCount() != 0 {
		t.Fatalf("Return Object Failed, expect:%d, get:%d", 0, pool.GetIdleObjectCount())
	}
}

func TestReturn_NotBelongToPool(t *testing.T) {
	pool, err := NewObjectPool(uint_512, uint_1024, idle_300s,
					conn_constructor, conn_destructor, conn_id_extractor)
	if err != nil {
		t.Fatalf("New() create object failed, err:%v", err)
	}
	defer pool.Close()

	object_holder := &objectHolder {
		object: nil,
		createTime: time.Now(),
		lastUseTime: time.Now(),
		useCount: 0,
		usable: true,
	}

	err = pool.ReturnObject(object_holder)
	if err == nil {
		t.Fatalf("Cannot return object that does not belong to this pool")
	}

	if pool.GetObjectCount() != 0 {
		t.Fatalf("ObjectCount should be ZERO, get:%d", pool.GetObjectCount())
	}
}

func TestUseCount(t *testing.T) {
	pool, err := NewObjectPool(uint_512, uint_1024, idle_300s,
					conn_constructor, conn_destructor, conn_id_extractor)
	if err != nil {
		t.Fatalf("New() create object failed, err:%v", err)
	}
	defer pool.Close()

	object_holder, err := pool.GetObject()
	if err != nil {
        t.Fatalf("get object failed:%s", err)
    }
	if object_holder.GetUseCount() != 1 {
		t.Fatalf("Invalid UseCount, expect:%d, get:%d", 1, object_holder.GetUseCount())
	}

	err = pool.ReturnObject(object_holder)
	if err != nil {
        t.Fatalf("get object failed:%s", err)
    }
	object_holder, err = pool.GetObject()
	if err != nil {
        t.Fatalf("get object failed:%s", err)
    }
	if object_holder.GetUseCount() != 2 {
		t.Fatalf("Invalid UseCount, expect:%d, get:%d", 2, object_holder.GetUseCount())
	}

	err = pool.ReturnObject(object_holder)
	if err != nil {
        t.Fatalf("get object failed:%s", err)
    }
	object_holder, err = pool.GetObject()
	if err != nil {
        t.Fatalf("get object failed:%s", err)
    }
	if object_holder.GetUseCount() != 3 {
		t.Fatalf("Invalid UseCount, expect:%d, get:%d", 3, object_holder.GetUseCount())
	}

	err = pool.ReturnObject(object_holder)
	if err != nil {
        t.Fatalf("get object failed:%s", err)
    }
}


// Start multi goroutines, every routine first GetObject() & Write & Read & ReturnObject()
// then sleep some time rand between [0ms, 10ms] and repeat the operation before.
func TestCurrency(t *testing.T) {
	pool, err := NewObjectPool(uint_512, uint_1024, idle_300s,
					conn_constructor, conn_destructor, conn_id_extractor)
	if err != nil {
		t.Fatalf("New() create object failed, err:%v", err)
	}


	rand.Seed(time.Now().UnixNano())

	var wg sync.WaitGroup
	concurrency_count := 300
	for idx := 0; idx < concurrency_count; idx += 1 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			object, err := pool.GetObject()
			if err != nil {
				atomic.AddInt32(&concurrency_get_fail, 1)
				t.Errorf("IDX:%d get failed, err:%s", idx, err)
				return
			}
			conn := object.ExtractObject().(net.Conn)
			
			rw_count := rand.Int31n(10) + 10
			for i := int32(0); i < rw_count; i += 1 {
				data_write := fmt.Sprintf("hello from client:%d, loop:%d", idx, i)
				data_read_byte := make([]byte, len(data_write))

				conn.SetDeadline(time.Now().Add(idle_2s))
				write_len, err := io.WriteString(conn, data_write)
				if err != nil || write_len != len(data_write) {
					atomic.AddInt32(&concurrency_write_fail, 1)
					object.MarkUnusable()
					t.Errorf("IDX:%d LOOP:%d, write data failed, write_len:%d, expect:%d， err:%s", 
							idx, i, write_len, len(data_write), err)
					break
				}

				read_len, err := io.ReadFull(conn, data_read_byte)
				if err != nil || read_len != len(data_read_byte) {
					atomic.AddInt32(&concurrency_read_fail, 1)
					object.MarkUnusable()
					t.Errorf("IDX:%d LOOP:%d, read data failed, read_len:%d, expect:%d, err:%s",
							idx, i, read_len, len(data_read_byte), err)
					break
				}

				if string(data_read_byte) != data_write {
					atomic.AddInt32(&concurrency_read_fail, 1)
					object.MarkUnusable()
					t.Errorf("IDX:%d LOOP:%d, parse read failed, expect:%s, get:%s",
							idx, i, data_write, string(data_read_byte))
					break
				}

				sleep_time, err := time.ParseDuration(fmt.Sprintf("%dms", rand.Int31n(18) + 2))
				if err != nil {
					atomic.AddInt32(&concurrency_other_fail, 1)
					t.Errorf("IDX:%d LOOP:%d, parse duration failed, err:%s",
								idx, i, err)
					break
				}
                time.Sleep(sleep_time)
			}

			conn = nil
			err = pool.ReturnObject(object)
            if err != nil {
                t.Fatalf("return object failed, IDX:%d, err:%s", idx, err)
            }


			rw_count = rand.Int31n(10) + 10
			for i := int32(0); i < rw_count; i += 1 {
				data_write := fmt.Sprintf("hello from client:%d, loop:%d", idx, i)
				data_read_byte := make([]byte, len(data_write))

				object, err := pool.GetObject()
				if err != nil {
					atomic.AddInt32(&concurrency_get_fail, 1)
					t.Errorf("IDX:%d get failed, err:%s", idx, err)
					return
				}
				conn := object.ExtractObject().(net.Conn)

				//conn.SetDeadline(time.Now().Add(idle_50ms))
				conn.SetDeadline(time.Now().Add(idle_2s))
				write_len, err := io.WriteString(conn, data_write)
				if err != nil || write_len != len(data_write) {
					atomic.AddInt32(&concurrency_write_fail, 1)
					object.MarkUnusable()
					t.Errorf("IDX:%d LOOP:%d, write data failed, write_len:%d, expect:%d， err:%s", 
							idx, i, write_len, len(data_write), err)
					break
				}

				read_len, err := io.ReadFull(conn, data_read_byte)
				if err != nil || read_len != len(data_read_byte) {
					atomic.AddInt32(&concurrency_read_fail, 1)
					object.MarkUnusable()
					t.Errorf("IDX:%d LOOP:%d, read data failed, read_len:%d, expect:%d, err:%s",
							idx, i, read_len, len(data_read_byte), err)
					break
				}

				if string(data_read_byte) != data_write {
					atomic.AddInt32(&concurrency_read_fail, 1)
					object.MarkUnusable()
					t.Errorf("IDX:%d LOOP:%d, parse read failed, expect:%s, get:%s",
							idx, i, data_write, string(data_read_byte))
					break
				}

				usable := rand.Int31n(2)
				if usable == 0 {
					object.MarkUnusable()
				}

				pool.ReturnObject(object)

				sleep_time, err := time.ParseDuration(fmt.Sprintf("%dms", rand.Int31n(18) + 2))
				if err != nil {
					atomic.AddInt32(&concurrency_other_fail, 1)
					t.Errorf("IDX:%d LOOP:%d, parse duration failed, err:%s",
								idx, i, err)
					break
				}
                time.Sleep(sleep_time)
			}
		} ()
	}

	wg.Wait()

	if pool.GetObjectCount() != pool.GetIdleObjectCount() {
		t.Errorf("ObjectCount:%d != IdleObjectCount:%d", pool.GetObjectCount(), pool.GetIdleObjectCount())
	}

	if concurrency_get_fail != 0 || 
        concurrency_read_fail != 0 || 
        concurrency_write_fail != 0 || concurrency_other_fail != 0 {
		t.Errorf("concurrency_get_fail:%d, concurrency_read_fail:%d, concurrency_write_fail:%d, concurrency_other_fail:%d", 
			concurrency_get_fail, concurrency_read_fail, concurrency_write_fail, concurrency_other_fail)
	}

	close_start_time := time.Now().UnixNano()
	object_count := pool.GetObjectCount()
	pool.Close()
	t.Logf("ObjectCount:%d, TimeUsage:%d", object_count, (time.Now().UnixNano() - close_start_time) / int64(time.Millisecond))
}


func TestItemIdle(t *testing.T) {
	pool, err := NewObjectPool(uint_512, uint_1024, idle_300s,
					conn_constructor, conn_destructor, conn_id_extractor)
	if err != nil {
		t.Fatalf("New() create object failed, err:%v", err)
	}

    pool.Close()
}


func TestIsClosed_OK(t *testing.T) {
	pool, err := NewObjectPool(uint_512, uint_1024, idle_300s,
					conn_constructor, conn_destructor, conn_id_extractor)
	if err != nil {
		t.Fatalf("create pool failed")
	}

	if pool.IsClosed() {
		t.Fatalf("pool should be OPEN")
	}

	pool.Close()
	if !pool.IsClosed() {
		t.Fatalf("pool should be CLOSED")
	}	
}

// Test GetObjectCount() & GetIdleObjectCount()
func TestGetObjectCount(t *testing.T) {
	pool := new_pool(t)
	defer pool.Close()

	object_active_count := 10
	object_idle_count := 10
	object_active := make([]*objectHolder, 10)
    object_idle := make([]*objectHolder, 10)
	for idx := 0; idx < object_active_count; idx += 1 {
		object_active[idx] = get_object_and_check(t, pool)
	}

	for idx := 0; idx < object_idle_count; idx += 1 {
		object_idle[idx] = get_object_and_check(t, pool)

	}
    for _, object_holder := range object_idle {
		err := pool.ReturnObject(object_holder)
        if err != nil {
            t.Fatalf("return object failed, err:%s", err)
        }
    }

	// check
	if int(pool.GetIdleObjectCount()) != object_idle_count {
		t.Fatalf("idle object count invalid, expect:%d, get:%d", object_idle_count, pool.GetIdleObjectCount())
	}

	if int(pool.GetObjectCount()) != object_idle_count + object_active_count {
		t.Fatalf("total object count invalid, expect:%d, get:%d", object_idle_count + object_active_count, pool.GetObjectCount())
	}

    sleep_time, _ := time.ParseDuration("100ms")
    time.Sleep(sleep_time)
	if int(server_active_conn_count) != object_idle_count + object_active_count {
		t.Fatalf("server_active_conn_count:%d, object_idle_count:%d, object_active_count:%d",
			server_active_conn_count, object_idle_count, object_active_count)
	}

	// clean up
	for _, object_holder := range object_active {
		pool.ReturnObject(object_holder)
	}

	if int(pool.GetIdleObjectCount()) != object_idle_count + object_active_count {
		t.Fatalf("idle object count invalid, expect:%d, get:%d", object_idle_count + object_active_count, pool.GetIdleObjectCount())
	}
}

// Test GetMaxObjectCount() & GetMinObjectCount() & GetIdleTime()
func TestGetMaxObjectCount(t *testing.T) {
	pool, err := NewObjectPool(uint_512, uint_1024, idle_300s,
					conn_constructor, conn_destructor, conn_id_extractor)
	if err != nil {
		t.Fatalf("create object failed, err:%s", err)
	}
	defer pool.Close()

	if pool.GetMaxObjectCount() != uint_1024 {
		t.Errorf("GetMaxObjectCount() return invalid value, expect:%d, get:%d", uint_1024, pool.GetMaxObjectCount())
	}

	if pool.GetMinObjectCount() != uint_512 {
		t.Errorf("GetMinObjectCount() return invalid value, expect:%d, get:%d", uint_512, pool.GetMinObjectCount())
	}

	if pool.GetIdleTime().Nanoseconds() != idle_300s.Nanoseconds() {
		t.Errorf("GetIdleTime() return invalid value, expect:%s, get:%s", idle_300s, pool.GetIdleTime())
	}
}

func fatal_is_not_nil(t *testing.T, object interface{}) {
	if object == nil {
		t.Fatalf("assert_is_not_nil() failed")
	}
}

func fatal_error(t *testing.T, err error) {
	if err != nil {
		t.Fatal("err:%s", err)
	}
}

func new_pool(t *testing.T) *objectPool{
	pool, err := NewObjectPool(uint_512, uint_1024, idle_300s,
					conn_constructor, conn_destructor, conn_id_extractor)
	if err != nil {
		t.Fatalf("create pool failed, err:%s", err)
	}

	return pool
}

func get_object_and_check(t *testing.T, pool *objectPool) *objectHolder {
	object_holder, err := pool.GetObject()
	if err != nil {
		t.Fatalf("GetObject() failed")
	}
	return object_holder
}
