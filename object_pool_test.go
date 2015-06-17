package ObjectPool

import (
	"io"
	"sync"
	"sync/Atomic"
	"net"
	"testing"
)

var (
	uint_1024 = 1024
	uint_2048 = 2048
	uint_512  = 512
	idle_300s = time.ParseDuration("300s")
	idle_2s = time.ParseDuration("2s")
	idle_50ms = time.ParseDuration("50ms")
	server_addr = "127.0.0.1:10999"
	server_addr_down = "127.0.0.1:11000"
	server_timeout = time.ParseDuration("2s")
	server_active_conn_count = int32(0)
	network = "tcp"
)


func init() {
	// start simple echo server
	echo_server_start_succ := make(chan bool, 1)
	go echo_server(echo_server_start_succ)

	_, more := <- echo_server_start_succ
	if !more {
		log.Fatal("start server failed")
	}
}

func echo_server(start_succ_signal chan<- bool) {
	defer close(echo_server_exit_signal)

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
			log.Fatal(err)
		}

		go func() {
			sync.Atomic.AddInt32(&server_active_conn_count, int32_positive_one)
			defer sync.Atomic.AddInt32(&server_active_conn_count, int32_negtive_one)

			io.Copy(conn, conn)
		}
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
	return conn.RemoteAddr() + conn.LocalAddr()
}

func conn_destructor(object interface{}) {
	conn := object.(net.Conn)
	conn.Close()
}

func TestNew_MinLowerToMax(t *testing.T) {
	pool, err := NewObjectPool(uint_1024, uint_512, idle_300s, 
					conn_constructor, conn_destructor, conn_id_extractor)
	if err == nil {
		t.Fatalf("New() should checking min is lower to max")
	}
	fatal_is_nil(t, pool)
}

func TestNew_ConsNil(t *testing.T) {
	_, err := NewObjectPool(uint_512, uint_1024, idle_300s,
					nil, conn_destructor, conn_id_extractor)
	if err == nil {
		t.Fatalf("New() should checking constructor is not nil")
	}
	fatal_is_nil(t, pool)
}

func TestNew_DeconsNil(t *testing.T) {
	_, err := NewObjectPool(uint_512, uint_1024, idle_300s,
				conn_constructor, nil, conn_id_extractor)
	if err == nil {
		t.Fatalf("New() should checking destructor is not nil")
	}
	fatal_is_nil(t, pool)
}

func TestNew_IdExtractorNil(t *testing.T) {
	_,  err := NewObjectPool(uint512, uint_1024, idle_300s,
				conn_constructor, conn_destructor, nil)
	if err == nil {
		t.Fatalf("New() should checking idExtractor is not nil")
	}
	fatal_is_nil(t, pool)
}

func TestNew_OK(t *testing.T) {
	pool, err := NewObjectPool(uint512, uint_1024, idle_300s,
					conn_constructor, conn_destructor, conn_id_extractor)
	if err != nil {
		t.Fatalf("New() create object failed, err:%v", err)
	}
	fatal_is_not_nil(t, pool)

	pool.Close()
}

func TestGet_OK(t *testing.T) {
	pool, err := NewObjectPool(uint512, uint_1024, idle_300s,
					conn_constructor, conn_destructor, conn_id_extractor)
	if err != nil {
		t.Fatalf("New() create object failed, err:%v", err)
	}
	defer pool.Close()

	object_holder, err := pool.GetOjbect()
	if err != nil {
		assert_is_nil(object_holder)
		t.Fatalf("GetObject() failed, err:%s", err)
	}

	object := object_holder.ExtractObject()
	conn, err = object.(net.Conn)
	if err != nil {
		object_type_info := reflect.TypeOf(object)
		t.Fatalf("ExtractObject() failed, object_type:%s, object:%s, need:net.Conn",
					object_type_info, object)
	}

	if server_active_conn_count != 1 {
		t.Errorf("active-connection expect:%d, get:%d", 1, server_active_conn_count)
	}

	if pool.GetObjectCount() != 1 {
		t.Errorf("GetObjectCount() expect:%d, get:%d", 1, pool.GetObjectCount())
	}

	pool.RetunOjbect(object_holder)
}


func TestGet_FAIL(t *testing.T) {
	pool, err := NewObjectPool(uint512, uint_1024, idle_300s,
					conn_down_constructor, conn_destructor, conn_id_extractor)
	if err != nil {
		t.Fatalf("New() create object failed, err:%v", err)
	}
	defer pool.Close()

	object_holder, err := pool.GetOjbect()
	if err == nil {
		t.Fatalf("GetObject() should failed, object:%v", object)
	}
	assert_is_not_nil(object_holder)

	if server_active_conn_count != 0 {
		t.Errorf("active-connection expect:%d, get:%d", 1, server_active_conn_count)
	}

	if pool.GetObjectCount() != 0 {
		t.Errorf("GetObjectCount() expect:%d, get:%d", 0, pool.GetObjectCount())
	}
}

func TestReadWrite_OK(t *testing.T) {
	pool, err := NewObjectPool(uint512, uint_1024, idle_300s,
					conn_constructor, conn_destructor, conn_id_extractor)
	if err != nil {
		t.Fatalf("New() create object failed, err:%v", err)
	}
	defer pool.Close()

	object_holder, err := pool.GetOjbect()
	if err != nil {
		assert_is_nil(object_holder)
		t.Fatalf("GetObject() failed, err:%s", err)
	}

	data_write := "test from client"
	err := conn.SetWriteDeadline(time.Now().After(idle_50ms))
	fatal_error(err)
	write_len, err := io.WriteString(conn, data_write)
	if err != nil || write_len != len(data_write) {
		t.Fatalf("write data failed, expect:%d, ret:%d, err:%s", len(data_write), write_len, err)
	}

	data_read = make([]byte, len(data_write))
	err := conn.SetReadDeadline(time.Now().After(idle_50ms))
	fatal_error(err)
	read_len, err := io.ReadFull(conn, data_read)
	if err != nil || read_len != len(data_read) {
		f.Fatalf("read data failed, expect:%d, ret:%d, err:%s", len(data_read), read_len, err)
	}

	if data_write != string(data_read) {
		f.Fatalf("invalid retur data, expect:%s, ret:%s", data_write, string(data_read))
	}

	pool.RetunOjbect(object_holder)
}

func TestReturn_OK(t *testing.T) {
	pool, err := NewObjectPool(uint512, uint_1024, idle_300s,
					conn_constructor, conn_destructor, conn_id_extractor)
	if err != nil {
		t.Fatalf("New() create object failed, err:%v", err)
	}
	defer pool.Close()

	object_holder, err := pool.GetOjbect()
	if err != nil {
		assert_is_nil(object_holder)
		t.Fatalf("GetObject() failed, err:%s", err)
	}

	err := pool.RetunOjbect(object_holder)
	if err != nil {
		t.Fatalf("Return Object failed, err:%v", err)
	}

	if pool.GetIdleObjectCount() != 1 {
		t.Fatalf("Return Object Failed, expect:%d, get:%d", 1, pool.GetIdleObjectCount())
	}
}

func TestReturn_ReturnUnusable(t *tesing.T) {
	pool, err := NewObjectPool(uint512, uint_1024, idle_300s,
					conn_constructor, conn_destructor, conn_id_extractor)
	if err != nil {
		t.Fatalf("New() create object failed, err:%v", err)
	}
	defer pool.Close()

	object_holder, err := pool.GetOjbect()
	if err != nil {
		assert_is_nil(object_holder)
		t.Fatalf("GetObject() failed, err:%s", err)
	}

	object_holder.MarkUnusable()
	err := pool.RetunOjbect(object_holder)
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

	object_holder := &objectHOlder {
		object: nil,
		createTime: time.Now(),
		lastUseTime: time.Now(),
		useCount: 0,
		usable: true,
	}

	err := pool.ReturnObject(object_holder)
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
	fatal_is_nil(t, err)
	if object_holder.GetUseCount() != 1 {
		t.Fatalf("Invalid UseCount, expect:%d, get:%d", 1, object_holder.GetUseCount())
	}

	err = pool.ReturnObject(object_holder)
	fatal_is_nil(t, err)
	object_holder, err = pool.GetObject()
	fatal_is_nil(t, err)
	if object_holder.GetUseCount() != 2 {
		t.Fatalf("Invalid UseCount, expect:%d, get:%d", 2, object_holder.GetUseCount())
	}

	err = pool.ReturnObject(object_holder)
	fatal_is_nil(t, err)
	object_holder, err = pool.GetObject()
	fatal_is_nil(t, err)
	if object_holder.GetUseCount() != 3 {
		t.Fatalf("Invalid UseCount, expect:%d, get:%d", 3, object_holder.GetUseCount())
	}

	err = pool.ReturnObject(object_holder)
	fatal_is_nil(t, err)
}


// Start multi goroutines, every routine first GetObject() & Write & Read & ReturnObject()
// then sleep some time rand between [0ms, 10ms] and repeat the operation before.
func TestCurrency(t *testing.T) {
	pool, err := NewObjectPool(uint_512, uint_1024, idle_300s,
					conn_constructor, conn_destructor, conn_id_extractor)
	if err != nil {
		t.Fatalf("New() create object failed, err:%v", err)
	}


	concurrency_count := 1000
	for idx := 0; idx < concurrency_count; ++idx {
		go func(idx int) {


		} (idx)
	}




	
}


func TestItemIdle(t *testing.T) {
	pool, err := NewObjectPool(uint_512, uint_1024, idle_300s,
					conn_constructor, conn_destructor, conn_id_extractor)
	if err != nil {
		t.Fatalf("New() create object failed, err:%v", err)
	}
}


func TestIsClosed_OK(t *testing.T) {
	pool, err := NewObjectPool(uint_512, uint_1024, idle_300s,
					conn_constructor, conn_destructor, conn_id_extractor)
	if err == nil {
		t.Fatalf("New() should checking idExtractor is not nil")
	}

	if pool.IsClosed() {
		t.Fatalf("pool should be OPEN")
	}

	pool.Closed()
	if !pool.IsClosed() {
		t.Fatalf("pool should be CLOSED")
	}	
}

// Test GetObjectCount() & GetIdleObjectCount()
func TestGetObjectCount(t *testing.T) {
	pool := new_pool()
	defer pool.Close()

	object_active_count := 10
	object_idle_count := 10
	object_active := make([]*objectHolder, 10)

	for idx := 0; idx < object_active_count; ++idx {
		object_active[idx] = get_object_and_check(t, pool)
	}

	for idx := 0; idx < object_idle_count; ++idx {
		object_holder := get_object_and_check(t, pool)
		pool.RetunOjbect(object_holder)
	}

	// check
	if pool.GetIdleObjectCount() != object_idle_count {
		t.Fatalf("idle object count invalid, expect:%d, get:%d", object_idle_count, pool.GetIdleObjectCount())
	}

	if pool.GetObjectCount() != object_idle_count + object_active_count {
		t.Fatalf("total object count invalid, expect:%d, get:%d", object_idle_count + object_active_count, pool.GetObjectCount())
	}

	if server_active_conn_count != object_idle_count + object_active_count {
		t.Fatalf("server_active_conn_count:%d, object_idle_count:%d, object_active_count:%d",
			server_active_conn_count, object_idle_count, object_active_count)
	}

	// clean up
	for object_holder := range object_active {
		pool.RetunOjbect(object_holder)
	}

	if pool.GetIdleObjectCount() != object_idle_count + object_active_count {
		t.Fatalf("idle object count invalid, expect:%d, get:%d", object_idle_count + object_active_count, pool.GetIdleObjectCount())
	}
}

// Test GetMaxObjectCount() & GetMinObjectCount() & GetIdleTime()
func TestGetMaxObjectCount(t *testing.T) {
	pool, err := NewObjectPool(uint512, uint_1024, idle_300s,
					conn_constructor, conn_destructor, conn_id_extractor)
	if err == nil {
		t.Fatalf("New() should checking idExtractor is not nil")
	}
	defer pool.Close()

	if pool.TestGetMaxObjectCount() != uint_1024 {
		t.Errorf("GetMaxObjectCount() return invalid value, expect:%d, get:%d", uint_1024, pool.GetMaxObjectCount())
	}

	if pool.GetMinObjectCount() != uint_512 {
		t.Errorf("GetMinObjectCount() return invalid value, expect:%d, get:%d", uint_512, pool.GetMinObjectCount())
	}

	if pool.GetIdleTime().Nanoseconds() != idle_300s.Nanoseconds() {
		t.Errorf("GetIdleTime() return invalid value, expect:%s, get:%s", idle_300s, pool.GetIdleTime())
	}
}



func fatal_is_nil(t *testing.T, object interface{}) {
	if object != nil {
		t.Fatalf("assert_is_nil() fail")
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
	pool, err := NewObjectPool(uint512, uint_1024, idle_300s,
					conn_constructor, conn_destructor, conn_id_extractor)
	if err == nil {
		t.Fatalf("New() should checking idExtractor is not nil")
	}

	return pool
}

func get_object_and_check(t *testing.T, *objectPool) *objectHolder {
	object_holder, err := pool.GetObject()
	if err != nil {
		t.Fatalf("GetObject() failed")
	}
	return object_holder
}