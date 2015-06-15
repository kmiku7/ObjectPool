package ObjectPool

import (
	"errors"
	"time"
)

type objectPool struct {
	pool           []*objectHolder
	closed         bool
	objectCount    uint64
	objectIdle     uint64
	maxObjectCount uint64
	minObjectCount uint64
	idleTime       time.Duration
}

func NewObjectPool(max_object uint64, min_object uint64, idle_time time.Duration) (objectPool, error) {
	if min_object > max_object {
		return errors.New("min_object should lower or equal to max_object, max_object:%d, min_object:%d", max_object, min_object)
	}

}

// Accessor
func (p objectPool) IsClosed() bool {
	return p.closed
}

func (p objectPool) GetObjectCount() uint64 {
	return p.objectCount
}

func (p objectPool) GetIdleObjectCount() uint64 {
	return p.objectIdle
}

func (p objectPool) GetMaxObjectCount() uint64 {
	return p.maxObjectCount
}

func (p objectPool) GetMinObjectCount() uint64 {
	return p.minObjectCount
}

func (p objectPool) GetIdleTime() time.Duration {
	return p.idleTime
}
