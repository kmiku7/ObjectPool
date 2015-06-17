package ObjectPool

import (
	"errors"
	"time"
	"sync"
    "fmt"
)

type Constructor func() (interface{}, error)
type Destructor func(interface{})
type IdExtractor func(interface{}) string

var (
	ErrIsClosed = errors.New("object pool is closed")
    ErrNotExists = errors.New("object is not exist in the pool")
)

type objectPool struct {
	constructor 	Constructor
	destructor 		Destructor
	idExtractor 	IdExtractor

	idlePool 		[]*objectHolder
	activePool		map[*objectHolder]bool
	closed        	bool
	maxObjectCount 	uint32
	minObjectCount 	uint32
	idleTime       	time.Duration
	decreaseStep	uint32

	mutex			sync.Mutex

	destructQueue 	chan *objectHolder
	finishSignal 	chan bool
}

func NewObjectPool(
		min_object 	uint32,
		max_object 	uint32,
		idle_time 	time.Duration,
		constructor Constructor,
		destructor 	Destructor,
		idExtractor IdExtractor) (*objectPool, error) {

	if max_object != 0 && min_object > max_object {
		return nil, fmt.Errorf("min_object should lower or equal to max_object, max_object:%d, min_object:%d", max_object, min_object)
	}

	if constructor == nil {
		return nil, errors.New("need parameter constructor")
	}

	if destructor == nil {
		return nil, errors.New("need parameter destructor")
	}

	if idExtractor == nil {
		return nil, errors.New("need parameter idExtractor")
	}

	pool := &objectPool {
		constructor:	constructor,
		destructor:		destructor,
		idExtractor:	idExtractor,
		closed: false,
		maxObjectCount: max_object,
		minObjectCount: min_object,
		idleTime: idle_time,
	}

    decreaseStep := uint32(0)
    if max_object != 0 {
        decreaseStep = max_object - min_object
    }
	decreaseStep = decreaseStep / 20
    if decreaseStep < 10 {
        decreaseStep = 10
    }

	pool.decreaseStep = decreaseStep

	pool.destructQueue = make(chan*objectHolder, decreaseStep * 2)
	pool.finishSignal = make(chan bool, 1)
    pool.activePool = make(map[*objectHolder]bool)

	go pool.idleObjectDestructor()

	return pool, nil
}

func (p *objectPool) GetObject() (*objectHolder, error) {
	p.mutex.Lock()

	if p.closed {
		p.mutex.Unlock()
		return nil, ErrIsClosed
	}

	objectIdle := len(p.idlePool)
	if objectIdle > 0 {
		object := p.idlePool[objectIdle-1]
		p.idlePool = p.idlePool[:objectIdle-1]
		p.activePool[object] = true
		p.mutex.Unlock()
        object.useCount += 1
		return object, nil
	}

	objectActive := len(p.activePool)

	if objectActive + objectIdle > int(p.maxObjectCount) {
		p.mutex.Unlock()
		return nil, errors.New("reach max object count limits")
	}

	object := &objectHolder{}
	p.activePool[object] = true
	object.useCount = 1
	object.usable = true
	object.lastUseTime = time.Now()
	object.createTime = object.lastUseTime

	p.mutex.Unlock()

	inner_object, cons_err := p.constructor()
	if cons_err != nil {
		p.mutex.Lock()
		delete(p.activePool, object)
		p.mutex.Unlock()
		return nil, fmt.Errorf("create new object failed, constructor_error:%s", cons_err)
	}

	object.object = inner_object

	return object, nil
}

func (p *objectPool) ReturnObject(object *objectHolder) error {
	p.mutex.Lock()

	if p.closed {
		p.mutex.Unlock()
		return ErrIsClosed
	}

    if object == nil {
		p.mutex.Unlock()
        return nil
    }

    if _, has := p.activePool[object]; !has {
		p.mutex.Unlock()
        return ErrNotExists
    }

	delete(p.activePool, object)

	allCount := len(p.idlePool) + len(p.activePool)

	if allCount > int(p.minObjectCount) {
		decreaseCount := allCount - int(p.minObjectCount)
		if int(p.decreaseStep)	< decreaseCount {
            decreaseCount = int(p.decreaseStep)
        }
		if len(p.idlePool) < decreaseCount  {
            decreaseCount = len(p.idlePool)
        }

        count := 0
		CASUAL:
		for ; count < decreaseCount; count += 1 {
			if p.idlePool[count].lastUseTime.Add(p.idleTime).After(time.Now()) {
				break CASUAL
			}
			select {
			case p.destructQueue <- p.idlePool[count]:
			default:
				break CASUAL
			}
		}
		copy(p.idlePool, p.idlePool[count:])
		p.idlePool = p.idlePool[:len(p.idlePool)-count]
		allCount = len(p.idlePool) + len(p.activePool)
	}


	if object.IsUsable() {
		p.idlePool = append(p.idlePool, object)
		p.mutex.Unlock()
	} else {
		p.mutex.Unlock()
		p.destructor(object.object)
	}
	return nil
}

func (p *objectPool) Close() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.closed {
		return
	}

	p.closed = true
	close(p.destructQueue)

	// must wait all object destructed.
	for _, object := range p.idlePool {
		p.destructor(object.object)
	}
	p.idlePool = []*objectHolder{}

	for object, _ := range p.activePool {
		p.destructor(object.object)
	}
	p.activePool = map[*objectHolder]bool{}

	<- p.finishSignal
}


func (p *objectPool) idleObjectDestructor() {
	for object := range p.destructQueue {
		p.destructor(object.object)
	}
	p.finishSignal <- true
}


// Accessor
func (p objectPool) IsClosed() bool {
	return p.closed
}

func (p objectPool) GetObjectCount() uint32 {
	return uint32(len(p.idlePool) + len(p.activePool))
}

func (p objectPool) GetIdleObjectCount() uint32 {
	return uint32(len(p.idlePool))
}

func (p objectPool) GetMaxObjectCount() uint32 {
	return p.maxObjectCount
}

func (p objectPool) GetMinObjectCount() uint32 {
	return p.minObjectCount
}

func (p objectPool) GetIdleTime() time.Duration {
	return p.idleTime
}
