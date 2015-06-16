package ObjectPool

import (
	"errors"
	"time"
	"sync"
)

type Constructor func() (interface{}, error)
type Destructor func(interface{})
type IdExtractor func(interface{}) string

var {
	ErrIsClosed = error.New("object pool is closed")
}

type objectPool struct {
	constructor 	Constructor
	destructor 		Destructor
	idExtractor 	IdExtractor

	idlePool 		[]*objectHolder
	activePool		map[*objectHolder]bool
	closed        	bool
	maxObjectCount 	uint64
	minObjectCount 	uint64
	idleTime       	time.Duration
	decreaseStep	uint64

	mutex			sync.Mutex

	destructQueue 	chan *objectHolder
	finishSignal 	chan bool
}

func NewObjectPool(
		min_object 	uint64,
		max_object 	uint64,
		idle_time 	time.Duration,
		constructor Constructor,
		destructor 	Destructor,
		idExtractor IdExtractor
	) (*objectPool, error) {

	if max_object != 0 && min_object > max_object {
		return nil, errors.New("min_object should lower or equal to max_object, max_object:%d, min_object:%d", max_object, min_object)
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

	decreaseStep := max_object != 0 ? max_object - min_object : 0
	decreaseStep = decreaseStep / 20
	decreaseStep = decreaseStep > 10 ? decreaseStep : 10

	pool.decreaseStep = decreaseStep

	pool.destructQueue = make(chan interface{}, decreaseStep * 2)
	pool.exitSignal = make(chan bool, 1)
	pool.finishSignal = make(chan bool, 1)

	go p.idleObjectDestructor()

	return pool, nil
}

func (p *objectPool) GetObject() (*objectHolder, error) {
	p.mutex.Lock()

	if p.closed {
		p.mutex.Unlock()
		return nil, ErrIsClosed
	}

	p.closed = true
	objectIdle := len(p.idlePool)
	if objectIdle > 0 {
		object := p.idlePool[objectIdle-1]
		p.idlePool = p.idlePool[:objectIdle-1]
		activePool[object] = true
		p.mutex.Unlock()
		return object, nil
	}

	objectActive := len(p.activePool)

	if objectActive + objectIdle > p.max_object {
		p.mutex.Unlock()
		return nil, errors.New("reach max object count limits")
	}

	object = &objectHolder{}
	activePool[object] = true
	object.useCount = 0
	object.usable = true
	object.createTime = object.lastUseTime = time.Now()

	p.mutex.Unlock()

	inner_object, cons_err := p.constructor()
	if cons_err != nil {
		p.mutex.Lock()
		delete(p.activePool, object)
		p.mutex.Unlock()
		return nil, errors.New("create new object failed, constructor_error:%s", cons_err)
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

	delete(p.activePool, objectHolder)

	allCount := len(p.idlePool) + len(p.activePool)

	if allCount > p.minObjectCount {
		decreaseCount := allCount - p.minObjectCount
		decreaseCount := p.decreaseStep	< decreaseCount ? p.decreaseStep : decreaseCount
		decreaseCount := len(p.idlePool) < p.decreaseStep ? len(p.idlePool) : p.decreaseStep
		CASUAL:
		for count := 0; count < p.decreaseStep; ++count {
			if p.idlePool[count].lastUseTime.Add(p.idleTime).After(time.Now()) {
				break CASUAL
			}
			select {
			case p.destructQueue <- p.idlePool[count]:
			case default:
				break CASUAL
			}
		}
		copy(p.idlePool, p.idlePool[count:])
		p.idlePool := p.idlePool[:len(idlePool)-count]
		allCount = len(p.idlePool) + len(p.activePool)
	}


	if object.IsUsable() {
		p.idlePool = append(p.idlePool, objectHolder)
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
	for object := range p.idlePool {
		p.destructor(object.object)
	}
	p.idlePool = []*objectHolder

	for object, _ := range p.activePool {
		p.destructor(object.object)
	}
	p.activePool = map[*objectHolder]bool{}

	<- p.finishSignal
}


func (p *objectOPool) idleObjectDestructor() {
	CLEANER:
	for object := <- p.destructQueue {
		p.destructor(object.object)
	}
	p.finishSignal <- true
}


// Accessor
func (p objectPool) IsClosed() bool {
	return p.closed
}

func (p objectPool) GetObjectCount() uint64 {
	return len(p.idlePool) + len(p.activePool)
}

func (p objectPool) GetIdleObjectCount() uint64 {
	return len(p.idlePool)
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
