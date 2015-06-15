package ObjectPool

import "time"

type objectHolder struct {
	object      interface{}
	createTime  time.Time
	useCount    uint64
	lastUseTime time.Time
}

func (o objectHolder) ExtractObject() interface{} {
	return o.object
}

func (o objectHolder) GetCreateTime() time.Time {
	return o.createTime
}

func (o objectHolder) GetUseCount() uint64 {
	return o.useCount
}
