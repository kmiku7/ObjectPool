package ObjectPool

import "time"

type objectHolder struct {
	object      interface{}
	createTime  time.Time
	lastUseTime time.Time
	useCount    uint64
	usable      bool
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

func (o objectHolder) IsUsable() bool {
	return o.usable
}

func (o *objectHolder) MarkUnusable() {
	o.usable = false
}
