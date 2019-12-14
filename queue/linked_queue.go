package queue

import (
	"reflect"
	"sync"
	"time"
)

type Element struct {
	key   string
	value []byte
	ts    int64
	valid bool
	ttl   int64
	prev  *Element
	next  *Element
	lock  *sync.RWMutex
}

type LinkedQueue interface {
	Pop() Element
	Push(data *Element) error
	Peek() Element
	Size() int
	IsEmpty() bool
	Update(ele *Element, key string, value []byte) error
	Remove(ele *Element) error
	MemUsage() uintptr
}

type queueImp struct {
	size int
	head *Element
	tail *Element
	lock sync.RWMutex
}

func NewByteQueue() LinkedQueue {
	head := &Element{valid: false}
	tail := &Element{valid: false}
	head.next = tail
	tail.prev = head
	return &queueImp{
		head: head,
		tail: tail,
		size: 0,
	}
}

func (b *queueImp) Pop() Element {
	if b.IsEmpty() {
		return Element{valid: false}
	}
	b.lock.Lock()
	defer b.lock.Unlock()
	first := b.head.next
	second := b.head.next.next
	b.head.next, second.prev = second, b.head
	b.size--
	return *first
}

func (b *queueImp) Push(ele *Element) error {
	b.lock.Lock()
	defer b.lock.Unlock()
	if ele.valid == false {
		ele.valid = true
	}
	ele.ts = time.Now().Unix()
	b.addToTail(ele)
	b.size++
	return nil
}

func (b *queueImp) Peek() Element {
	if b.IsEmpty() {
		return Element{valid: false}
	}
	b.lock.RLock()
	defer b.lock.RUnlock()
	return *b.head.next
}

func (b *queueImp) Remove(ele *Element) error {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.detachFromQueue(ele)
	b.size--
	return nil
}

func (b *queueImp) Size() int {
	return b.size
}

func (b *queueImp) IsEmpty() bool {
	return b.size <= 0
}

func (b *queueImp) MemUsage() uintptr {
	return reflect.TypeOf(*b).Size()
}

func (e *Element) IsValid() bool {
	return e.valid
}

func (e *Element) IsExpired(currentTimestamp int64) bool {
	if currentTimestamp-e.ts > e.ttl {
		return true
	}
	return false
}

func (e *Element) Key() string {
	return e.key
}

func (e *Element) Value() []byte {
	return e.value
}

func (e *Element) LastModifyTimestamp() int64 {
	return e.ts
}

func (e *Element) MemUsage() uintptr {
	e.lock.Lock()
	defer e.lock.Unlock()
	size := reflect.TypeOf(*e).Size()
	size += uintptr(len(e.value))
	return size
}

func (b *queueImp) Update(ele *Element, key string, value []byte) error {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.detachFromQueue(ele)
	ele.key = key
	ele.value = value
	ele.ts = time.Now().Unix()
	b.addToTail(ele)
	return nil
}

func (b *queueImp) detachFromQueue(ele *Element) {
	prev := ele.prev
	next := ele.next
	prev.next, next.prev = next, prev
}

func (b *queueImp) addToTail(ele *Element) {
	prevLast := b.tail.prev
	ele.prev, ele.next = prevLast, b.tail
	prevLast.next, b.tail.prev = ele, ele
}

func NewElementWithTTL(key string, value []byte, ttl int64) *Element {
	return &Element{
		key:   key,
		value: value,
		valid: true,
		ttl:   ttl,
	}
}
