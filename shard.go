package lightCache

import (
	"github.com/guanhonly/lightCache/queue"
	"os"
	"reflect"
	"sync"
	"time"
)

type shard struct {
	hash              map[string]*queue.Element
	entries           queue.LinkedQueue
	ttl               int64
	lock              sync.RWMutex
	enablePersistence bool
	logHandler        *os.File
}

func initShard(ttl int64) *shard {
	return &shard{
		hash:    make(map[string]*queue.Element),
		entries: queue.NewByteQueue(),
		ttl:     ttl,
	}
}

func (s *shard) cleanUp(currentTimestamp int64) {
	s.lock.Lock()
	for {
		if s.entries.IsEmpty() {
			break
		}
		oldestEntry := s.entries.Peek()
		if !oldestEntry.IsValid() {
			break
		}
		if !s.Evict(oldestEntry, currentTimestamp) {
			break
		}
	}
	s.lock.Unlock()
}

func (s *shard) Evict(oldestEntry queue.Element, currentTimestamp int64) bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	if oldestEntry.IsExpired(currentTimestamp) {
		ele := s.entries.Pop()
		if ele.IsValid() {
			delete(s.hash, oldestEntry.Key())
		} else {
			return false
		}
		if s.enablePersistence {
			go s.writeToWAL([]byte(CommandDelete), []byte(ele.Key()))
		}
		return true
	}
	return false
}

func (s *shard) setWithTTL(key string, value []byte, ttl int64) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if prevElement, exist := s.hash[key]; exist {
		return s.entries.Update(prevElement, key, value)
	}
	newElement := queue.NewElementWithTTL(key, value, ttl)
	s.hash[key] = newElement
	return s.entries.Push(newElement)
}

func (s *shard) set(key string, value []byte) error {
	return s.setWithTTL(key, value, s.ttl)
}

func (s *shard) get(key string) ([]byte, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	if element, exist := s.hash[key]; exist {
		nowTime := time.Now().Unix()
		if s.ttl <= 0 || !element.IsExpired(nowTime) {
			return element.Value(), true
		}
		if element.IsExpired(nowTime) {
			s.delete(key)
		}
	}
	return nil, false
}

func (s *shard) delete(key string) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	element := s.hash[key]
	delete(s.hash, key)
	return s.entries.Remove(element)
}

func (s *shard) memUsage() uintptr {
	s.lock.Lock()
	defer s.lock.Unlock()
	memSize := s.topMemUsage()
	memSize += s.hashMemUsage()
	memSize += s.queueMemUsage()
	return memSize
}

func (s *shard) topMemUsage() uintptr {
	return reflect.TypeOf(*s).Size()
}

func (s *shard) hashMemUsage() uintptr {
	size := uintptr(0)
	for key, value := range s.hash {
		size += uintptr(len(key)) + value.MemUsage()
	}
	return size
}

func (s *shard) queueMemUsage() uintptr {
	return s.entries.MemUsage()
}

func (s *shard) removeOldestOne() {
	entry := s.entries.Pop()
	if entry.IsValid() {
		delete(s.hash, entry.Key())
	}
}

func (s *shard) oldestTime() int64 {
	oldestEntry := s.entries.Peek()
	if oldestEntry.IsValid() {
		return oldestEntry.LastModifyTimestamp()
	}
	return -1
}

func (s *shard) oldestKey() string {
	oldestEntry := s.entries.Peek()
	if oldestEntry.IsValid() {
		return oldestEntry.Key()
	}
	return ""
}

func (s *shard) size() int {
	return len(s.hash)
}

func (s *shard) writeToWAL(args ...[]byte) {
	s.lock.Lock()
	defer s.lock.Unlock()
	var command []byte
	for _, arg := range args {
		command = append(command, arg...)
		command = append(command, '\t')
	}
	command = append(command, '\n')
	_, _ = s.logHandler.Write(command)
}
