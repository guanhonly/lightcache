package lightCache

import (
	"bufio"
	"io"
	"log"
	"os"
	"os/user"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	MaxRetries           int    = 100
	DefaultShardNum      uint32 = 32
	DefaultGlobalTTL     int64  = 60 * 60
	DefaultCleanInterval        = 10 * time.Second
	CommandSet                  = "SET"
	CommandSetWithTTL           = "SETWITHTTL"
	CommandDelete               = "DEL"
)

type cache struct {
	shards            []*shard
	globalTTL         int64
	close             chan struct{}
	mask              uint32
	cleanInterval     time.Duration
	capacity          int64
	maxMemory         uintptr
	lock              *sync.RWMutex
	enablePersistence bool
	logHandler        *os.File
}

type cacheBuilder struct {
	shardsNum         uint32
	globalTTL         int64
	capacity          int64
	maxMemory         uintptr
	cleanInterval     time.Duration
	enablePersistence bool
}

func NewCacheBuilder() *cacheBuilder {
	return &cacheBuilder{}
}

func (c *cacheBuilder) WithShardNum(num uint) {
	intNum := int(num)
	if intNum <= 0 {
		intNum = int(DefaultShardNum)
	}

	if !isPowerOfTwo(intNum) {
		intNum = getClosetPowerOfTwo(intNum)
	}
	c.shardsNum = uint32(intNum)
}

// the unit of ttl is second
func (c *cacheBuilder) WithGlobalTTL(num int64) {
	c.globalTTL = num
}

func (c *cacheBuilder) WithCapacity(cap int64) {
	c.capacity = cap
}

func (c *cacheBuilder) WithMaxMemory(maxMem uintptr) {
	c.maxMemory = maxMem
}

func (c *cacheBuilder) WithCleanWindow(duration time.Duration) {
	c.cleanInterval = duration
}

func (c *cacheBuilder) WithPersistence(persistence bool) {
	c.enablePersistence = persistence
}

func DefaultCacheBuilder() *cacheBuilder {
	return &cacheBuilder{
		shardsNum:         DefaultShardNum,
		globalTTL:         DefaultGlobalTTL,
		cleanInterval:     DefaultCleanInterval,
		enablePersistence: false,
	}
}

func (c *cacheBuilder) Build() *cache {
	if c.shardsNum <= 0 {
		c.shardsNum = DefaultShardNum
	}
	cache := &cache{
		shards:            make([]*shard, c.shardsNum),
		globalTTL:         c.globalTTL,
		close:             make(chan struct{}),
		mask:              c.shardsNum,
		cleanInterval:     c.cleanInterval,
		capacity:          c.capacity,
		maxMemory:         c.maxMemory,
		enablePersistence: c.enablePersistence,
	}
	for i := 0; i < int(c.shardsNum); i++ {
		cache.shards[i] = initShard(c.globalTTL)
	}
	go cleaner(cache)
	go memoryMaster(cache)
	if c.enablePersistence {
		cache.initWAL()
	}
	return cache
}

func (c *cache) initWAL() {
	usr, err := user.Current()
	if err != nil {
		log.Println(err)
		c.enablePersistence = false
		return
	}
	walFileName := "/WAL.lc"
	walDir := filepath.Join(usr.HomeDir, "/lightCache")
	walPath := filepath.Join(walDir, walFileName)
	if !pathExists(walDir) {
		_ = os.MkdirAll(walDir, 0777)
		_, _ = os.Create(walPath)
	} else {
		if !pathExists(walPath) {
			_, _ = os.Create(walPath)
		}
	}
	fp, err := os.OpenFile(walPath, os.O_RDWR|os.O_APPEND, 0777)
	if err != nil {
		log.Println(err)
		c.enablePersistence = false
		return
	}
	reader := bufio.NewReader(fp)
	for {
		line, _, err := reader.ReadLine()
		if err == io.EOF {
			break
		}
		c.executeOneCommand(string(line))
	}
	c.logHandler = fp
	for _, shard := range c.shards {
		shard.enablePersistence = true
		shard.logHandler = fp
	}
}

func (c *cache) executeOneCommand(cmd string) {
	args := strings.Split(cmd, "\t")
	if len(args) < 2 {
		return
	}
	switch args[0] {
	case CommandSet:
		if len(args) < 3 {
			return
		}
		key := args[1]
		value := []byte(args[2])
		_ = c.Set(key, value)
	case CommandSetWithTTL:
		if len(args) < 4 {
			return
		}
		key := args[1]
		value := []byte(args[2])
		ttl, err := strconv.ParseInt(args[3], 10, 64)
		if err != nil {
			return
		}
		_ = c.SetWithTTL(key, value, ttl)
	case CommandDelete:
		if len(args) < 2 {
			return
		}
		key := args[1]
		_ = c.Delete(key)
	}
}

func pathExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil || os.IsExist(err)
}

func cleaner(c *cache) {
	ticker := time.NewTicker(c.cleanInterval)
	defer ticker.Stop()
	for {
		select {
		case t := <-ticker.C:
			c.cleanUp(t.Unix())
		case <-c.close:
			return
		}
	}
}

func (c *cache) cleanUp(currentTimestamp int64) {
	for _, shard := range c.shards {
		shard.cleanUp(currentTimestamp)
	}
}

func memoryMaster(c *cache) {
	if c.maxMemory > 0 {
		ticker := time.NewTicker(c.cleanInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				go c.manageSizeAndMemory()
			case <-c.close:
				return
			}
		}
	}
}

func (c *cache) Set(key string, value []byte) error {
	if c.enablePersistence {
		go c.writeToWAL([]byte(CommandSet), value)
	}
	hashKey := hash(key) % c.mask
	return c.shards[hashKey].set(key, value)
}

func (c *cache) writeToWAL(args ...[]byte) {
	c.lock.Lock()
	defer c.lock.Unlock()
	var command []byte
	for _, arg := range args {
		command = append(command, arg...)
		command = append(command, '\t')
	}
	command = append(command, '\n')
	_, _ = c.logHandler.Write(command)
}

func (c *cache) SetWithTTL(key string, value []byte, ttl int64) error {
	if c.enablePersistence {
		go c.writeToWAL([]byte(CommandSetWithTTL), []byte(key), value, Int64ToBytes(ttl))
	}
	hashKey := hash(key) % c.mask
	return c.shards[hashKey].setWithTTL(key, value, ttl)
}

func (c *cache) Get(key string) (value []byte, hit bool) {
	hashKey := hash(key) % c.mask
	return c.shards[hashKey].get(key)
}

func (c *cache) Delete(key string) error {
	if c.enablePersistence {
		go c.writeToWAL([]byte(CommandDelete), []byte(key))
	}
	hashKey := hash(key) % c.mask
	return c.shards[hashKey].delete(key)
}

func (c *cache) Close() error {
	close(c.close)
	return nil
}

func (c *cache) memUsage() uintptr {
	memSize := reflect.TypeOf(*c).Size()
	for _, shard := range c.shards {
		memSize += shard.memUsage()
	}
	return memSize
}

func (c *cache) size() int {
	size := 0
	for _, shard := range c.shards {
		size += shard.size()
	}
	return size
}

func (c *cache) manageSizeAndMemory() {
	if c.maxMemory > 0 {
		retryTimes := 0
		for (int64(c.size()) > c.capacity || c.memUsage() > c.maxMemory) && retryTimes < MaxRetries {
			c.removeOldestOne()
			retryTimes++
		}
	}
}

func (c *cache) removeOldestOne() {
	allOldestTimestamp := make([]tuple, c.mask)
	for i, shard := range c.shards {
		allOldestTimestamp[i] = tuple{
			ts:    shard.oldestTime(),
			index: i,
			key:   shard.oldestKey(),
		}
	}
	oldestIndex, oldestKey := findOldestIndexAndKey(allOldestTimestamp)
	if c.enablePersistence {
		go c.writeToWAL([]byte(CommandDelete), []byte(oldestKey))
	}
	c.shards[oldestIndex].removeOldestOne()
}

func findOldestIndexAndKey(all []tuple) (int, string) {
	res := all[0]
	for _, i := range all {
		if i.ts < res.ts {
			res = i
		}
	}
	return res.index, res.key
}

type tuple struct {
	ts    int64
	index int
	key   string
}
