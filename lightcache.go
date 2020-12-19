package lightCache

import (
	"bufio"
	"io"
	"log"
	"os"
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
	CmdSeparator                = ' '
	DefaultLogChanSize          = 1 << 10
)

type cache struct {
	shards            []*shard
	globalTTL         int64
	close             chan struct{}
	mask              uint32
	cleanInterval     time.Duration
	capacity          int64
	maxMemory         uintptr
	lock              sync.RWMutex
	enablePersistence bool
	logHandler        *os.File
	logChan           chan []byte
}

type cacheBuilder struct {
	shardsNum         uint32
	globalTTL         int64
	capacity          int64
	maxMemory         uintptr
	cleanInterval     time.Duration
	enablePersistence bool
	logFileName       *string
	strongPersistence bool
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

func (c *cacheBuilder) WithLogFileName(fileName string) {
	c.logFileName = &fileName
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
	if c.enablePersistence {
		var walFileName string
		if c.logFileName == nil {
			walFileName = "WAL.log"
		} else {
			walFileName = *c.logFileName
		}
		cache.initWAL(walFileName)
		if c.strongPersistence {
			cache.logChan = make(chan []byte) // If strong persistence is enabled, use no buffer channel to ensure it.
		} else {
			cache.logChan = make(chan []byte, DefaultLogChanSize)
		}
	}
	return cache
}

func (c *cache) initWAL(walPath string) {
	if !pathExists(walPath) {
		_, _ = os.Create(walPath)
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
	args := strings.Split(cmd, string(CmdSeparator))
	if len(args) < 2 {
		return
	}
	currTs := time.Now().Unix()
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
		expiredAt, err := strconv.ParseInt(args[3], 10, 64)
		if err != nil {
			return
		}
		if expiredAt > currTs {
			_ = c.SetWithTTL(key, value, expiredAt-currTs)
		}
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
			if int64(c.size()) > c.capacity || c.memUsage() > c.maxMemory {
				c.cleanUp(t.Unix())
			}
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

func (c *cache) Set(key string, value []byte) error {
	if c.enablePersistence {
		c.appendWAL([]byte(CommandSet), value)
	}
	hashKey := hash(key) % c.mask
	return c.shards[hashKey].set(key, value)
}

func (c *cache) appendWAL(args ...[]byte) {
	var cmd []byte
	for _, arg := range args {
		cmd = append(cmd, arg...)
		cmd = append(cmd, CmdSeparator)
	}
	c.logChan <- cmd
}

func (c *cache) handleWAL() {
	for cmd := range c.logChan {
		c.logHandler.Write(cmd)
		c.logHandler.Write([]byte{'\n'})
	}
}

func (c *cache) SetWithTTL(key string, value []byte, ttl int64) error {
	if c.enablePersistence {
		c.appendWAL([]byte(CommandSetWithTTL), []byte(key), value, Int64ToBytes(ttl+time.Now().Unix()))
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
		c.appendWAL([]byte(CommandDelete), []byte(key))
	}
	hashKey := hash(key) % c.mask
	return c.shards[hashKey].delete(key)
}

func (c *cache) Close() error {
	close(c.close)
	if c.logHandler != nil {
		c.logHandler.Close()
	}
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
