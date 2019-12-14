package lightCache

import (
	"encoding/binary"
	"hash/fnv"
)

func isPowerOfTwo(num int) bool {
	return (num & (num - 1)) == 0
}

func getClosetPowerOfTwo(num int) int {
	bit := 32 << (^uint(0) >> 63)
	if bit == 32 {
		if num > 1<<31 {
			return 1 << 32
		}
		return int(getHighestBitOfInt32(int32(num)))
	}
	if num > 1<<63-1 {
		return 1<<63 - 1
	}
	return int(getHighestBitOfInt64(int64(num)))
}

func getHighestBitOfInt32(num int32) int32 {
	num |= num >> 1
	num |= num >> 2
	num |= num >> 4
	num |= num >> 8
	num |= num >> 16
	return num - (num >> 1)
}

func getHighestBitOfInt64(num int64) int64 {
	num |= num >> 1
	num |= num >> 2
	num |= num >> 4
	num |= num >> 8
	num |= num >> 16
	num |= num >> 32
	return num - (num >> 1)
}

func hash(key string) uint32 {
	hasher := fnv.New32()
	hasher.Write([]byte(key))
	return hasher.Sum32()
}

func Int64ToBytes(i int64) []byte {
	var buf = make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(i))
	return buf
}
