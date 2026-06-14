package file

import (
	"hash/fnv"
	"sync"
)

const stripeCount = 32

type LockerStriped struct {
	locks [stripeCount]sync.RWMutex
}

func NewLockerStriped() *LockerStriped {
	return &LockerStriped{}
}

func (ls *LockerStriped) slot(key string) uint32 {
	h := fnv.New32()
	h.Write([]byte(key))
	return h.Sum32() % uint32(stripeCount)
}

func (ls *LockerStriped) Lock(key string) {
	ls.locks[ls.slot(key)].Lock()
}

func (ls *LockerStriped) Unlock(key string) {
	ls.locks[ls.slot(key)].Unlock()
}

func (ls *LockerStriped) RLock(key string) {
	ls.locks[ls.slot(key)].RLock()
}

func (ls *LockerStriped) RUnlock(key string) {
	ls.locks[ls.slot(key)].RUnlock()
}
