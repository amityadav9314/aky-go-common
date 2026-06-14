package file

import "sync"

type lockEntry struct {
	mu       *sync.RWMutex
	refCount int
}

type LockerPerFile struct {
	mu    sync.Mutex
	leMap map[string]*lockEntry
}

func NewLockerPerFile() *LockerPerFile {
	return &LockerPerFile{
		leMap: make(map[string]*lockEntry),
	}
}

func (locker *LockerPerFile) Lock(key string) {
	// lock for the map
	locker.mu.Lock()
	le := locker.leMap[key]
	var mu *sync.RWMutex
	if le == nil {
		mu = &sync.RWMutex{}
		locker.leMap[key] = &lockEntry{
			refCount: 1,
			mu:       mu,
		}
	} else {
		mu = le.mu
		le.refCount++
	}

	locker.mu.Unlock()
	mu.Lock()
}

func (locker *LockerPerFile) Unlock(key string) {
	// lock for the map
	locker.mu.Lock()
	le := locker.leMap[key]
	le.refCount--
	le.mu.Unlock()
	if le.refCount == 0 {
		delete(locker.leMap, key)
	}
	locker.mu.Unlock()
}

func (locker *LockerPerFile) RLock(key string) {
	locker.mu.Lock()
	le := locker.leMap[key]
	if le == nil {
		le = &lockEntry{mu: &sync.RWMutex{}}
		locker.leMap[key] = le
	}
	le.refCount++
	locker.mu.Unlock()

	le.mu.RLock()
}

func (locker *LockerPerFile) RUnlock(key string) {
	locker.mu.Lock()
	le := locker.leMap[key]
	le.refCount--
	le.mu.RUnlock()
	if le.refCount == 0 {
		delete(locker.leMap, key)
	}
	locker.mu.Unlock()
}
