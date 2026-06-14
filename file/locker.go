package file

type Locker interface {
	Lock(key string)
	RLock(key string)
	Unlock(key string)
	RUnlock(key string)
}
