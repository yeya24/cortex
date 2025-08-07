package distributed_execution

import (
	"sync"
	"time"
)

const (
	DefaultTTL = 5 * time.Minute
)

type QueryResultCache struct {
	sync.RWMutex
	cache map[FragmentKey]FragmentResult
}

type FragmentStatus string

const (
	StatusWriting FragmentStatus = "writing"
	StatusDone    FragmentStatus = "done"
	StatusError   FragmentStatus = "error"
)

type FragmentResult struct {
	Data       interface{}
	Status     FragmentStatus
	Expiration time.Time
}

type FragmentKey struct {
	queryID    uint64
	fragmentID uint64
}

func MakeFragmentKey(queryID uint64, fragmentID uint64) FragmentKey {
	return FragmentKey{
		queryID:    queryID,
		fragmentID: fragmentID,
	}
}

func (f FragmentKey) GetQueryID() uint64 {
	return f.queryID
}

func (f FragmentKey) GetFragmentID() uint64 {
	return f.fragmentID
}

func NewQueryResultCache() *QueryResultCache {
	return &QueryResultCache{
		cache: make(map[FragmentKey]FragmentResult),
	}
}

func (qrc *QueryResultCache) Size() int {
	return len(qrc.cache)
}

func (qrc *QueryResultCache) InitWriting(key FragmentKey) {
	qrc.Lock()
	defer qrc.Unlock()
	qrc.cache[key] = FragmentResult{
		Status:     StatusWriting,
		Expiration: time.Now().Add(DefaultTTL),
	}
}

func (qrc *QueryResultCache) SetComplete(key FragmentKey, data interface{}) {
	qrc.Lock()
	defer qrc.Unlock()
	qrc.cache[key] = FragmentResult{
		Data:       data,
		Status:     StatusDone,
		Expiration: time.Now().Add(DefaultTTL),
	}
}

func (qrc *QueryResultCache) SetError(key FragmentKey) {
	qrc.Lock()
	defer qrc.Unlock()
	qrc.cache[key] = FragmentResult{
		Status:     StatusError,
		Expiration: time.Now().Add(DefaultTTL),
	}
}

func (qrc *QueryResultCache) IsReady(key FragmentKey) bool {
	qrc.RLock()
	defer qrc.RUnlock()
	if result, ok := qrc.cache[key]; ok {
		return result.Status == StatusDone
	}
	return false
}

func (qrc *QueryResultCache) Get(key FragmentKey) (FragmentResult, bool) {
	qrc.RLock()
	defer qrc.RUnlock()
	result, ok := qrc.cache[key]
	return result, ok
}

func (qrc *QueryResultCache) GetFragmentStatus(key FragmentKey) FragmentStatus {
	qrc.RLock()
	defer qrc.RUnlock()
	result, ok := qrc.cache[key]
	if !ok {
		return FragmentStatus("")
	}
	return result.Status
}

//go func() {
//	ticker := time.NewTicker(5 * time.Minute)
//	defer ticker.Stop()
//
//	for range ticker.C {
//		cache.CleanExpired()
//	}
//}()

func (qrc *QueryResultCache) CleanExpired() {
	qrc.Lock()
	defer qrc.Unlock()
	now := time.Now()
	for key, result := range qrc.cache {
		if now.After(result.Expiration) {
			delete(qrc.cache, key)
		}
	}
}

func (qrc *QueryResultCache) ClearQuery(queryID uint64) {
	qrc.Lock()
	defer qrc.Unlock()
	for key := range qrc.cache {
		if key.queryID == queryID {
			delete(qrc.cache, key)
		}
	}
}
