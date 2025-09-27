package main

import (
	"fmt"
	"sync"
	"time"
)

type ShardCacheItem struct {
	value      interface{}
	expiration *time.Time
}

type Cache interface {
	Get(key interface{}) (interface{}, bool)
	Set(key interface{}, value interface{}) error
	SetWithTTL(key interface{}, value interface{}, ttl time.Duration) error
}

type ShardCache struct {
	items map[string]*ShardCacheItem
	mutex sync.RWMutex
}

func NewShardCache() *ShardCache {
	return &ShardCache{
		items: make(map[string]*ShardCacheItem),
	}
}

func (c *ShardCache) Set(key string, value interface{}) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.items[key] = &ShardCacheItem{
		value:      value,
		expiration: nil,
	}
}

func (c *ShardCache) SetWithTTL(key string, value interface{}, ttl time.Duration) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	var expiration time.Time
	if ttl > 0 {
		expiration = time.Now().Add(ttl)
	}

	c.items[key] = &ShardCacheItem{
		value:      value,
		expiration: &expiration,
	}
}

func (c *ShardCache) deleteExpired(key string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if _, ok := c.items[key]; !ok {
		return
	}
	delete(c.items, key)

}
func (c *ShardCache) Get(key string) (interface{}, bool) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	item, ok := c.items[key]
	if !ok {
		return nil, false
	}

	if item.expiration != nil && time.Now().After(*item.expiration) {
		go c.deleteExpired(key)
		return nil, false
	}
	return item.value, true
}

type InMemoryCache struct {
	shards     map[int]*ShardCache
	shardCount int
}

func NewInMemoryCache(n int) *InMemoryCache {
	shards := make(map[int]*ShardCache, n)

	for i := 0; i < n; i++ {
		shards[i] = NewShardCache()
	}

	return &InMemoryCache{
		shards:     shards,
		shardCount: n,
	}
}

func (i *InMemoryCache) Get(key interface{}) (interface{}, bool) {
	keyStr, ok := key.(string)
	if !ok {
		return nil, false
	}

	shardIndex := i.getShardIndex(keyStr)
	shard := i.shards[shardIndex]
	return shard.Get(keyStr)
}

func (i *InMemoryCache) Set(key interface{}, value interface{}) error {
	keyStr, ok := key.(string)
	if !ok {
		return fmt.Errorf("key must be a string")
	}

	shardIndex := i.getShardIndex(keyStr)
	shard := i.shards[shardIndex]
	shard.Set(keyStr, value)
	return nil
}

func (i *InMemoryCache) SetWithTTL(key interface{}, value interface{}, ttl time.Duration) error {
	keyStr, ok := key.(string)
	if !ok {
		return fmt.Errorf("key must be a string")
	}

	shardIndex := i.getShardIndex(keyStr)
	shard := i.shards[shardIndex]
	shard.SetWithTTL(keyStr, value, ttl)
	return nil
}

func (i *InMemoryCache) getShardIndex(key string) int {
	hash := 0
	for _, char := range key {
		hash = (hash*31 + int(char)) % i.shardCount
	}
	return hash
}

func main() {
	cache := NewInMemoryCache(4)
	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := cache.Set("key1", "value1")
		if err != nil {
			fmt.Printf("Error setting key1: %v\n", err)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := cache.SetWithTTL("key2", "value2", 2*time.Second)
		if err != nil {
			fmt.Printf("Error setting key2: %v\n", err)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := cache.SetWithTTL("key3", "value3", 100*time.Millisecond)
		if err != nil {
			fmt.Printf("Error setting key3: %v\n", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		value, found := cache.Get("key1")
		fmt.Printf("Goroutine Get key1: %v, found: %v\n", value, found)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		value, found := cache.Get("key2")
		fmt.Printf("Goroutine Get key2: %v, found: %v\n", value, found)
	}()

	value, found := cache.Get("key1")
	fmt.Printf("Main thread Get key1: %v, found: %v\n", value, found)

	value, found = cache.Get("key2")
	fmt.Printf("Main thread Get key2: %v, found: %v\n", value, found)

	value, found = cache.Get("key3")
	fmt.Printf("Main thread Get key3: %v, found: %v\n", value, found)

	time.Sleep(200 * time.Millisecond)
	value, found = cache.Get("key3")
	fmt.Printf("After TTL expiration - key3: %v, found: %v\n", value, found)

	wg.Wait()
}
