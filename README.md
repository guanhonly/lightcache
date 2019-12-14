# lightCache
A lightweight but full-featured Go key-value local memory cache.  
It's an initial version so far, some features may not meet expectations.
## Features(Expected)
* Lightweight.
* Fast enough.
* Thread safe.
* Enable TTL(Time-to-live).
* Customizable TTL for each item.
* Customizable capacity(the maximum number of items allowed in cache).
* Customizable maximum memory usage.
* Simple API.
* Simple source code.
* Data persistence(inspired by Redis AOF).

## QuickStart
### Installation
```shell script
go get github.com/guanhonly/lightCache
```
### Usage
```go
// create lightCache with default config, which
// uses global ttl as 1 hour and does not enable
// consistence.
cache := DefaultCacheBuilder().Build()

// set key value without customized ttl(use global ttl).
cache.Set("key", []byte("value"))

// set key value with customized ttl.
cache.SetWithTTL("key", []byte("value"), 3*60*60)

// get value through key and check if hit cache.
value, hit := cache.Get("key")

// delete key from cache.
cache.Delet("key")
```

### Limitations
* Keys must be string and values must be bytes. Other types must marshaled to bytes to store in cache.
* Don't support callback functions for expiration, which many other caches did. This is for light weight.
