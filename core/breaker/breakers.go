package breaker

import "sync"

var (
	lock     sync.RWMutex
	breakers = make(map[string]Breaker)
)

// 包装从breakers获取breaker逻辑再操作，下面几个函数均同
// Do calls Breaker.Do on the Breaker with given name.
func Do(name string, req func() error) error {
	return do(name, func(b Breaker) error {
		return b.Do(req)
	})
}

// DoWithAcceptable calls Breaker.DoWithAcceptable on the Breaker with given name.
func DoWithAcceptable(name string, req func() error, acceptable Acceptable) error {
	return do(name, func(b Breaker) error {
		return b.DoWithAcceptable(req, acceptable)
	})
}

// DoWithFallback calls Breaker.DoWithFallback on the Breaker with given name.
func DoWithFallback(name string, req func() error, fallback func(err error) error) error {
	return do(name, func(b Breaker) error {
		return b.DoWithFallback(req, fallback)
	})
}

// DoWithFallbackAcceptable calls Breaker.DoWithFallbackAcceptable on the Breaker with given name.
func DoWithFallbackAcceptable(name string, req func() error, fallback func(err error) error,
	acceptable Acceptable) error {
	return do(name, func(b Breaker) error {
		return b.DoWithFallbackAcceptable(req, fallback, acceptable)
	})
}

// GetBreaker returns the Breaker with the given name.
// 获取或新建breaker
func GetBreaker(name string) Breaker {
	lock.RLock()
	b, ok := breakers[name]
	lock.RUnlock()
	if ok {
		return b
	}

	lock.Lock()
	b, ok = breakers[name]
	if !ok {
		b = NewBreaker(WithName(name))
		breakers[name] = b
	}
	lock.Unlock()

	return b
}

// NoBreakerFor disables the circuit breaker for the given name.
// 限制指定名称breaker使用
func NoBreakerFor(name string) {
	lock.Lock()
	breakers[name] = newNoOpBreaker()
	lock.Unlock()
}

// 根据传入execute函数对查找到的name熔断器执行操作
func do(name string, execute func(b Breaker) error) error {
	return execute(GetBreaker(name))
}
