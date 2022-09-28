package stringx

import (
	crand "crypto/rand"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

const (
	// 可随机字符列表
	letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	// 一次6位用来表示字符索引(最大化利用一次随机生成的结果，因为只需6位即可表示所有letterBytes索引)
	letterIdxBits = 6 // 6 bits to represent a letter index
	// 生成id长度
	idLen = 8
	// 默认随机字符串长度
	defaultRandLen = 8
	// letterIdxBits位全1二进制掩码
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	// 63位随机数可以用来做letterIdxMax次随机
	letterIdxMax = 63 / letterIdxBits // # of letter indices fitting in 63 bits
)

// 创建单例对象作为全局对象使用（使用当前nano时间作为随机种子）
var src = newLockedSource(time.Now().UnixNano())

type lockedSource struct {
	source rand.Source
	lock   sync.Mutex
}

// 返回新的随机源（因为非并发安全所以需要lock操作）
func newLockedSource(seed int64) *lockedSource {
	return &lockedSource{
		source: rand.NewSource(seed),
	}
}

func (ls *lockedSource) Int63() int64 {
	ls.lock.Lock()
	defer ls.lock.Unlock()
	return ls.source.Int63()
}

func (ls *lockedSource) Seed(seed int64) {
	ls.lock.Lock()
	defer ls.lock.Unlock()
	ls.source.Seed(seed)
}

// Rand returns a random string.
func Rand() string {
	return Randn(defaultRandLen)
}

// RandId returns a random id string.
func RandId() string {
	b := make([]byte, idLen)
	// crand.Reader是一个全局、共享的密码用强随机数生成器
	_, err := crand.Read(b)
	if err != nil {
		// 生成失败降级为随机字符串
		return Randn(idLen)
	}
	// 根据crand.Reader格式化为16进制字符串
	return fmt.Sprintf("%x%x%x%x", b[0:2], b[2:4], b[4:6], b[6:8])
}

// Randn returns a random string with length n.
func Randn(n int) string {
	b := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	// i:用于保证生成n长度字符串
	// cache:使用Int63一次生成多次使用
	// remain:当前cache剩余可用次数
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		// 次数不足重置remain并重新生成cache
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		// 从cache中取出末letterIdxBits位，当取出的结果满足索引范围则随机出一个字符
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}

// Seed sets the seed to seed.
// 替换全局随机种子
func Seed(seed int64) {
	src.Seed(seed)
}
