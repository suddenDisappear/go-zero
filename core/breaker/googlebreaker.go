package breaker

import (
	"math"
	"time"

	"github.com/zeromicro/go-zero/core/collection"
	"github.com/zeromicro/go-zero/core/mathx"
)

const (
	// 250ms for bucket duration
	window     = time.Second * 10 // 时间窗口总大小
	buckets    = 40               // 桶个数
	k          = 1.5              // 请求通过权重(假设10s内一共发起100个请求，通过了50个，则计算丢弃率时为15%)
	protection = 5                // 保留不统计请求数（总数减去该数量后才参与丢弃率计算）
)

// googleBreaker is a netflixBreaker pattern from google.
// see Client-Side Throttling section in https://landing.google.com/sre/sre-book/chapters/handling-overload/
type googleBreaker struct {
	k     float64
	stat  *collection.RollingWindow
	proba *mathx.Proba
}

// 新建google breaker
func newGoogleBreaker() *googleBreaker {
	// 每个bucket时间范围（滑动窗口总时间/桶个数）
	bucketDuration := time.Duration(int64(window) / int64(buckets))
	// 创建滑动窗口
	st := collection.NewRollingWindow(buckets, bucketDuration)
	return &googleBreaker{
		stat:  st,
		k:     k,
		proba: mathx.NewProba(),
	}
}

func (b *googleBreaker) accept() error {
	// 计算加权后通过数量
	accepts, total := b.history()
	weightedAccepts := b.k * float64(accepts)
	// https://landing.google.com/sre/sre-book/chapters/handling-overload/#eq2101
	// total+1防止除0异常
	dropRatio := math.Max(0, (float64(total-protection)-weightedAccepts)/float64(total+1))
	if dropRatio <= 0 {
		return nil
	}

	// 有<dropRaio>概率随机丢弃请求
	if b.proba.TrueOnProba(dropRatio) {
		return ErrServiceUnavailable
	}

	return nil
}

// 判断操作是否允许，如果不允许则报错
// 否则返回goolePromise
func (b *googleBreaker) allow() (internalPromise, error) {
	if err := b.accept(); err != nil {
		return nil, err
	}

	return googlePromise{
		b: b,
	}, nil
}

func (b *googleBreaker) doReq(req func() error, fallback func(err error) error, acceptable Acceptable) error {
	// 判断请求是否被接受
	if err := b.accept(); err != nil {
		// 如果设置了降级函数则触发服务降级逻辑
		if fallback != nil {
			return fallback(err)
		}

		return err
	}

	// 业务逻辑崩溃保证能正常标记
	defer func() {
		if e := recover(); e != nil {
			b.markFailure()
			panic(e)
		}
	}()

	// 处理业务逻辑
	err := req()
	// 业务逻辑错误若可以接受则认为请求处理成功
	if acceptable(err) {
		b.markSuccess()
	} else {
		b.markFailure()
	}

	return err
}

// 标记为成功
func (b *googleBreaker) markSuccess() {
	b.stat.Add(1)
}

// 标记为失败
func (b *googleBreaker) markFailure() {
	b.stat.Add(0)
}

// accepts:成功次数
// total:总调用次数
func (b *googleBreaker) history() (accepts, total int64) {
	b.stat.Reduce(func(b *collection.Bucket) {
		accepts += int64(b.Sum)
		total += b.Count
	})

	return
}

type googlePromise struct {
	b *googleBreaker
}

func (p googlePromise) Accept() {
	p.b.markSuccess()
}

func (p googlePromise) Reject() {
	p.b.markFailure()
}
