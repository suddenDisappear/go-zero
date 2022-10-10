package collection

import (
	"sync"
	"time"

	"github.com/zeromicro/go-zero/core/timex"
)

type (
	// RollingWindowOption let callers customize the RollingWindow.
	RollingWindowOption func(rollingWindow *RollingWindow)

	// RollingWindow defines a rolling window to calculate the events in buckets with time interval.
	RollingWindow struct {
		lock          sync.RWMutex
		size          int
		win           *window
		interval      time.Duration
		offset        int
		ignoreCurrent bool
		lastTime      time.Duration // start time of the last bucket
	}
)

// NewRollingWindow returns a RollingWindow that with size buckets and time interval,
// use opts to customize the RollingWindow.
// 新建滑动窗口
// size:滑动窗口桶数量
// interval:每个桶覆盖时间范围
func NewRollingWindow(size int, interval time.Duration, opts ...RollingWindowOption) *RollingWindow {
	if size < 1 {
		panic("size must be greater than 0")
	}

	w := &RollingWindow{
		size:     size,            // 窗口大小
		win:      newWindow(size), // 窗口初始化
		interval: interval,        // 每个桶覆盖时间大小
		lastTime: timex.Now(),     // 上一个桶时间（初始化为当前相对时间）
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

// Add adds value to current bucket.
func (rw *RollingWindow) Add(v float64) {
	// 并发保护
	rw.lock.Lock()
	defer rw.lock.Unlock()
	// 更新为当前时间对应桶下标
	rw.updateOffset()
	// 加和
	rw.win.add(rw.offset, v)
}

// Reduce runs fn on all buckets, ignore current bucket if ignoreCurrent was set.
func (rw *RollingWindow) Reduce(fn func(b *Bucket)) {
	// 并发保护
	rw.lock.RLock()
	defer rw.lock.RUnlock()

	var diff int
	span := rw.span()
	// 例如：初始化为0s,lastTime为90s(offset为9)，间隔为10s，总大小为100s，桶数量为10个
	// 当前时间为122s(有效统计桶覆盖时间范围为起始时间[30s,120s]对应桶)，则span=3，即[10, 11, 12]号桶内存储的数据属于过期数据。统计范围为[9+3+1, 9+3+1+8)，即[3，4，5，6，7，8，9]
	// ignore current bucket, because of partial data
	if span == 0 && rw.ignoreCurrent {
		diff = rw.size - 1 // 忽视当前桶，因为当前桶计数可能不完全（需要配置开启）
	} else {
		diff = rw.size - span // 默认统计包含当前桶
	}
	// diff <= 0说明统计范围已经超出了滑动窗口记录范围，不进行统计
	if diff > 0 {
		// 从offset+span+1开始统计，因为offset+span也不属于历史数据范围
		offset := (rw.offset + span + 1) % rw.size
		// 统计[offset, offset+diff)
		rw.win.reduce(offset, diff, fn)
	}
}

// 从上一次触发到当前触发间隔桶个数
func (rw *RollingWindow) span() int {
	offset := int(timex.Since(rw.lastTime) / rw.interval)
	if 0 <= offset && offset < rw.size {
		return offset
	}

	return rw.size
}

func (rw *RollingWindow) updateOffset() {
	// 如果span<=0，则认为桶下标未变更，不更新offset
	span := rw.span()
	if span <= 0 {
		return
	}

	// 上一次触发offset
	offset := rw.offset
	// reset expired buckets
	// 重置过期桶
	for i := 0; i < span; i++ {
		// offset + i + 1: (offset, offset+span]
		rw.win.resetBucket((offset + i + 1) % rw.size)
	}

	// 更新offset
	rw.offset = (offset + span) % rw.size
	now := timex.Now()
	// align to interval time boundary
	// 对齐时间(对齐为offset桶左侧值，offset桶覆盖时间范围为[lastTime,lastTime+interval])
	rw.lastTime = now - (now-rw.lastTime)%rw.interval
}

// Bucket defines the bucket that holds sum and num of additions.
type Bucket struct {
	Sum   float64 // 加和(可以自行决定增加范围，如成功+1，失败+0)
	Count int64   // 计数(每次调用均自增1)
}

// 计数和加和同时增加
func (b *Bucket) add(v float64) {
	b.Sum += v
	b.Count++
}

// 重置桶计数
func (b *Bucket) reset() {
	b.Sum = 0
	b.Count = 0
}

// 窗口结构定义
type window struct {
	buckets []*Bucket // 桶列表
	size    int       // 时间窗口大小
}

func newWindow(size int) *window {
	// 初始化桶列表
	buckets := make([]*Bucket, size)
	for i := 0; i < size; i++ {
		buckets[i] = new(Bucket)
	}
	return &window{
		buckets: buckets,
		size:    size,
	}
}

// 根据桶下标加和&计数
func (w *window) add(offset int, v float64) {
	w.buckets[offset%w.size].add(v)
}

// 遍历桶并用fn处理
// 有效桶范围：[start, start+count)
func (w *window) reduce(start, count int, fn func(b *Bucket)) {
	for i := 0; i < count; i++ {
		fn(w.buckets[(start+i)%w.size])
	}
}

// 根据桶下标重置桶
func (w *window) resetBucket(offset int) {
	w.buckets[offset%w.size].reset()
}

// IgnoreCurrentBucket lets the Reduce call ignore current bucket.
// reduce配置当前桶不参与reduce计算
func IgnoreCurrentBucket() RollingWindowOption {
	return func(w *RollingWindow) {
		w.ignoreCurrent = true
	}
}
