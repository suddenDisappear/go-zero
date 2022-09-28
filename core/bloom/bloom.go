package bloom

import (
	"errors"
	"strconv"

	"github.com/zeromicro/go-zero/core/hash"
	"github.com/zeromicro/go-zero/core/stores/redis"
)

const (
	// for detailed error rate table, see http://pages.cs.wisc.edu/~cao/papers/summary-cache/node8.html
	// maps as k in the error rate table
	maps = 14
	// lua:保证操作原子性
	// 设置offsets脚本
	setScript = `
for _, offset in ipairs(ARGV) do
	redis.call("setbit", KEYS[1], offset, 1)
end
`
	// lua:保证操作原子性
	// 检查offsets是否设置脚本
	testScript = `
for _, offset in ipairs(ARGV) do
	if tonumber(redis.call("getbit", KEYS[1], offset)) == 0 then
		return false
	end
end
return true
`
)

// ErrTooLargeOffset indicates the offset is too large in bitset.
var ErrTooLargeOffset = errors.New("too large offset")

type (
	// A Filter is a bloom filter.
	Filter struct {
		bits   uint
		bitSet bitSetProvider
	}

	bitSetProvider interface {
		check([]uint) (bool, error)
		set([]uint) error
	}
)

// New create a Filter, store is the backed redis, key is the key for the bloom filter,
// bits is how many bits will be used, maps is how many hashes for each addition.
// best practices:
// elements - means how many actual elements
// when maps = 14, formula: 0.7*(bits/maps), bits = 20*elements, the error rate is 0.000067 < 1e-4
// for detailed error rate table, see http://pages.cs.wisc.edu/~cao/papers/summary-cache/node8.html
func New(store *redis.Redis, key string, bits uint) *Filter {
	return &Filter{
		bits:   bits,
		bitSet: newRedisBitSet(store, key, bits),
	}
}

// Add adds data into f.
func (f *Filter) Add(data []byte) error {
	// 获取hash映射的位
	locations := f.getLocations(data)
	// 设置对应位
	return f.bitSet.set(locations)
}

// Exists checks if data is in f.
func (f *Filter) Exists(data []byte) (bool, error) {
	// 获取hash映射的位
	locations := f.getLocations(data)
	// 检查所有位是否都被设置
	isSet, err := f.bitSet.check(locations)
	if err != nil {
		return false, err
	}

	return isSet, nil
}

// 获取数据对应位
func (f *Filter) getLocations(data []byte) []uint {
	locations := make([]uint, maps)
	// 一共需要maps次hash映射到对应位
	for i := uint(0); i < maps; i++ {
		// 没有使用k个不同hash函数而是对原数据使用k个后缀进行hash(利用murmurhash3碰撞激烈达到k个hash类似效果)
		hashValue := hash.Hash(append(data, byte(i)))
		// 根据实际位数进行映射
		locations[i] = uint(hashValue % uint64(f.bits))
	}

	return locations
}

type redisBitSet struct {
	store *redis.Redis
	key   string
	bits  uint
}

func newRedisBitSet(store *redis.Redis, key string, bits uint) *redisBitSet {
	return &redisBitSet{
		store: store,
		key:   key,
		bits:  bits,
	}
}

// 检查offset是否越界及str转化
func (r *redisBitSet) buildOffsetArgs(offsets []uint) ([]string, error) {
	var args []string

	for _, offset := range offsets {
		if offset >= r.bits {
			return nil, ErrTooLargeOffset
		}

		args = append(args, strconv.FormatUint(uint64(offset), 10))
	}

	return args, nil
}

func (r *redisBitSet) check(offsets []uint) (bool, error) {
	args, err := r.buildOffsetArgs(offsets)
	if err != nil {
		return false, err
	}

	resp, err := r.store.Eval(testScript, []string{r.key}, args)
	if err == redis.Nil {
		return false, nil
	} else if err != nil {
		return false, err
	}

	exists, ok := resp.(int64)
	if !ok {
		return false, nil
	}

	return exists == 1, nil
}

func (r *redisBitSet) del() error {
	_, err := r.store.Del(r.key)
	return err
}

func (r *redisBitSet) expire(seconds int) error {
	return r.store.Expire(r.key, seconds)
}

func (r *redisBitSet) set(offsets []uint) error {
	args, err := r.buildOffsetArgs(offsets)
	if err != nil {
		return err
	}

	_, err = r.store.Eval(setScript, []string{r.key}, args)
	if err == redis.Nil {
		return nil
	}

	return err
}
