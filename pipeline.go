package redis

import (
	"errors"
	"sync"

	"time"

	"github.com/go-redis/redis/internal/pool"
)

type pipelineExecer func([]Cmder) error

// Pipeline implements pipelining as described in
// http://redis.io/topics/pipelining. It's safe for concurrent use
// by multiple goroutines.

type RedisPipilineInterface interface {
	Append(key, value string) *IntCmd
	Auth(password string) *StatusCmd
	BLPop(timeout time.Duration, keys ...string) *StringSliceCmd
	BRPop(timeout time.Duration, keys ...string) *StringSliceCmd
	BRPopLPush(source, destination string, timeout time.Duration) *StringCmd
	BgRewriteAOF() *StatusCmd
	BgSave() *StatusCmd
	BitCount(key string, bitCount *BitCount) *IntCmd
	BitOpAnd(destKey string, keys ...string) *IntCmd
	BitOpNot(destKey string, key string) *IntCmd
	BitOpOr(destKey string, keys ...string) *IntCmd
	BitOpXor(destKey string, keys ...string) *IntCmd
	BitPos(key string, bit int64, pos ...int64) *IntCmd
	ClientGetName() *StringCmd
	ClientKill(ipPort string) *StatusCmd
	ClientList() *StringCmd
	ClientPause(dur time.Duration) *BoolCmd
	ClientSetName(name string) *BoolCmd
	Close() error
	ClusterAddSlots(slots ...int) *StatusCmd
	ClusterAddSlotsRange(min, max int) *StatusCmd
	ClusterCountFailureReports(nodeID string) *IntCmd
	ClusterCountKeysInSlot(slot int) *IntCmd
	ClusterDelSlots(slots ...int) *StatusCmd
	ClusterDelSlotsRange(min, max int) *StatusCmd
	ClusterFailover() *StatusCmd
	ClusterForget(nodeID string) *StatusCmd
	ClusterInfo() *StringCmd
	ClusterKeySlot(key string) *IntCmd
	ClusterMeet(host, port string) *StatusCmd
	ClusterNodes() *StringCmd
	ClusterReplicate(nodeID string) *StatusCmd
	ClusterResetHard() *StatusCmd
	ClusterResetSoft() *StatusCmd
	ClusterSaveConfig() *StatusCmd
	ClusterSlaves(nodeID string) *StringSliceCmd
	ClusterSlots() *ClusterSlotsCmd
	Command() *CommandsInfoCmd
	ConfigGet(parameter string) *SliceCmd
	ConfigResetStat() *StatusCmd
	ConfigSet(parameter, value string) *StatusCmd
	DbSize() *IntCmd
	DebugObject(key string) *StringCmd
	Decr(key string) *IntCmd
	DecrBy(key string, decrement int64) *IntCmd
	Del(keys ...string) *IntCmd
	Discard() error
	Dump(key string) *StringCmd
	Echo(message interface{}) *StringCmd
	Eval(script string, keys []string, args ...interface{}) *Cmd
	EvalSha(sha1 string, keys []string, args ...interface{}) *Cmd
	Exec() ([]Cmder, error)
	Exists(keys ...string) *IntCmd
	Expire(key string, expiration time.Duration) *BoolCmd
	ExpireAt(key string, tm time.Time) *BoolCmd
	FlushAll() *StatusCmd
	FlushDb() *StatusCmd
	GeoAdd(key string, geoLocation ...*GeoLocation) *IntCmd
	GeoDist(key string, member1, member2, unit string) *FloatCmd
	GeoHash(key string, members ...string) *StringSliceCmd
	GeoPos(key string, members ...string) *GeoPosCmd
	GeoRadius(key string, longitude, latitude float64, query *GeoRadiusQuery) *GeoLocationCmd
	GeoRadiusByMember(key, member string, query *GeoRadiusQuery) *GeoLocationCmd
	Get(key string) *StringCmd
	GetBit(key string, offset int64) *IntCmd
	GetRange(key string, start, end int64) *StringCmd
	GetSet(key string, value interface{}) *StringCmd
	HDel(key string, fields ...string) *IntCmd
	HExists(key, field string) *BoolCmd
	HGet(key, field string) *StringCmd
	HGetAll(key string) *StringStringMapCmd
	HIncrBy(key, field string, incr int64) *IntCmd
	HIncrByFloat(key, field string, incr float64) *FloatCmd
	HKeys(key string) *StringSliceCmd
	HLen(key string) *IntCmd
	HMGet(key string, fields ...string) *SliceCmd
	HMSet(key string, fields map[string]interface{}) *StatusCmd
	HScan(key string, cursor uint64, match string, count int64) *ScanCmd
	HSet(key, field string, value interface{}) *BoolCmd
	HSetNX(key, field string, value interface{}) *BoolCmd
	HVals(key string) *StringSliceCmd
	Incr(key string) *IntCmd
	IncrBy(key string, value int64) *IntCmd
	IncrByFloat(key string, value float64) *FloatCmd
	Info(section ...string) *StringCmd
	Keys(pattern string) *StringSliceCmd
	LIndex(key string, index int64) *StringCmd
	LInsert(key, op string, pivot, value interface{}) *IntCmd
	LInsertAfter(key string, pivot, value interface{}) *IntCmd
	LInsertBefore(key string, pivot, value interface{}) *IntCmd
	LLen(key string) *IntCmd
	LPop(key string) *StringCmd
	LPush(key string, values ...interface{}) *IntCmd
	LPushX(key string, value interface{}) *IntCmd
	LRange(key string, start, stop int64) *StringSliceCmd
	LRem(key string, count int64, value interface{}) *IntCmd
	LSet(key string, index int64, value interface{}) *StatusCmd
	LTrim(key string, start, stop int64) *StatusCmd
	LastSave() *IntCmd
	MGet(keys ...string) *SliceCmd
	MSet(pairs ...interface{}) *StatusCmd
	MSetNX(pairs ...interface{}) *BoolCmd
	Migrate(host, port, key string, db int64, timeout time.Duration) *StatusCmd
	Move(key string, db int64) *BoolCmd
	ObjectEncoding(key string) *StringCmd
	ObjectIdleTime(key string) *DurationCmd
	ObjectRefCount(key string) *IntCmd
	PExpire(key string, expiration time.Duration) *BoolCmd
	PExpireAt(key string, tm time.Time) *BoolCmd
	PFAdd(key string, els ...interface{}) *IntCmd
	PFCount(keys ...string) *IntCmd
	PFMerge(dest string, keys ...string) *StatusCmd
	PTTL(key string) *DurationCmd
	Persist(key string) *BoolCmd
	Ping() *StatusCmd
	Process(cmd Cmder) error
	PubSubChannels(pattern string) *StringSliceCmd
	PubSubNumPat() *IntCmd
	PubSubNumSub(channels ...string) *StringIntMapCmd
	Publish(channel, message string) *IntCmd
	Quit() *StatusCmd
	RPop(key string) *StringCmd
	RPopLPush(source, destination string) *StringCmd
	RPush(key string, values ...interface{}) *IntCmd
	RPushX(key string, value interface{}) *IntCmd
	RandomKey() *StringCmd
	ReadOnly() *StatusCmd
	ReadWrite() *StatusCmd
	Rename(key, newkey string) *StatusCmd
	RenameNX(key, newkey string) *BoolCmd
	Restore(key string, ttl time.Duration, value string) *StatusCmd
	RestoreReplace(key string, ttl time.Duration, value string) *StatusCmd
	SAdd(key string, members ...interface{}) *IntCmd
	SCard(key string) *IntCmd
	SDiff(keys ...string) *StringSliceCmd
	SDiffStore(destination string, keys ...string) *IntCmd
	SInter(keys ...string) *StringSliceCmd
	SInterStore(destination string, keys ...string) *IntCmd
	SIsMember(key string, member interface{}) *BoolCmd
	SMembers(key string) *StringSliceCmd
	SMove(source, destination string, member interface{}) *BoolCmd
	SPop(key string) *StringCmd
	SPopN(key string, count int64) *StringSliceCmd
	SRandMember(key string) *StringCmd
	SRandMemberN(key string, count int64) *StringSliceCmd
	SRem(key string, members ...interface{}) *IntCmd
	SScan(key string, cursor uint64, match string, count int64) *ScanCmd
	SUnion(keys ...string) *StringSliceCmd
	SUnionStore(destination string, keys ...string) *IntCmd
	Save() *StatusCmd
	Scan(cursor uint64, match string, count int64) *ScanCmd
	ScriptExists(scripts ...string) *BoolSliceCmd
	ScriptFlush() *StatusCmd
	ScriptKill() *StatusCmd
	ScriptLoad(script string) *StringCmd
	Select(index int) *StatusCmd
	Set(key string, value interface{}, expiration time.Duration) *StatusCmd
	SetBit(key string, offset int64, value int) *IntCmd
	SetNX(key string, value interface{}, expiration time.Duration) *BoolCmd
	SetRange(key string, offset int64, value string) *IntCmd
	SetXX(key string, value interface{}, expiration time.Duration) *BoolCmd
	Shutdown() *StatusCmd
	ShutdownNoSave() *StatusCmd
	ShutdownSave() *StatusCmd
	SlaveOf(host, port string) *StatusCmd
	SlowLog()
	Sort(key string, sort Sort) *StringSliceCmd
	SortInterfaces(key string, sort Sort) *SliceCmd
	StrLen(key string) *IntCmd
	Sync()
	TTL(key string) *DurationCmd
	Time() *TimeCmd
	Type(key string) *StatusCmd
	Unlink(keys ...string) *IntCmd
	Wait(numSlaves int, timeout time.Duration) *IntCmd
	ZAdd(key string, members ...Z) *IntCmd
	ZAddCh(key string, members ...Z) *IntCmd
	ZAddNX(key string, members ...Z) *IntCmd
	ZAddNXCh(key string, members ...Z) *IntCmd
	ZAddXX(key string, members ...Z) *IntCmd
	ZAddXXCh(key string, members ...Z) *IntCmd
	ZCard(key string) *IntCmd
	ZCount(key, min, max string) *IntCmd
	ZIncr(key string, member Z) *FloatCmd
	ZIncrBy(key string, increment float64, member string) *FloatCmd
	ZIncrNX(key string, member Z) *FloatCmd
	ZIncrXX(key string, member Z) *FloatCmd
	ZInterStore(destination string, store ZStore, keys ...string) *IntCmd
	ZRange(key string, start, stop int64) *StringSliceCmd
	ZRangeByLex(key string, opt ZRangeBy) *StringSliceCmd
	ZRangeByScore(key string, opt ZRangeBy) *StringSliceCmd
	ZRangeByScoreWithScores(key string, opt ZRangeBy) *ZSliceCmd
	ZRangeWithScores(key string, start, stop int64) *ZSliceCmd
	ZRank(key, member string) *IntCmd
	ZRem(key string, members ...interface{}) *IntCmd
	ZRemRangeByLex(key, min, max string) *IntCmd
	ZRemRangeByRank(key string, start, stop int64) *IntCmd
	ZRemRangeByScore(key, min, max string) *IntCmd
	ZRevRange(key string, start, stop int64) *StringSliceCmd
	ZRevRangeByLex(key string, opt ZRangeBy) *StringSliceCmd
	ZRevRangeByScore(key string, opt ZRangeBy) *StringSliceCmd
	ZRevRangeByScoreWithScores(key string, opt ZRangeBy) *ZSliceCmd
	ZRevRangeWithScores(key string, start, stop int64) *ZSliceCmd
	ZRevRank(key, member string) *IntCmd
	ZScan(key string, cursor uint64, match string, count int64) *ScanCmd
	ZScore(key, member string) *FloatCmd
	ZUnionStore(dest string, store ZStore, keys ...string) *IntCmd
}

type Pipeline struct {
	cmdable
	statefulCmdable

	exec pipelineExecer

	mu     sync.Mutex
	cmds   []Cmder
	closed bool
}

func (c *Pipeline) Process(cmd Cmder) error {
	c.mu.Lock()
	c.cmds = append(c.cmds, cmd)
	c.mu.Unlock()
	return nil
}

// Close closes the pipeline, releasing any open resources.
func (c *Pipeline) Close() error {
	c.mu.Lock()
	c.discard()
	c.closed = true
	c.mu.Unlock()
	return nil
}

// Discard resets the pipeline and discards queued commands.
func (c *Pipeline) Discard() error {
	c.mu.Lock()
	err := c.discard()
	c.mu.Unlock()
	return err
}

func (c *Pipeline) discard() error {
	if c.closed {
		return pool.ErrClosed
	}
	c.cmds = c.cmds[:0]
	return nil
}

// Exec executes all previously queued commands using one
// client-server roundtrip.
//
// Exec always returns list of commands and error of the first failed
// command if any.
func (c *Pipeline) Exec() ([]Cmder, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, pool.ErrClosed
	}

	if len(c.cmds) == 0 {
		return nil, errors.New("redis: pipeline is empty")
	}

	cmds := c.cmds
	c.cmds = nil

	return cmds, c.exec(cmds)
}

func (c *Pipeline) pipelined(fn func(RedisPipilineInterface) error) ([]Cmder, error) {
	if err := fn(c); err != nil {
		return nil, err
	}
	cmds, err := c.Exec()
	_ = c.Close()
	return cmds, err
}
