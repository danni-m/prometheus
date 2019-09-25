package redis

import (
	"context"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage"

	"github.com/mediocregopher/radix/v3"
)

const CHANNEL_BUFFER = 1000

type RedisStorage struct {
	c chan *CmdBatch
	logger log.Logger
	rpool *radix.Pool
}

func NewRedisStorage(logger log.Logger) *RedisStorage {
	rpool, err := radix.NewPool("tcp", "redis://localhost:6379", 10, radix.PoolPipelineConcurrency(10))
	if err != nil {
		panic(err)
	}
	rs := &RedisStorage{
		logger: logger,
		c: make(chan *CmdBatch, CHANNEL_BUFFER),
		rpool: rpool,
	}
	go rs.send()
	return rs
}

func (r *RedisStorage) send() {
	//batch := <- r.c
	for batch := range r.c {
		e := r.rpool.Do(radix.Pipeline(*batch...))
		if e!= nil {
			_ = level.Error(r.logger).Log("msg", "Send failed", "error", e)
		}

	}
}

func (r *RedisStorage) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return nil, nil
}

func (r *RedisStorage) StartTime() (int64, error) {
	return int64(model.Latest), nil
}

func (r *RedisStorage) Appender() (storage.Appender, error) {
	return NewRedisAppender(r.logger, r.c, r.rpool), nil
}

func (r *RedisStorage) Close() error {
	// TODO
	return nil
}