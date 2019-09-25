package redis

import (
	"context"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage"
)

type RedisStorage struct {
	c chan batch
	logger log.Logger
}

func NewRedisStorage(logger log.Logger) *RedisStorage {
	return &RedisStorage{logger: logger}
}

func (r *RedisStorage) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return nil, nil
}

func (r *RedisStorage) StartTime() (int64, error) {
	return int64(model.Latest), nil
}

func (r *RedisStorage) Appender() (storage.Appender, error) {
	level.Error(r.logger).Log("msg", "new appender")
	return NewRedisAppender(r.logger, r.c), nil
}

func (r *RedisStorage) Close() error {
	// TODO
	return nil
}