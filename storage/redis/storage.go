package redis

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage"
	"golang.org/x/sync/syncmap"

	"github.com/mediocregopher/radix/v3"
)

const ChannelBuffer = 100

type RedisStorage struct {
	c chan *CmdBatch
	logger log.Logger
	rpool *radix.Pool
	readpool *radix.Pool
	cache syncmap.Map
}

func CreatePools(urlStr string) (*radix.Pool, *radix.Pool, error) {
	parsedUrl, e := url.Parse(urlStr)
	if e != nil {
		return nil,	nil, e
	}
	poolSize := 10

	if parsedUrl.Scheme == "redis" {
		writePool, err := radix.NewPool("tcp", urlStr, poolSize)
		if err != nil {
			return nil, nil, err
		}
		readPool, err := radix.NewPool("tcp", urlStr, poolSize, radix.PoolPipelineWindow(0, 0))
		if err != nil {
			return nil, nil, err
		}

		return writePool, readPool, nil
	} else if parsedUrl.Scheme == "sentinel" {
		sentinel, e := radix.NewSentinel(strings.Replace(parsedUrl.Path, "/", "", 1), []string{parsedUrl.Host})
		if e != nil {
			return nil, nil, e
		}
		connFunc := radix.PoolConnFunc(func(network, _ string) (conn radix.Conn, e error) {
			primary, _ := sentinel.Addrs()
			return radix.DefaultConnFunc(network, primary)
		})
		writePool, err := radix.NewPool("tcp", urlStr, poolSize, connFunc)
		if err != nil {
			return nil, nil, err
		}
		readPool, err := radix.NewPool("tcp", urlStr, poolSize, connFunc, radix.PoolPipelineWindow(0, 0))
		if err != nil {
			return nil, nil, err
		}

		return writePool, readPool, nil

	} else {
		return nil, nil, fmt.Errorf("unkown scheme: %s", parsedUrl.Scheme)
	}
}

func NewRedisStorage(logger log.Logger, url string) *RedisStorage {
	if url == "" {
		return nil
	}
	rpool, readpool, err := CreatePools(url)
	if err != nil {
		panic(err)
	}
	rs := &RedisStorage{
		logger: logger,
		c: make(chan *CmdBatch, ChannelBuffer),
		rpool: rpool,
		readpool: readpool,
	}
	go rs.send()
	return rs
}

func (r *RedisStorage) send() {
	for batch := range r.c {
		e := r.rpool.Do(radix.Pipeline(*batch...))
		if e!= nil {
			_ = level.Error(r.logger).Log("msg", "Send failed", "error", e)
		}

	}
}

func (r *RedisStorage) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return NewRedisQuerier(r.readpool), nil
}

func (r *RedisStorage) StartTime() (int64, error) {
	return int64(model.Latest), nil
}

func (r *RedisStorage) Appender() (storage.Appender, error) {
	return NewRedisAppender(r.logger, r.c, &r.cache), nil
}

func (r *RedisStorage) Close() error {
	// TODO
	return nil
}