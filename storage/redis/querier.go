package redis

import (
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
)

type RedisQuerier struct {

}

func (r *RedisQuerier) Select(*storage.SelectParams, ...*labels.Matcher) (storage.SeriesSet, storage.Warnings, error) {
	return nil, nil, nil
}

func (r *RedisQuerier) LabelValues(name string) ([]string, storage.Warnings, error) {
	return nil, nil, nil
}

func (r *RedisQuerier) LabelNames() ([]string, storage.Warnings, error) {
	return nil, nil, nil
}

func (r *RedisQuerier) Close() error {
	// TODO?
	return nil
}

