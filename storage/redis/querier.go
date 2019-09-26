package redis

import (
	"bufio"
	"bytes"
	"errors"
	"sort"
	"strconv"

	"github.com/mediocregopher/radix/v3"
	"github.com/mediocregopher/radix/v3/resp/resp2"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
)

const TS_MRANGE = "TS.MRANGE"
const FILTER = "FILTER"

var errInvalidMrangeEntry = errors.New("invalid mrange entry")

type RedisLabelSet struct {
	L labels.Labels
}

func (r *RedisLabelSet) UnmarshalRESP(br *bufio.Reader) error {
	var labelsArray resp2.ArrayHeader
	var labelArray resp2.ArrayHeader
	if err := labelsArray.UnmarshalRESP(br); err != nil {
		return err
	}

	for j := 0; j < labelsArray.N; j++ {
		if err := labelArray.UnmarshalRESP(br); err != nil {
			return err
		}
		if labelArray.N != 2 {
			return errInvalidMrangeEntry
		}
		var key resp2.BulkString
		var value resp2.BulkString
		if err := key.UnmarshalRESP(br); err != nil {
			return err
		}
		if err := value.UnmarshalRESP(br); err != nil {
			return err
		}
		r.L = append(r.L, labels.Label{
			Name:  key.S,
			Value: value.S,
		})
	}
	return nil
}

type Sample struct {
	timestamp int64
	value     float64
}

func (s *Sample) UnmarshalRESP(br *bufio.Reader) error {
	var rootArray resp2.ArrayHeader
	var t resp2.Int
	var v resp2.BulkString
	if err := rootArray.UnmarshalRESP(br); err != nil {
		return err
	}
	if rootArray.N != 2 {
		return errInvalidMrangeEntry
	}
	if err := t.UnmarshalRESP(br); err != nil {
		return err
	}
	if err := v.UnmarshalRESP(br); err != nil {
		return err
	}
	s.timestamp = t.I
	var err error
	s.value, err = strconv.ParseFloat(v.S, 64)
	if err != nil {
		return err
	}
	return nil
}

type RedisSamples struct {
	Samples []Sample
}

func (r *RedisSamples) UnmarshalRESP(br *bufio.Reader) error {
	var rootArray resp2.ArrayHeader
	if err := rootArray.UnmarshalRESP(br); err != nil {
		return err
	}
	for i := 0; i < rootArray.N; i++ {
		var sample Sample
		if err := sample.UnmarshalRESP(br); err != nil {
			return err
		}
		r.Samples = append(r.Samples, sample)
	}
	return nil
}

type RedisTSResult struct {
	Samples RedisSamples
	keyName string
	labels  RedisLabelSet
}

func (r *RedisTSResult) Len() int {
	return len(r.Samples.Samples)
}
func (r *RedisTSResult) Labels() labels.Labels {
	return r.labels.L
}

func (r *RedisTSResult) Iterator() storage.SeriesIterator {
	return &RedisTSResultIterator{result: r}
}

type RedisTSResultIterator struct {
	result *RedisTSResult
	index  int
}

func (r *RedisTSResultIterator) Seek(t int64) bool {
	r.index = sort.Search(len(r.result.Samples.Samples), func(n int) bool {
		return r.result.Samples.Samples[n].timestamp >= t
	})
	return r.index < r.result.Len()
}

func (r *RedisTSResultIterator) At() (t int64, v float64) {
	return r.result.Samples.Samples[r.index].timestamp, r.result.Samples.Samples[r.index].value
}

func (r *RedisTSResultIterator) Next() bool {
	r.index++
	return r.index < r.result.Len()
}

func (r *RedisTSResultIterator) Err() error {
	return nil
}

func (r *RedisTSResult) UnmarshalRESP(br *bufio.Reader) error {
	var rootArray resp2.ArrayHeader
	if err := rootArray.UnmarshalRESP(br); err != nil {
		return err
	}
	if rootArray.N != 3 {
		return errInvalidMrangeEntry
	}
	var keyName resp2.BulkStringBytes
	if err := keyName.UnmarshalRESP(br); err != nil {
		return err
	}

	if err := r.labels.UnmarshalRESP(br); err != nil {
		return err
	}

	if err := r.Samples.UnmarshalRESP(br); err != nil {
		return err
	}
	return nil
}

type RedisMRangeResult struct {
	Results []RedisTSResult
	index   int
}

func (r *RedisMRangeResult) Next() bool {
	if r.index+1 < len(r.Results) {
		r.index++
		return true
	} else {
		return false
	}
}

func (r *RedisMRangeResult) At() storage.Series {
	return &r.Results[r.index]
}

func (r *RedisMRangeResult) Err() error {
	return nil
}

func (r *RedisMRangeResult) UnmarshalRESP(br *bufio.Reader) error {
	r.index = -1
	var rootArray resp2.ArrayHeader
	if err := rootArray.UnmarshalRESP(br); err != nil {
		return err
	}
	for i := 0; i < rootArray.N; i++ {
		var item RedisTSResult
		if err := item.UnmarshalRESP(br); err != nil {
			return err
		}
		r.Results = append(r.Results, item)
	}
	return nil
}

type RedisQuerier struct {
	rpool *radix.Pool
}

func NewRedisQuerier(rpool *radix.Pool) *RedisQuerier {
	return &RedisQuerier{rpool: rpool}
}

func labelMatcherFormat(l *labels.Matcher) (string, error) {
	var buf bytes.Buffer
	buf.WriteString(l.Name)
	switch l.Type {
	case labels.MatchEqual:
		buf.WriteString("=")
	case labels.MatchNotEqual:
		buf.WriteString("!=")
	default:
		return "", errors.New("matcher is not supported")
	}

	buf.WriteString(l.Value)
	return buf.String(), nil
}

func (r *RedisQuerier) Select(sp *storage.SelectParams, ls ...*labels.Matcher) (storage.SeriesSet, storage.Warnings, error) {
	var args []string
	args = append(args, strconv.FormatInt(sp.Start, 10), strconv.FormatInt(sp.End, 10))
	args = append(args, FILTER)
	for l := range ls {
		matcher, err := labelMatcherFormat(ls[l])
		if err != nil {
			return nil, nil, err
		}
		args = append(args, matcher)
	}
	var result RedisMRangeResult
	cmd := radix.Cmd(&result, TS_MRANGE, args...)
	err := r.rpool.Do(cmd)
	if err != nil {
		return nil, nil, err
	}
	return &result, nil, nil
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
