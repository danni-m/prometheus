package redis

import (
	"bytes"
	"strconv"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/prometheus/pkg/labels"
)
const TS_ADD = "TS.ADD"
const LABELS = "LABELS"

type batch *[][]string

type RedisAppender struct {
	batch       [][]string
	cache       map[uint64]string
	sendChannel chan batch
	logger      log.Logger
}

func NewRedisAppender(logger log.Logger, sc chan batch) *RedisAppender {
	return &RedisAppender{
		batch: make([][]string, 0, 100),
		sendChannel: sc,
		cache: make(map[uint64]string),
		logger: logger,
	}
}

func metricToKeyName(ls labels.Labels) (keyName string) {
	var lbuf bytes.Buffer
	var metricName *string
	for i := range ls {
		if ls[i].Name == labels.MetricName {
			metricName = &ls[i].Value
		}
		lbuf.WriteString(ls[i].Name)
		lbuf.WriteString("=")
		lbuf.WriteString(ls[i].Value)
		if i < ls.Len() -1 {
			lbuf.WriteString(",")
		}
	}
	var buf bytes.Buffer
	buf.WriteString(*metricName)
	buf.WriteString("{")

	buf.WriteString("}")
	return buf.String()
}

func (r *RedisAppender) Add(l labels.Labels, t int64, v float64) (uint64, error) {
	keyName := metricToKeyName(l)
	ref := l.Hash()
	r.cache[ref] = keyName
	r.addToBatch(keyName, t, v, l)
	return ref, nil
}

func (r *RedisAppender) addToBatch(keyName string, t int64, v float64, l labels.Labels) {
	cmd := []string{
		TS_ADD,
		keyName,
		strconv.FormatInt(t, 10),
		strconv.FormatFloat(v, 'f', 6, 64),
		LABELS,
	}
	for i := range l {
		cmd = append(cmd, l[i].Name, l[i].Value)
	}
	r.batch = append(r.batch, cmd)
}

func (r *RedisAppender) AddFast(l labels.Labels, ref uint64, t int64, v float64) error {
	keyName := r.cache[ref]
	r.addToBatch(keyName, t, v, l)
	return nil
}

func (r *RedisAppender) Commit() error {
	level.Debug(r.logger).Log("msg", "Batch to be committed")
	r.sendChannel <- &r.batch
	r.batch = make([][]string, 0, 100)
	return nil
}

func (r *RedisAppender) Rollback() error {
	level.Debug(r.logger).Log("msg", "Batch to be rollbacked")
	r.batch = r.batch[:0]
	return nil
}
