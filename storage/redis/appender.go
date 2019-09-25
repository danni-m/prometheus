package redis

import (
	"bytes"
	"math"
	"strconv"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/mediocregopher/radix/v3"
	"github.com/prometheus/prometheus/pkg/labels"
)
const TS_ADD = "TS.ADD"
const LABELS = "LABELS"
const BATCH_SIZE = 500

type CmdBatch []radix.CmdAction

type RedisAppender struct {
	batch       CmdBatch
	cache       map[uint64]string
	sendChannel chan *CmdBatch
	logger      log.Logger
	rpool       *radix.Pool
}

func NewRedisAppender(logger log.Logger, sc chan *CmdBatch, pool *radix.Pool) *RedisAppender {
	return &RedisAppender{
		batch: make(CmdBatch, 0, BATCH_SIZE),
		sendChannel: sc,
		cache: make(map[uint64]string),
		logger: logger,
		rpool: pool,
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
	_, _ = lbuf.WriteTo(&buf)

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
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return
	}
	args := []string{
		keyName,
		strconv.FormatInt(t, 10),
		strconv.FormatFloat(v, 'f', 6, 64),
		LABELS,
	}
	for i := range l {
		args = append(args, l[i].Name, l[i].Value)
	}
	r.batch = append(r.batch, radix.Cmd(nil, TS_ADD, args...))
}

func (r *RedisAppender) AddFast(l labels.Labels, ref uint64, t int64, v float64) error {
	keyName := r.cache[ref]
	r.addToBatch(keyName, t, v, l)
	return nil
}

func (r *RedisAppender) Commit() error {
	select {
	case r.sendChannel <- &r.batch:
		//r.batch = make(CmdBatch, 0, BATCH_SIZE)
		return nil
	default:
		level.Error(r.logger).Log("msg", "couldn't send batch to channel")
	}
	return nil
}

func (r *RedisAppender) Rollback() error {
	level.Debug(r.logger).Log("msg", "Batch to be rollbacked")
	r.batch = r.batch[:0]
	return nil
}
