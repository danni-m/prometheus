package redis

import (
	"bufio"
	"bytes"
	"errors"
	"math"
	"strconv"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/mediocregopher/radix/v3"
	"github.com/mediocregopher/radix/v3/resp/resp2"
	"github.com/prometheus/prometheus/pkg/labels"
	"golang.org/x/sync/syncmap"
)
const TS_ADD = "TS.ADD"
const LABELS = "LABELS"
const BATCH_SIZE = 500

type CmdBatch []radix.CmdAction

type RedisAppender struct {
	batch       CmdBatch
	cache       *syncmap.Map
	sendChannel chan *CmdBatch
	logger      log.Logger
}

type DummyResult struct {
}

func (d *DummyResult) UnmarshalRESP(br *bufio.Reader) error {
	var r resp2.Int
	err := r.UnmarshalRESP(br)
	return err
}

var DR DummyResult

func NewRedisAppender(logger log.Logger, sc chan *CmdBatch, cache *syncmap.Map) *RedisAppender {
	return &RedisAppender{
		batch: make(CmdBatch, 0, BATCH_SIZE),
		sendChannel: sc,
		cache: cache,
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
	_, _ = lbuf.WriteTo(&buf)

	buf.WriteString("}")
	return buf.String()
}

func (r *RedisAppender) Add(l labels.Labels, t int64, v float64) (uint64, error) {
	keyName := metricToKeyName(l)
	ref := l.Hash()
	r.cache.Store(ref, keyName)
	r.addToBatch(keyName, t, v, l)
	return ref, nil
}

func (r *RedisAppender) addToBatch(keyName string, t int64, v float64, l labels.Labels) {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return
	}
	args := make([]string, 0, 4 + len(l)*2)
	args = append(args,
		keyName,
		strconv.FormatInt(t, 10),
		strconv.FormatFloat(v, 'f', 6, 64),
		LABELS)

	for i := range l {
		args = append(args, l[i].Name, l[i].Value)
	}

	r.batch = append(r.batch, radix.Cmd(&DR, TS_ADD, args...))
}

func (r *RedisAppender) AddFast(ref uint64, t int64, v float64) error {
	keyName, ok := r.cache.Load(ref)
	if !ok {
		return errors.New("not found in cache")
	}
	r.addToBatch(keyName.(string), t, v, nil)
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
