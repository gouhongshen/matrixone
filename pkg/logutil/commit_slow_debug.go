package logutil

import (
	"bytes"
	"context"
	"fmt"
	"github.com/panjf2000/ants/v2"
	"math"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"
)

var AppendDurationChan chan time.Duration
var durCache []time.Duration
var fixedBucketCnt int64 = 30
var maxCacheLen int = 1000 * 10 * 8
var pool *ants.Pool
var caches sync.Pool

var threadProfile = pprof.Lookup("threadcreate")

func InitDebug() {
	fmt.Println("threads num:", threadProfile.Count(), runtime.NumGoroutine(), runtime.NumCPU())

	AppendDurationChan = make(chan time.Duration, 1000*100)
	pool, _ = ants.NewPool(5)
	caches.New = func() any {
		return make([]time.Duration, 0)
	}
	durCache = caches.Get().([]time.Duration)
}

func DebugLoop(ctx context.Context) {
	logTicker := time.NewTicker(time.Second * 15)
	for {
		select {
		case <-ctx.Done():
			close(AppendDurationChan)
			return
		case <-logTicker.C:
			tmpCache := durCache
			pool.Submit(func() {
				doLog(tmpCache)
			})

			durCache = caches.Get().([]time.Duration)

		case dur := <-AppendDurationChan:
			durCache = append(durCache, dur)
			if len(durCache) >= maxCacheLen {
				tmpCache := durCache
				pool.Submit(func() {
					doLog(tmpCache)
				})

				durCache = caches.Get().([]time.Duration)
			}
		}
	}
}

func doLog(cache []time.Duration) {
	fmt.Println("threads num:", threadProfile.Count(), runtime.NumGoroutine(), runtime.NumCPU())
	if len(cache) == 0 {
		return
	}

	minDur, maxDur := time.Duration(math.MaxInt64), time.Duration(0)
	for idx := range cache {
		if cache[idx] < minDur {
			minDur = cache[idx]
		}

		if cache[idx] > maxDur {
			maxDur = cache[idx]
		}
	}

	step := time.Duration(int64(maxDur-minDur+1) / (fixedBucketCnt - 2))
	bucket := make([]int, fixedBucketCnt)

	for idx := range cache {
		gap := (cache[idx] - minDur) / step
		bucket[gap]++
	}

	cache = cache[:0]

	buf := &bytes.Buffer{}
	for idx := range bucket {
		lo := minDur + time.Duration(idx*int(step))
		hi := minDur + time.Duration((idx+1)*int(step))

		mid := (lo + hi) / 2

		if mid < time.Microsecond*10 {
			buf.WriteString(fmt.Sprintf("%5dns: %8d", mid.Nanoseconds(), bucket[idx]))
		} else if mid < time.Millisecond*10 {
			buf.WriteString(fmt.Sprintf("%5dus: %8d", mid.Microseconds(), bucket[idx]))
		} else if mid < time.Second*10 {
			buf.WriteString(fmt.Sprintf("%5dms: %8d", mid.Milliseconds(), bucket[idx]))
		} else {
			buf.WriteString(fmt.Sprintf("%5fs: %8d", mid.Seconds(), bucket[idx]))
		}

		buf.WriteString("\n")
	}

	buf.WriteString("\n\n\n")

	fmt.Println(buf.String())

	caches.Put(cache)

}
