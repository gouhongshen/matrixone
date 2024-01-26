package logutil

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"time"
)

var AppendDurationChan chan time.Duration
var durCache []time.Duration
var fixedBucketCnt int64 = 30
var maxCacheLen int = 1000 * 100

func InitDebug() {
	AppendDurationChan = make(chan time.Duration, 1000*100)
	durCache = make([]time.Duration, 0)
}

func DebugLoop(ctx context.Context) {
	logTicker := time.NewTicker(time.Second * 15)
	for {
		select {
		case <-ctx.Done():
			close(AppendDurationChan)
			return
		case <-logTicker.C:
			doLog()
		case dur := <-AppendDurationChan:
			durCache = append(durCache, dur)
			if len(durCache) >= maxCacheLen {
				doLog()
			}
		}
	}
}

func doLog() {
	if len(durCache) == 0 {
		return
	}

	minDur, maxDur := time.Duration(math.MaxInt64), time.Duration(0)
	for idx := range durCache {
		if durCache[idx] < minDur {
			minDur = durCache[idx]
		}

		if durCache[idx] > maxDur {
			maxDur = durCache[idx]
		}
	}

	step := time.Duration(int64(maxDur-minDur+1) / (fixedBucketCnt - 2))
	bucket := make([]int, fixedBucketCnt)

	for idx := range durCache {
		gap := (durCache[idx] - minDur) / step
		bucket[gap]++
	}

	durCache = durCache[:0]

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

}
