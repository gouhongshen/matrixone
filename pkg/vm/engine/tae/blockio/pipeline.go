// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package blockio

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/util/metric/stats"
	w "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks/worker"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

var (
	_jobPool = sync.Pool{
		New: func() any {
			return new(tasks.Job)
		},
	}
	_readerPool = sync.Pool{
		New: func() any {
			return new(objectio.ObjectReader)
		},
	}
)

func getJob(
	ctx context.Context,
	id string,
	typ tasks.JobType,
	exec tasks.JobExecutor) *tasks.Job {
	job := _jobPool.Get().(*tasks.Job)
	job.Init(ctx, id, typ, exec)
	return job
}

func putJob(job *tasks.Job) {
	job.Reset()
	_jobPool.Put(job)
}

func getReader(
	fs fileservice.FileService,
	location objectio.Location) *objectio.ObjectReader {
	job := _readerPool.Get().(*objectio.ObjectReader)
	job.Init(location, fs)
	return job
}

func putReader(reader *objectio.ObjectReader) {
	reader.Reset()
	_readerPool.Put(reader)
}

// At present, the read and write operations of all modules of mo-service use blockio.
// I have started/stopped IoPipeline when mo is initialized/stopped, but in order to
// be compatible with the UT of each module, I must add readColumns and noopPrefetch.

// Most UT cases do not call Start(), so in order to be compatible with these cases,
// the pipeline uses readColumns and noopPrefetch.In order to avoid the data race of UT,
// I did not switch pipeline.fetchFun and pipeline.prefetchFunc when
// I stopped, so I need to execute ResetPipeline again

const (
	Fetch = iota
	MetaPrefetch
	DataPrefetch
)

var pipeline *IoPipeline

type IOJobFactory func(context.Context, fetchParams) *tasks.Job

func init() {
	pipeline = NewIoPipeline()
}

func Start() {
	pipeline.Start()
	pipeline.fetch.fetchFun = pipeline.fetch.doFetch
	pipeline.metaPrefetch.prefetchFunc = pipeline.metaPrefetch.doPrefetch
	pipeline.dataPrefetch.prefetchFunc = pipeline.dataPrefetch.doPrefetch
}

func Stop() {
	pipeline.Stop()
}

func ResetPipeline() {
	pipeline = NewIoPipeline()
}

func makeName(location string) string {
	return fmt.Sprintf("%s-%d", location, time.Now().UTC().Nanosecond())
}

// load data job
func jobFactory(
	ctx context.Context,
	params fetchParams,
) *tasks.Job {
	return getJob(
		ctx,
		makeName(params.reader.GetName()),
		JTLoad,
		func(_ context.Context) (res *tasks.JobResult) {
			// TODO
			res = &tasks.JobResult{}
			ioVectors, err := readColumns(ctx, params)
			if err == nil {
				res.Res = ioVectors
			} else {
				res.Err = err
			}
			return
		},
	)
}

func fetchReader(params PrefetchParams) (reader *objectio.ObjectReader) {
	if params.reader != nil {
		reader = params.reader
	} else {
		reader = getReader(params.fs, params.key)
	}
	return
}

// prefetch data job
func prefetchJob(ctx context.Context, params PrefetchParams) *tasks.Job {
	reader := fetchReader(params)
	return getJob(
		ctx,
		makeName(reader.GetName()),
		JTLoad,
		func(_ context.Context) (res *tasks.JobResult) {
			// TODO
			res = &tasks.JobResult{}
			if params.dataType == objectio.CkpMetaStart {
				ioVectors, err := reader.ReadMultiSubBlocks(ctx, params.ids, nil)
				if err != nil {
					res.Err = err
					return
				}
				// no further reads
				res.Res = nil
				ioVectors.Release()
			} else if params.dataType == objectio.SchemaTombstone {
				ioVectors, err := reader.ReadTombstoneMultiBlocks(ctx,
					params.ids, nil)
				if err != nil {
					res.Err = err
					return
				}
				// no further reads
				res.Res = nil
				ioVectors.Release()
			} else {
				ioVectors, err := reader.ReadMultiBlocks(ctx,
					params.ids, nil)
				if err != nil {
					res.Err = err
					return
				}
				// no further reads
				res.Res = nil
				ioVectors.Release()
			}
			if params.reader == nil {
				putReader(reader)
			}
			return
		},
	)
}

// prefetch metadata job
func prefetchMetaJob(ctx context.Context, params PrefetchParams) *tasks.Job {
	name := params.key.Name().String()
	return getJob(
		ctx,
		makeName(name),
		JTLoad,
		func(_ context.Context) (res *tasks.JobResult) {
			res = &tasks.JobResult{}
			objectMeta, err := objectio.FastLoadObjectMeta(ctx, &params.key, true, params.fs)
			if err != nil {
				res.Err = err
				return
			}
			res.Res = objectMeta
			return
		},
	)
}

type FetchFunc = func(ctx context.Context, params fetchParams) (any, error)
type PrefetchFunc = func(params PrefetchParams) error

func readColumns(ctx context.Context, params fetchParams) (any, error) {
	return params.reader.ReadOneBlock(ctx, params.idxes, params.typs, params.blk, nil)
}

func noopPrefetch(params PrefetchParams) error {
	// Synchronous prefetch does not need to do anything
	return nil
}

type IoPipelineImpl struct {
	options struct {
		parallelism int
		queueDepth  int
		batchSize   int
	}

	queue     sm.Queue
	scheduler tasks.JobScheduler

	jobFactory IOJobFactory

	active    atomic.Bool
	onceStart sync.Once
	onceStop  sync.Once

	stats struct {
		droppedItemStats stats.Counter
	}
}

func (impl *IoPipelineImpl) fillDefaults() {
	if impl.options.parallelism <= 0 {
		impl.options.parallelism = runtime.NumCPU() * 4
	}

	if impl.options.queueDepth <= 0 {
		impl.options.queueDepth = 100000
	}

	if impl.options.batchSize <= 0 {
		impl.options.batchSize = 64
	}

	if impl.jobFactory == nil {
		impl.jobFactory = jobFactory
	}
}

// FetchIoPipeline

type FetchIoPipeline struct {
	IoPipelineImpl
	fetchFun FetchFunc
}

func newFetchIoPipeline(opts ...Option) *FetchIoPipeline {
	p := new(FetchIoPipeline)

	for _, opt := range opts {
		opt(&p.IoPipelineImpl)
	}

	p.fillDefaults()

	p.queue = sm.NewSafeQueue(p.options.queueDepth, p.options.batchSize, p.onFetch)
	p.scheduler = tasks.NewParallelJobScheduler(p.options.parallelism)

	p.fetchFun = readColumns

	return p
}

func (p *FetchIoPipeline) start() {
	p.onceStart.Do(func() {
		p.active.Store(true)
		p.queue.Start()
	})
}

func (p *FetchIoPipeline) stop() {
	p.onceStop.Do(func() {
		p.active.Store(false)
		p.queue.Stop()
		p.scheduler.Stop()
	})
}

func (p *FetchIoPipeline) onFetch(jobs ...any) {
	for _, j := range jobs {
		job := j.(*tasks.Job)
		if err := p.scheduler.Schedule(job); err != nil {
			job.DoneWithErr(err)
		}
	}
}

func (p *FetchIoPipeline) doAsyncFetch(ctx context.Context, params fetchParams) (job *tasks.Job, err error) {
	job = p.jobFactory(
		ctx,
		params,
	)
	if _, err = p.queue.Enqueue(job); err != nil {
		job.DoneWithErr(err)
		putJob(job)
		job = nil
	}
	return
}

func (p *FetchIoPipeline) doFetch(ctx context.Context, params fetchParams) (res any, err error) {
	job, err := p.doAsyncFetch(ctx, params)
	if err != nil {
		return
	}
	result := job.WaitDone()
	res, err = result.Res, result.Err
	putJob(job)
	return
}

// PrefetchIoPipeline

type PrefetchIoPipeline struct {
	IoPipelineImpl
	prefetchType int
	waitQ        sm.Queue
	prefetchFunc PrefetchFunc
}

func newPrefetchIoPipeline(pfType int, opts ...Option) *PrefetchIoPipeline {
	p := new(PrefetchIoPipeline)
	for _, opt := range opts {
		opt(&p.IoPipelineImpl)
	}
	p.prefetchType = pfType

	p.fillDefaults()

	p.waitQ = sm.NewSafeQueue(p.options.queueDepth, p.options.batchSize, p.onWait)

	// the prefetch queue is supposed to be an unblocking queue
	if pfType == MetaPrefetch {
		p.queue = sm.NewNonBlockingQueue(p.options.queueDepth, p.options.batchSize, p.onMetaPrefetch)
	} else {
		p.queue = sm.NewNonBlockingQueue(p.options.queueDepth, p.options.batchSize, p.onDataPrefetch)
	}

	p.scheduler = tasks.NewParallelJobScheduler(p.options.parallelism)
	p.prefetchFunc = noopPrefetch

	return p
}

func (p *PrefetchIoPipeline) start() {
	p.onceStart.Do(func() {
		p.active.Store(true)
		p.waitQ.Start()
		p.queue.Start()
	})
}

func (p *PrefetchIoPipeline) stop() {
	p.onceStop.Do(func() {
		p.active.Store(false)
		p.queue.Stop()
		p.scheduler.Stop()
		p.waitQ.Stop()
	})
}

func (p *PrefetchIoPipeline) doPrefetch(params PrefetchParams) (err error) {
	if _, err = p.queue.Enqueue(params); err == sm.ErrFull {
		p.stats.droppedItemStats.Add(1)
	}
	// prefetch doesn't care about what type of err has occurred
	return nil
}

func (p *PrefetchIoPipeline) onMetaPrefetch(items ...any) {
	if len(items) == 0 {
		return
	}
	if !p.active.Load() {
		return
	}

	for _, item := range items {
		job := prefetchMetaJob(context.Background(), item.(PrefetchParams))
		p.schedulerPrefetch(job)
	}
}

func (p *PrefetchIoPipeline) onDataPrefetch(items ...any) {
	if len(items) == 0 {
		return
	}
	if !p.active.Load() {
		return
	}

	processes := make([]PrefetchParams, 0)
	for _, item := range items {
		processes = append(processes, item.(PrefetchParams))
	}
	merged := mergePrefetch(processes)
	for _, option := range merged {
		job := prefetchJob(context.Background(), option)
		p.schedulerPrefetch(job)
	}
}

func (p *PrefetchIoPipeline) schedulerPrefetch(job *tasks.Job) {
	if err := p.scheduler.Schedule(job); err != nil {
		job.DoneWithErr(err)
		logutil.Debugf("err is %v", err.Error())
		putJob(job)
	} else {
		if _, err := p.waitQ.Enqueue(job); err != nil {
			job.DoneWithErr(err)
			logutil.Debugf("err is %v", err.Error())
			putJob(job)
		}
	}
}

func (p *PrefetchIoPipeline) onWait(jobs ...any) {
	for _, j := range jobs {
		job := j.(*tasks.Job)
		res := job.WaitDone()
		if res == nil {
			logutil.Infof("job is %v", job.String())
			putJob(job)
			return
		}
		if res.Err != nil {
			logutil.Warnf("Prefetch %s err: %s", job.ID(), res.Err)
		}
		putJob(job)
	}
}

type IoPipeline struct {
	fetch        *FetchIoPipeline
	metaPrefetch *PrefetchIoPipeline
	dataPrefetch *PrefetchIoPipeline

	stats struct {
		selectivityStats *objectio.Stats
	}

	printer *stopper.Stopper
}

func NewIoPipeline(opts ...Option) *IoPipeline {
	p := new(IoPipeline)

	p.fetch = newFetchIoPipeline(opts...)
	p.metaPrefetch = newPrefetchIoPipeline(MetaPrefetch, opts...)
	p.dataPrefetch = newPrefetchIoPipeline(DataPrefetch, opts...)

	p.stats.selectivityStats = objectio.NewStats()
	p.printer = stopper.NewStopper("IOPrinter")

	return p
}

func (p *IoPipeline) Start() {
	p.fetch.start()
	p.metaPrefetch.start()
	p.dataPrefetch.start()

	if err := p.printer.RunNamedTask("io-printer-job", p.crontask); err != nil {
		panic(err)
	}
}

func (p *IoPipeline) Stop() {
	p.fetch.stop()
	p.metaPrefetch.stop()
	p.dataPrefetch.stop()
	p.printer.Stop()
}

func (p *IoPipeline) Fetch(ctx context.Context, params fetchParams) (res any, err error) {
	return p.fetch.fetchFun(ctx, params)
}

func (p *IoPipeline) Prefetch(params PrefetchParams) (err error) {
	if len(params.ids) == 0 {
		return p.metaPrefetch.prefetchFunc(params)
	}

	return p.dataPrefetch.prefetchFunc(params)
}

func (p *IoPipeline) crontask(ctx context.Context) {
	hb := w.NewHeartBeaterWithFunc(time.Second*10, func() {
		logutil.Info(p.stats.selectivityStats.ExportString())
		wdrops := p.metaPrefetch.stats.droppedItemStats.SwapW(0)
		if wdrops > 0 {
			logutil.Infof("MetaPrefetchDropStats: %d", wdrops)
		}

		wdrops = p.dataPrefetch.stats.droppedItemStats.SwapW(0)
		if wdrops > 0 {
			logutil.Infof("DataPrefetchDropStats: %d", wdrops)
		}

		logutil.Info(objectio.ExportMetaCacheStats())
	}, nil)
	hb.Start()
	<-ctx.Done()
	hb.Stop()
}
