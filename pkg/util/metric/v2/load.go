// Copyright 2024 Matrix Origin
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

package v2

import "github.com/prometheus/client_golang/prometheus"

var (
	LoadDataBatchRowsHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "load",
			Name:      "batch_rows",
			Help:      "Bucketed histogram of rows per LOAD DATA batch.",
			Buckets:   prometheus.ExponentialBuckets(256, 2, 12),
		}, []string{"format", "parallel_load"})

	LoadDataBatchBytesEstimateHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "load",
			Name:      "batch_bytes_estimate",
			Help:      "Estimated bytes per LOAD DATA batch.",
			Buckets:   prometheus.ExponentialBuckets(64*1024, 2, 16),
		}, []string{"format", "parallel_load"})

	LoadDataBatchBytesHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "load",
			Name:      "batch_bytes",
			Help:      "Actual bytes per LOAD DATA batch.",
			Buckets:   prometheus.ExponentialBuckets(64*1024, 2, 16),
		}, []string{"format", "parallel_load"})

	LoadDataBatchParseDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "load",
			Name:      "batch_parse_duration_seconds",
			Help:      "Bucketed histogram of LOAD DATA batch parse duration.",
			Buckets:   getDurationBuckets(),
		}, []string{"format", "parallel_load"})

	LoadDataBatchMPoolInuseBytesGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "load",
			Name:      "batch_mpool_inuse_bytes",
			Help:      "Current mpool in-use bytes when finishing a LOAD DATA batch.",
		}, []string{"format", "parallel_load"})

	LoadDataFileOffsetDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "load",
			Name:      "file_offset_duration_seconds",
			Help:      "Bucketed histogram of file offset calculation duration.",
			Buckets:   getDurationBuckets(),
		}, []string{"format", "mode"})

	LoadDataParallelStrategyCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "load",
			Name:      "parallel_strategy_total",
			Help:      "Count of LOAD DATA parallel strategies selected.",
		}, []string{"format", "read_parallel", "write_parallel"})
)

func initLoadMetrics() {
	registry.MustRegister(LoadDataBatchRowsHistogram)
	registry.MustRegister(LoadDataBatchBytesEstimateHistogram)
	registry.MustRegister(LoadDataBatchBytesHistogram)
	registry.MustRegister(LoadDataBatchParseDurationHistogram)
	registry.MustRegister(LoadDataBatchMPoolInuseBytesGauge)
	registry.MustRegister(LoadDataFileOffsetDurationHistogram)
	registry.MustRegister(LoadDataParallelStrategyCounter)
}
