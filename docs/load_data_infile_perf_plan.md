# MatrixOne LOAD DATA INFILE 性能/内存分析与落地优化方案

## 范围与结论概览
- 重点代码路径集中在 `pkg/sql/plan/build_load.go`, `pkg/sql/compile/compile.go`, `pkg/sql/colexec/external/external.go`, `pkg/sql/colexec/external/types.go`, `pkg/sql/util/csvparser/csv_parser.go`, `pkg/sql/colexec/s3util.go`, `pkg/frontend/mysql_buffer.go`。
- 4G 文件偶发 OOM 的核心原因是：字符串化批量 + 真实内存估算偏小 + 并发写入内存阈值叠加。
- 1T 并行加载耗时 1h+ 的核心原因是：本地/压缩文件不支持并行读、严格模式下 offset 计算成本高、字符串->类型转换重复且耗 CPU。

## 现状链路（LOAD DATA INFILE）
1) 语句规划：
   - 并行策略与压缩/LOCAL 限制：`pkg/sql/plan/build_load.go`。
   - 并行时为压缩或 LOCAL 插入 cast：`projectNode.ProjectList = makeCastExpr(...)`。
2) 编译执行：
   - 并行读写判断：`getReadWriteParallelFlag` in `pkg/sql/compile/compile.go`。
   - 并行读写：`compileExternScanParallelReadWrite`。
   - 仅并行写（单 reader）：`compileExternScanParallelWrite`，并设置 `extern.Es.ParallelLoad = true`。
3) 外部读取：
   - CSV 读取与 batch 组装：`scanCsvFile`/`makeBatchRows` in `pkg/sql/colexec/external/external.go`。
   - `ParallelLoad` 时所有列使用 `varchar`：`makeType` in `pkg/sql/colexec/external/external.go`。
4) 写入：
   - 写 S3 的内存阈值：`WriteS3Threshold` in `pkg/sql/colexec/s3util.go`。

## 为什么 4G LOAD 会 OOM（主要根因）
1) `ParallelLoad` 把所有列强制为 `varchar`（`makeType` in `pkg/sql/colexec/external/external.go`），数值/时间类型被字符串化：
   - 每个字段都会发生 `string -> []byte` 拷贝（`getColData`），内存占用显著放大。
2) batch 大小估算偏小：
   - `makeBatchRows` 里的 `curBatchSize` 只累加字段字符串长度，未计入 vector 元数据、null 位图、复制开销。
   - `maxBatchSize` 使用 `0.6 * MaxMsgSize`（默认 60MB），真实内存峰值可能 >2x。
3) 压缩/LOCAL 并行时的双重物化：
   - 外部读取产出 `varchar`，`projectNode` 再做 cast，导致同批次同时存在字符串与目标类型向量，峰值翻倍。
4) 多 writer 的内存阈值叠加：
   - `CNS3Writer` 默认 128MB 内存阈值 * 并发写入数，易在高并发时把内存顶满。
5) LOCAL 模式下的大包缓存：
   - `ReadLoadLocalPacket` 为每个大包分配临时 buffer（`pkg/frontend/mysql_buffer.go`），客户端 `max_allowed_packet` 较大时显著放大内存峰值。

## 为什么 1T 并行 LOAD 仍要 1h+
1) LOCAL/压缩文件禁用并行读：
   - `getReadWriteParallelFlag` 对 `param.Local` 或压缩文件强制 `readParallel=false`（`pkg/sql/compile/compile.go`），只有单 reader，吞吐上限低。
2) 严格模式下 offset 计算成本高：
   - `ReadFileOffset` + `getTailSizeStrict` 需解析 CSV 直到找到合法行，1T 文件分片多时额外开销明显。
3) 重复解析/转换：
   - 压缩/LOCAL 并行路径先读为字符串，再 cast 为目标类型，CPU/内存都被重复消耗。
4) CSV 读取块偏小：
   - `csvparser.ReadBlockSize = 64KB`，大文件顺序读取时 syscall 与解析开销较高。

## 可提升的性能与内存优化点
- 批次大小改为基于 `batch.Size()` 或 `mpool` 实际使用量控制，避免仅用字符串长度估算。
- 把 `BatchSizeInLoadData`、`LoadDataConcurrencyCount` 这两个已存在但未生效的配置真正接入执行逻辑。
- 压缩/LOCAL 并行场景避免“字符串化 + cast”的双物化，改为一次性类型转换。
- offset 计算改为更轻量的行边界扫描，或改为每个 reader 自己跳过首行对齐。
- CSV 读取块大小可配置，提高顺序读吞吐。
- S3 writer 的内存阈值与并发写入数联动，避免阈值叠加引发峰值过高。

## 可落地实施方案（分阶段）

### 阶段 0：可观测性补齐（1-2 天）
目标：先把“内存占用/解析耗时”量化，为后续优化提供数据支撑。
- 在 `pkg/sql/colexec/external/external.go` 增加以下统计：
  - 每批 `bat.Size()`、`curBatchSize`、`bat.RowCount()`。
  - `makeBatchRows` 的解析耗时。
- 在 `pkg/sql/plan/build_load.go` 打印并行策略决策结果（是否并行读/写）。
- 期望：能够定位 OOM 时的单批内存峰值与并发数。

### 阶段 1：内存上限与并发上限（3-5 天）
目标：把峰值内存控制在可预期范围内。
1) 使用真实内存估算控制 batch：
   - 修改 `makeBatchRows`：以 `bat.Size()` 或 `mpool` 增量为判断条件，超过 `maxBatchSize` 立即停止。
2) 将现有配置接入实际逻辑：
   - `BatchSizeInLoadData` -> 覆盖 `OneBatchMaxRow`（`pkg/sql/colexec/external/external.go`）。
   - `LoadDataConcurrencyCount` -> 限制 `parallelSize` 上限（`pkg/sql/compile/compile.go`）。
3) S3 writer 内存阈值动态化：
   - `memoryThreshold = min(WriteS3Threshold, totalBudget / writerCount)`。
   - `totalBudget` 可来自 `proc.GetLim().Size` 或新增配置。
4) LOCAL 大包限制：
   - 在 `ReadLoadLocalPacket` 增加最大包大小检查，超过则拒绝或分块（需要客户端配合）。

### 阶段 2：避免双物化（3-5 天）
目标：减少字符串化带来的 CPU/内存浪费。
- 对“并行 + 压缩/LOCAL”路径：
  1) 将 `compileExternScanParallelWrite` 中的 `extern.Es.ParallelLoad=true` 改为按需关闭。
  2) 同时在 `build_load.go` 中仅在确实需要时插入 `makeCastExpr`，避免无意义 cast。
  3) 若必须 cast，考虑将 cast 下推到 `external` 内部，直接产出目标类型向量。
- 期望：同一批次只保留一次物化，峰值内存下降 >30%。

### 阶段 3：并行读优化（5-8 天）
目标：提高大文件吞吐，特别是 1T+。
1) offset 计算轻量化：
   - 取消 `ReadFileOffset` 的全局解析；每个 reader 自己 seek 到起始偏移后跳到下一行。
   - 严格模式可增加“轻量状态机（引号/转义）”保证行边界正确。
2) CSV 读取块可配置：
   - 允许配置 `csvparser.ReadBlockSize`（如 1MB），提高顺序读效率。
3) 并行度动态调节：
   - `parallelSize = min(config.LoadDataConcurrencyCount, cpu, fileSize/threshold)`。

### 阶段 4：运行侧最佳实践（可立即执行）
目标：无需改代码即可显著提速。
- 对 1T 级别文件，优先拆分成多个 4~16GB 子文件并行加载。
- 压缩文件（gzip）不支持并行读，优先改为不压缩或可切分压缩格式（如 parquet/zstd）。
- 大字段/宽表场景，适当降低 batch 行数或 batch 字节数，避免单批过大。

## 验收指标
- 4G CSV 在 8C/32G 环境下稳定完成，峰值内存 < 60% 物理内存。
- 1T CSV 在并行读写开启后，吞吐提升 >= 2x（或 1h+ 降到 30min 以内）。
- 压缩/LOCAL 场景下的双物化消除后，CPU 使用降低 >= 20%。

## 风险与回滚
- 并行度和 batch 调整可能影响吞吐与稳定性，需要灰度开关。
- offset 计算改动涉及 CSV 兼容性，需要保留严格模式回退路径。
- 建议所有改动都引入配置开关，方便回滚与A/B测试。
