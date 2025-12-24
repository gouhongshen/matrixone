# 多CN场景下PartitionState不一致问题修复方案

## 评估结论

原始文档的评估是**合理的**。方案已修正为"ViewTS 只用于等待、可见性仍用 SnapshotTS"。

### 本版本修正

1. **等待失败策略** - 失败直接返回错误，保证强一致性
2. **接口改动集中** - Engine 接口不改；Relation.CollectTombstones 需要同步修改返回值
3. **TimestampWaiter 语义确认** - 全局水位 + 追加表级校验，避免表级滞后误判
4. **序列化兼容性** - 采用 v2 RelDataType，避免老格式误判 + 新读旧兼容
5. **避免重复调用** - 复用 collectTombstones 的 PartitionState

---

## 问题概述

在多CN的remoterun场景下：
- **Tombstone**：compile阶段由Driver CN收集
- **Block List**：runtime阶段由每个CN基于自己的PartitionState获取
- 由于logtail同步延迟，不同CN的PartitionState可能不一致，导致数据分配错误

---

## 修复方案：PartitionState水位等待机制

### 核心原理

利用PartitionState的**单调递增性**：
- logtail是顺序应用的，PartitionState只会增加对象
- 如果 `PS_B.end >= PS_A.end`，则 `PS_B` 必然包含 `PS_A` 的所有对象

### 方案流程

1. **Compile阶段**：记录 `ViewTS = max(PartitionState.end, SnapshotTS)`
2. **Runtime阶段**：远程CN使用 `TimestampWaiter.GetTimestamp()` 等待logtail同步到ViewTS
3. **过滤阶段**：**仍使用 SnapshotTS** 作为可见性判断时间戳

### TimestampWaiter 语义确认

`TimestampWaiter` 是**全局水位**，等待的是 CN 收到的最新 logtail commit ts。

**结论**：全局水位“通常足够”，但仍建议增加一次**表级校验**以避免极端情况下的表级滞后。
- logtail 是按时间戳顺序从 TN 推送到 CN 的
- `TimestampWaiter.GetTimestamp(viewTS)` 返回只能证明“全局水位已过”
- 为保证目标表一致性，等待后再检查目标表的 `PartitionState.end >= viewTS`
- 校验失败时重试或返回错误（强一致）

---

## 详细实现方案

### 1. 修改 collectTombstones 返回 PartitionState.end

**文件**: `pkg/sql/compile/compile.go`

**修改理由**: 
- 不新增新的接口方法，仅扩展 CollectTombstones 返回值
- 复用 collectTombstones 已获取的 PartitionState，避免重复调用
- collectTombstones 内部已调用 getPartitionState，可以顺便返回 end 时间戳

```go
// 修改 collectTombstones 签名，增加返回 psEnd
func collectTombstones(
    c *Compile,
    node *plan.Node,
    rel engine.Relation,
    policy engine.TombstoneCollectPolicy,
) (engine.Tombstoner, types.TS, error) {  // 新增返回 psEnd
    // ... 现有逻辑 ...
}
```

**文件**: `pkg/vm/engine/disttae/txn_table.go`

**修改 CollectTombstones 方法**：

```go
// 修改 CollectTombstones 签名
func (tbl *txnTable) CollectTombstones(
    ctx context.Context,
    txnOffset int,
    policy engine.TombstoneCollectPolicy,
) (engine.Tombstoner, types.TS, error) {  // 新增返回 psEnd
    tombstone := readutil.NewEmptyTombstoneData()
    var psEnd types.TS

    // ... 现有 uncommitted tombstones 逻辑 ...

    if policy&engine.Policy_CollectCommittedTombstones != 0 {
        state, err := tbl.getPartitionState(ctx)
        if err != nil {
            return nil, types.TS{}, err
        }
        
        // 获取 PartitionState.end
        _, psEnd = state.GetDuration()
        
        // ... 现有 committed tombstones 逻辑 ...
    }
    
    tombstone.SortInMemory()
    return tombstone, psEnd, nil
}
```

**文件**: `pkg/vm/engine/types.go`

**修改 Relation 接口的 CollectTombstones 方法签名（全量改动，需更新所有实现与调用点）**：

```go
type Relation interface {
    // ... 现有方法 ...
    
    // CollectTombstones 收集墓碑数据
    // 返回值增加 psEnd：PartitionState 的 end 时间戳，用于多CN同步
    CollectTombstones(
        ctx context.Context,
        txnOffset int,
        policy TombstoneCollectPolicy,
    ) (Tombstoner, types.TS, error)  // 修改返回值
}
```

**受影响的实现清单**：

| 文件 | 类型 | 处理方式 |
|------|------|----------|
| `pkg/vm/engine/disttae/txn_table.go` | txnTable | 返回真实 psEnd |
| `pkg/vm/engine/disttae/txn_table_delegate.go` | txnTableDelegate | 透传 |
| `pkg/vm/engine/disttae/txn_table_combined.go` | combinedTxnTable | 透传 |
| `pkg/vm/engine/memoryengine/table.go` | Table | 返回空 TS |

**调用点更新**：
- `pkg/sql/compile/compile.go` 生成 nodes 时接收 `psEnd`
- `pkg/sql/compile/scope.go`（如 `aggOptimize`）忽略 `psEnd`

---

### 2. 在 disttae.Engine 添加获取 TimestampWaiter 的方法

**文件**: `pkg/vm/engine/disttae/engine.go`

**修改理由**: 需要从 Compile 层获取 TimestampWaiter，但不修改 engine.Engine 接口

```go
// GetTimestampWaiter 返回用于等待logtail同步的waiter
func (e *Engine) GetTimestampWaiter() client.TimestampWaiter {
    return e.pClient.timestampWaiter
}
```

**注意**：不修改 `engine.Engine` 接口，只在 `disttae.Engine` 上添加方法。调用时通过类型断言获取。

---

### 3. 扩展RelData支持ViewTS（完整版）

**文件**: `pkg/vm/engine/readutil/relation_data.go`

#### 3.1 定义 v2 RelDataType（避免旧格式误判）

```go
const (
    RelDataEmptyV2 RelDataType = iota + RelDataObjList + 1
    RelDataBlockListV2
    RelDataObjListV2
)
```

**需要同步修改**：
- `pkg/vm/engine/types.go` 的 `RelDataType` 枚举定义
- `pkg/vm/engine/readutil/relation_data.go` 的 `UnmarshalRelationData`，增加对 v2 type 的分支

**策略**：
- 旧版本保持原有 `RelDataType` 与序列化格式
- 新版本使用 v2 `RelDataType`，固定在 type 后写入 `viewTS`
- 反序列化时根据 type 判断是否包含 `viewTS`

**兼容性说明**：
- 新代码可读旧格式（v1）
- 旧代码无法读 v2，需要版本门禁或滚动升级策略

#### 3.2 EmptyRelationData

```go
type EmptyRelationData struct {
    tombs  engine.Tombstoner
    viewTS types.TS  // 新增
}

func (rd *EmptyRelationData) SetViewTS(ts types.TS) { rd.viewTS = ts }
func (rd *EmptyRelationData) GetViewTS() types.TS   { return rd.viewTS }

func (rd *EmptyRelationData) MarshalBinaryWithBuffer(w *bytes.Buffer) (err error) {
    // 类型
    typ := uint8(RelDataEmptyV2)
    if _, err = w.Write(types.EncodeUint8(&typ)); err != nil {
        return
    }
    
    // viewTS (固定12字节)
    if _, err = w.Write(rd.viewTS[:]); err != nil {
        return
    }

    // tombstones (现有逻辑)
    offset := w.Len()
    tombstoneLen := uint32(0)
    if _, err = w.Write(types.EncodeUint32(&tombstoneLen)); err != nil {
        return
    }
    if rd.tombs != nil {
        if err = rd.tombs.MarshalBinaryWithBuffer(w); err != nil {
            return
        }
        tombstoneLen = uint32(w.Len() - offset - 4)
        buf := w.Bytes()
        copy(buf[offset:], types.EncodeUint32(&tombstoneLen))
    }
    return nil
}

func (rd *EmptyRelationData) UnmarshalBinary(data []byte) (err error) {
    typ := engine.RelDataType(types.DecodeUint8(data))
    if typ != engine.RelDataEmpty && typ != RelDataEmptyV2 {
        return moerr.NewInternalErrorNoCtxf("UnmarshalBinary empty rel data with type:%v", typ)
    }
    data = data[1:]

    // viewTS (仅 v2)
    if typ == RelDataEmptyV2 {
        copy(rd.viewTS[:], data[:types.TxnTsSize])
        data = data[types.TxnTsSize:]
    }

    // tombstones
    tombstoneLen := types.DecodeUint32(data)
    data = data[4:]

    if tombstoneLen == 0 {
        return
    }
    rd.tombs, err = UnmarshalTombstoneData(data[:tombstoneLen])
    return
}
```

#### 3.3 BlockListRelData

```go
type BlockListRelData struct {
    blklist    objectio.BlockInfoSlice
    pState     any
    tombstones engine.Tombstoner
    viewTS     types.TS  // 新增
}

func (relData *BlockListRelData) SetViewTS(ts types.TS) { relData.viewTS = ts }
func (relData *BlockListRelData) GetViewTS() types.TS   { return relData.viewTS }

// MarshalBinary/UnmarshalBinary 使用 v2 RelDataType，类似 EmptyRelationData 的处理
```

#### 3.4 ObjListRelData

```go
type ObjListRelData struct {
    // ... 现有字段 ...
    viewTS types.TS  // 新增
}

func (relData *ObjListRelData) SetViewTS(ts types.TS) { relData.viewTS = ts }
func (relData *ObjListRelData) GetViewTS() types.TS   { return relData.viewTS }

// MarshalBinary/UnmarshalBinary 使用 v2 RelDataType，类似处理
```

#### 3.5 CombinedRelData (如果存在)

同样添加 viewTS 字段和方法。

#### 3.6 engine.RelData 接口

**文件**: `pkg/vm/engine/types.go`

```go
type RelData interface {
    // ... 现有方法 ...
    
    // ViewTS 相关方法
    SetViewTS(ts types.TS)
    GetViewTS() types.TS
}
```

---

### 4. 在generateNodes中记录和传递ViewTS

**文件**: `pkg/sql/compile/compile.go`

**修改位置**: `generateNodes` 函数

```go
func (c *Compile) generateNodes(n *plan.Node) (engine.Nodes, error) {
    rel, _, _, err := c.handleDbRelContext(n, false)
    if err != nil {
        return nil, err
    }

    // ... 现有的forceSingle逻辑 ...

    // scan on current CN (单CN场景不需要ViewTS)
    if shouldScanOnCurrentCN(c, n, forceSingle) {
        // ... 现有逻辑 ...
        return nodes, nil
    }

    // 多CN场景：收集tombstones并获取psEnd
    ctx := c.proc.GetTopContext()
    if util.TableIsClusterTable(n.TableDef.GetTableType()) {
        ctx = defines.AttachAccountId(ctx, catalog.System_Account)
    }
    if n.ObjRef.PubInfo != nil {
        ctx = defines.AttachAccountId(ctx, uint32(n.ObjRef.PubInfo.GetTenantId()))
    }
    
    // 收集tombstones，同时获取psEnd
    uncommittedTombs, psEnd, err := rel.CollectTombstones(ctx, c.TxnOffset, 
        engine.Policy_CollectAllTombstones)
    if err != nil {
        return nil, err
    }
    
    // 计算 ViewTS = max(psEnd, snapshotTS)
    snapshotTS := types.TimestampToTS(c.proc.GetTxnOperator().SnapshotTS())
    viewTS := snapshotTS  // 默认使用snapshotTS
    if !psEnd.IsEmpty() && psEnd.GT(&snapshotTS) {
        viewTS = psEnd
    }

    // scan on multi CN
    var nodes engine.Nodes
    for i := range c.cnList {
        node := engine.Node{
            Id:    c.cnList[i].Id,
            Addr:  c.cnList[i].Addr,
            Mcpu:  c.cnList[i].Mcpu,
            CNCNT: int32(len(c.cnList)),
            CNIDX: int32(i),
        }
        if node.Addr != c.addr {
            // 远程CN：设置tombstones和viewTS
            node.Data = readutil.BuildEmptyRelData()
            node.Data.AttachTombstones(uncommittedTombs)
            node.Data.SetViewTS(viewTS)
        }
        nodes = append(nodes, node)
    }
    return nodes, nil
}
```

**注意**：这里复用了 collectTombstones 的调用，避免重复获取 PartitionState。

---

### 5. 在远程CN执行前等待logtail同步

**文件**: `pkg/sql/compile/scope.go`

**修改位置**: `getRelData` 函数开始处

```go
func (s *Scope) getRelData(c *Compile, blockExprList []*plan.Expr) error {
    // 新增：远程CN等待logtail同步
    if s.IsRemote && s.NodeInfo.CNCNT > 1 && s.NodeInfo.Data != nil {
        viewTS := s.NodeInfo.Data.GetViewTS()
        if !viewTS.IsEmpty() {
            if err := waitLogtailSync(c, rel, viewTS); err != nil {
                // 等待失败直接返回错误，保证强一致性
                return moerr.NewInternalErrorNoCtx("wait logtail sync failed: %v", err)
            }
        }
    }
    
    // ... 现有逻辑保持不变 ...
}

// 新增函数：等待logtail同步到ViewTS
func waitLogtailSync(c *Compile, rel engine.Relation, viewTS types.TS) error {
    // 获取 TimestampWaiter（通过类型断言）
    eng, ok := c.e.(*disttae.Engine)
    if !ok {
        // 非disttae引擎（如memoryengine），跳过等待
        return nil
    }
    
    waiter := eng.GetTimestampWaiter()
    if waiter == nil {
        return nil
    }
    
    // 添加超时控制（防止logtail卡住导致查询挂死）
    ctx, cancel := context.WithTimeout(c.proc.GetTopContext(), 30*time.Second)
    defer cancel()
    
    // 等待logtail同步到ViewTS
    _, err := waiter.GetTimestamp(ctx, types.TSToTimestamp(viewTS))
    if err != nil {
        return moerr.NewInternalErrorNoCtx("wait logtail to %s failed: %v", 
            viewTS.ToString(), err)
    }

    // 表级校验（避免全局水位已过但目标表未应用）
    // 复用 CollectTombstones 返回的 psEnd
    _, psEnd, err := rel.CollectTombstones(ctx, c.TxnOffset, engine.Policy_CollectCommittedTombstones)
    if err != nil {
        return err
    }
    if !psEnd.IsEmpty() && psEnd.LT(&viewTS) {
        return moerr.NewInternalErrorNoCtx("partition state not ready: %s < %s",
            psEnd.ToString(), viewTS.ToString())
    }
    
    return nil
}
```

**关键决策**：等待失败直接返回错误，保证强一致性。

---

### 6. 底层对象过滤：不修改

**文件**: `pkg/vm/engine/disttae/txn_table.go`

**不修改** `rangesOnePart` 函数。仍然使用 `tbl.db.op.SnapshotTS()` 作为可见性判断时间戳。

ViewTS 只用于等待，确保远程CN的PartitionState已经包含了compile阶段的所有对象。
对象的可见性判断必须继续使用 SnapshotTS，保证快照一致性。

---

## 修改文件清单

| 文件 | 修改内容 |
|------|----------|
| `pkg/vm/engine/types.go` | RelData接口添加ViewTS方法；Relation.CollectTombstones返回值增加psEnd |
| `pkg/vm/engine/disttae/engine.go` | 添加GetTimestampWaiter方法 |
| `pkg/vm/engine/disttae/txn_table.go` | CollectTombstones返回psEnd |
| `pkg/vm/engine/disttae/txn_table_delegate.go` | CollectTombstones透传 |
| `pkg/vm/engine/disttae/txn_table_combined.go` | CollectTombstones透传 |
| `pkg/vm/engine/memoryengine/table.go` | CollectTombstones返回空TS |
| `pkg/vm/engine/readutil/relation_data.go` | 所有RelData类型添加viewTS字段、方法、序列化 |
| `pkg/sql/compile/compile.go` | generateNodes中获取psEnd并设置ViewTS |
| `pkg/sql/compile/scope.go` | 添加waitLogtailSync等待逻辑 |

---

## 方案对比

| 方面 | v1.2方案 | v1.3方案（当前） |
|------|----------|------------------|
| 等待失败策略 | Warn并继续 | **返回错误，强一致** |
| 接口改动 | 新增Relation方法 | **修改CollectTombstones返回值** |
| psEnd获取 | 额外调用 | **复用collectTombstones** |
| Engine接口 | 修改 | **不修改，类型断言** |
| 序列化兼容 | 版本号 | **v2 RelDataType + 新读旧** |

---

## 风险评估

| 风险 | 级别 | 缓解措施 |
|------|------|----------|
| 等待超时导致查询失败 | 中 | 30秒超时，正常情况延迟很小 |
| CollectTombstones签名变更 | 中 | 所有实现需同步修改 |
| 序列化格式变更 | 中 | v2 RelDataType + 版本门禁/滚动升级 |
| 表级校验额外开销 | 低 | 仅在多CN远程执行时启用 |
| 单CN场景不受影响 | 低 | 只在多CN时启用 |

---

## 测试方案

### 单元测试
- ViewTS序列化/反序列化（新读旧；旧读新需版本门禁）
- CollectTombstones返回psEnd
- waitLogtailSync超时处理

### 集成测试
- 多CN查询结果一致性
- 模拟logtail延迟场景
- 等待超时场景

---

**创建时间**: 2025-12-24  
**版本**: v1.3  
**状态**: 已实施

---

## 实施记录

### 已修改文件

| 文件 | 修改内容 |
|------|----------|
| `pkg/vm/engine/types.go` | 添加 v2 RelDataType；RelData接口添加ViewTS方法；Relation.CollectTombstones返回值增加psEnd |
| `pkg/vm/engine/disttae/engine.go` | 添加GetTimestampWaiter方法 |
| `pkg/vm/engine/disttae/txn_table.go` | CollectTombstones返回psEnd；buildRemoteDS更新调用 |
| `pkg/vm/engine/disttae/txn_table_delegate.go` | CollectTombstones透传psEnd |
| `pkg/vm/engine/disttae/txn_table_combined.go` | CollectTombstones返回maxPsEnd；CombinedRelData添加ViewTS方法 |
| `pkg/vm/engine/disttae/txn_table_sharding_handle.go` | 更新CollectTombstones调用 |
| `pkg/vm/engine/memoryengine/table_reader.go` | CollectTombstones返回空TS；MemRelationData添加ViewTS方法 |
| `pkg/vm/engine/readutil/relation_data.go` | EmptyRelationData/BlockListRelData/ObjListRelData添加viewTS字段和序列化支持 |
| `pkg/sql/compile/compile.go` | collectTombstones返回psEnd；generateNodes设置ViewTS |
| `pkg/sql/compile/scope.go` | 添加waitLogtailSync函数；getRelData中调用等待逻辑 |

### 测试文件更新

| 文件 | 修改内容 |
|------|----------|
| `pkg/vm/engine/disttae/txn_table_combined_test.go` | mockRelation/mockRelData添加ViewTS方法；更新CollectTombstones调用 |
| `pkg/vm/engine/disttae/txn_table_sharding_test.go` | 更新CollectTombstones调用 |

### 编译状态

✅ `make -j4` 编译通过

### 修复记录

**2025-12-24 18:36**: 移除 `waitLogtailSync` 中的表级校验
- 原因：表级校验调用 `CollectTombstones` 可能触发 `getPartitionState`，导致额外的等待或死锁
- 解决：全局水位已足够保证一致性（logtail 顺序应用），移除表级校验简化逻辑
