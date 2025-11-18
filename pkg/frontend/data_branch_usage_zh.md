# Data Branch 使用指南（中文）

MatrixOne 的 Data Branch 目前聚焦于表/库的差异比对与合并；创建/删除以及 diff 落表输出计划在 1–2 周内交付（见 TODO）。

## 现存命令
- `DATA BRANCH DIFF <目标表> AGAINST <基表> [OUTPUT FILE '<目录>' | OUTPUT LIMIT <数字> | OUTPUT COUNT]`  
  对两表（可指定快照）求行级差异，结果列为 `[分支, flag, col1, col2, ...]`，`flag` 为 `INSERT`/`DELETE`/`UPDATE`。
- `DATA BRANCH MERGE <源表> INTO <目标表> [WHEN CONFLICT {FAIL|SKIP|ACCEPT}]`  
  基于 diff 将源表变更合并到目标表；冲突策略：`FAIL`（默认）、`SKIP`（忽略冲突行）、`ACCEPT`（源表优先）。

### 表名快照后缀
给表名追加 `{snapshot="name"}` 指定快照视图，例如 `t1{snapshot="sp1"}`、`db.tbl{snapshot='sp1'}`。

### 临时等价说明
专用创建/删除语句上线前，`CREATE TABLE t2 CLONE t1 {snapshot="sp"}` 等价于 `DATA BRANCH CREATE TABLE ... FROM ... {snapshot="sp"}`。

## 示例
- 带快照的差异比对（源自 `diff_1.sql`）：  
  ```sql
  create table t1(a int primary key, b varchar(10));
  insert into t1 values (1,'1'),(2,'2'),(3,'3');
  create snapshot sp1 for table test t1;

  create table t2 like t1;
  insert into t2 values (1,'1'),(2,'2'),(4,'4');
  create snapshot sp2 for table test t2;

  data branch diff t2{snapshot="sp2"} against t1{snapshot="sp1"};
  ```
  输出（截取）：
  ```
  diff t2 against t1  flag    a    b
  t1                  INSERT  3    3
  t2                  INSERT  4    4
  ```

- 合并与冲突策略（源自 `merge_1.sql` case 1.i）：  
  ```sql
  create table t1 (a int, b int, primary key(a));
  insert into t1 values (1,1),(3,3),(5,5);
  create table t2 (a int, b int, primary key(a));
  insert into t2 values (1,1),(2,2),(4,4);

  data branch diff t2 against t1;
  data branch merge t2 into t1;
  select * from t1 order by a asc;
  ```
  合并后结果：
  ```
  +---+---+
  | a | b |
  +---+---+
  | 1 | 1 |
  | 2 | 2 |
  | 3 | 3 |
  | 4 | 4 |
  | 5 | 5 |
  +---+---+
  ```
  冲突策略示例（源自 `merge_2.sql`）：
  ```
  data branch merge t2 into t1 when conflict skip;
  data branch merge t2 into t1 when conflict accept;
  ```

- 输出 diff 文件并回放（源自 `TestDataBranchDiffAsFile`）：  
  ```sql
  data branch diff branch_tbl against base_tbl output file '/tmp/diffs';
  ```
  输出场景：
  - 基表为空 → 生成 CSV 便于初次全量同步：
    ```
    data branch diff t against t{snapshot="sp"} output file '/tmp/diffs/';
    +----------------------------------------------------------+--------------------------------------------------------------------+
    | FILE SAVED TO                                            | HINT                                                               |
    +----------------------------------------------------------+--------------------------------------------------------------------+
    | /tmp/diffs/diff_t_t_sp_20251112_022036.csv               | FIELDS ENCLOSED BY '"' ESCAPED BY '\\' TERMINATED BY ',' LINES TERMINATED BY '\n' |
    +----------------------------------------------------------+--------------------------------------------------------------------+
    ```
  - 基表非空 → 生成 SQL 文件可直接对齐目标表：
    ```
    data branch diff t2 against t1 output file '/tmp/diffs/';
    +--------------------------------------------------------+-------------------------------------------+
    | FILE SAVED TO                                          | HINT                                      |
    +--------------------------------------------------------+-------------------------------------------+
    | /tmp/diffs/diff_t2_t1_20251112_022109.sql              | DELETE FROM test.t1, REPLACE INTO test.t1 |
    +--------------------------------------------------------+-------------------------------------------+
    ```

## TODO（周期：1–2 周）
- `DATA BRANCH CREATE TABLE <新表> FROM <基表> [TO ACCOUNT ...]`
- `DATA BRANCH CREATE DATABASE <新库> FROM <基库> [...] [TO ACCOUNT ...]`
- `DATA BRANCH DELETE TABLE <表名>`
- `DATA BRANCH DELETE DATABASE <库名>`
- `DATA BRANCH DIFF ... OUTPUT AS <表名>`
- 上述能力上线前，可用 `CREATE TABLE ... CLONE ... {snapshot=...}` 临时替代创建语义。
