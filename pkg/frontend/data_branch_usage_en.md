# Data Branch Usage Guide (EN)

MatrixOne data branch statements provide branch-style diff/merge workflows for tables and databases. Current focus is on comparing and merging branches; creation/deletion and diff-to-table output are targeted for delivery in 1–2 weeks (see TODO).

## Existing Commands
- `DATA BRANCH DIFF <target_table> AGAINST <base_table> [OUTPUT FILE '<dir>' | OUTPUT LIMIT <n> | OUTPUT COUNT]`  
  Compare two tables (optionally at snapshots) and emit row-level changes relative to their LCA. Default result columns: `[branch, flag, col1, col2, ...]`, where `flag` is `INSERT`/`DELETE`/`UPDATE`.
- `DATA BRANCH MERGE <source_table> INTO <dest_table> [WHEN CONFLICT {FAIL|SKIP|ACCEPT}]`  
  Apply changes from source into destination using the diff engine. Conflict policy: `FAIL` (default), `SKIP` (ignore conflicting rows), `ACCEPT` (take source).

### Snapshot suffix on table names
Append `{snapshot="name"}` to bind a table to a snapshot, e.g. `t1{snapshot="sp1"}` or `db.tbl{snapshot='sp1'}`.

### Temporary equivalence note
`CREATE TABLE t2 CLONE t1 {snapshot="sp"}` currently acts like `DATA BRANCH CREATE TABLE ... FROM ... {snapshot="sp"}` until the dedicated create/delete statements land.

## Examples
- Diff with snapshots:  
  ```sql
  create table t1(a int primary key, b varchar(10));
  insert into t1 values (1,'1'),(2,'2'),(3,'3');
  create snapshot sp1 for table test t1;

  create table t2 like t1;
  insert into t2 values (1,'1'),(2,'2'),(4,'4');
  create snapshot sp2 for table test t2;

  data branch diff t2{snapshot="sp2"} against t1{snapshot="sp1"};
  ```
  Result (trimmed):
  ```
  diff t2 against t1  flag    a    b
  t1                  INSERT  3    3
  t2                  INSERT  4    4
  ```

- Merge with conflict handling:  
  ```sql
  create table t1 (a int, b int, primary key(a));
  insert into t1 values (1,1),(3,3),(5,5);
  create table t2 (a int, b int, primary key(a));
  insert into t2 values (1,1),(2,2),(4,4);

  data branch diff t2 against t1;
  data branch merge t2 into t1;  -- default FAIL, no conflict here
  select * from t1 order by a asc;
  ```
  Result after merge:
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
  Conflict policy variants:
  ```
  data branch merge t2 into t1 when conflict skip;
  data branch merge t2 into t1 when conflict accept;
  ```

- Diff to file and replay:  
  ```sql
  data branch diff branch_tbl against base_tbl output file '/tmp/diffs';
  ```
  Output cases:
  - Base table empty → CSV for initial sync:
    ```
    data branch diff t against t{snapshot="sp"} output file '/tmp/diffs/';
    +----------------------------------------------------------+-----------------------------------------------------------------------------------+
    | FILE SAVED TO                                            | HINT                                                                              |
    +----------------------------------------------------------+-----------------------------------------------------------------------------------+
    | /tmp/diffs/diff_t_t_sp_20251112_022036.csv               | FIELDS ENCLOSED BY '"' ESCAPED BY '\\' TERMINATED BY ',' LINES TERMINATED BY '\n' |
    +----------------------------------------------------------+-----------------------------------------------------------------------------------+
    ```
  - Base table non-empty → SQL diff to apply on destination:
    ```
    data branch diff t2 against t1 output file '/tmp/diffs/';
    +--------------------------------------------------------+-------------------------------------------+
    | FILE SAVED TO                                          | HINT                                      |
    +--------------------------------------------------------+-------------------------------------------+
    | /tmp/diffs/diff_t2_t1_20251112_022109.sql              | DELETE FROM test.t1, REPLACE INTO test.t1 |
    +--------------------------------------------------------+-------------------------------------------+
    ```

## TODO (ETA 1–2 weeks)
- `DATA BRANCH CREATE TABLE <new_table> FROM <base_table> [TO ACCOUNT ...]`
- `DATA BRANCH CREATE DATABASE <new_db> FROM <base_db> [...] [TO ACCOUNT ...]`
- `DATA BRANCH DELETE TABLE <table>`
- `DATA BRANCH DELETE DATABASE <db>`
- `DATA BRANCH DIFF ... OUTPUT AS <table>`
- While these land, use `CREATE TABLE ... CLONE ... {snapshot=...}` as a temporary replacement for creation semantics.
