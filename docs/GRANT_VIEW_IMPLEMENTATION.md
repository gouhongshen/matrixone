# Feature 实施方案：支持 GRANT/REVOKE 权限到 VIEW

## Issue 链接
https://github.com/matrixorigin/matrixone/issues/23232

## 问题描述

当前执行以下 SQL 会报错：
```sql
grant select on view ci_store_id_1 to store_manager;
-- ERROR 20101 (HY000): internal error: the object type "Unknown ObjectType" is unsupported
```

## 根因分析

### 1. 语法解析层（已支持）
文件：`pkg/sql/parsers/tree/revoke.go` 和 `pkg/sql/parsers/dialect/mysql/mysql_sql.go`

语法解析器已经支持 `OBJECT_TYPE_VIEW`（第 237 行），并且在 yacc 文件中也有对应的解析规则（第 12724 行）。

### 2. 问题所在：`ObjectType.String()` 方法缺少 VIEW 处理

文件：`pkg/sql/parsers/tree/revoke.go`，第 214-228 行

```go
func (node *ObjectType) String() string {
    switch *node {
    case OBJECT_TYPE_TABLE:
        return "table"
    case OBJECT_TYPE_FUNCTION:
        return "function"
    case OBJECT_TYPE_PROCEDURE:
        return "procedure"
    case OBJECT_TYPE_ACCOUNT:
        return "account"
    case OBJECT_TYPE_DATABASE:
        return "database"
    default:
        return "Unknown ObjectType"  // <-- VIEW 走到这里
    }
}
```

### 3. 问题所在：`convertAstObjectTypeToObjectType` 函数缺少 VIEW 处理

文件：`pkg/frontend/authenticate.go`，第 4886-4901 行

```go
func convertAstObjectTypeToObjectType(ctx context.Context, ot tree.ObjectType) (objectType, error) {
    var objType objectType
    switch ot {
    case tree.OBJECT_TYPE_TABLE:
        objType = objectTypeTable
    case tree.OBJECT_TYPE_DATABASE:
        objType = objectTypeDatabase
    case tree.OBJECT_TYPE_ACCOUNT:
        objType = objectTypeAccount
    default:
        return 0, moerr.NewInternalErrorf(ctx, `the object type "%s" is unsupported`, ot.String())
    }
    return objType, nil
}
```

### 4. 问题所在：`checkPrivilegeObjectTypeAndPrivilegeLevel` 函数缺少 VIEW 处理

文件：`pkg/frontend/authenticate.go`，第 4905-4985 行

该函数处理 `OBJECT_TYPE_TABLE`、`OBJECT_TYPE_DATABASE`、`OBJECT_TYPE_ACCOUNT`，但没有处理 `OBJECT_TYPE_VIEW`。

### 5. 问题所在：`formSqlFromGrantPrivilege` 函数缺少 VIEW 处理

文件：`pkg/frontend/authenticate.go`，第 7166-7225 行

该函数用于生成检查 WITH GRANT OPTION 的 SQL，没有处理 VIEW 类型。

---

## 设计决策

### 方案选择：VIEW 复用 TABLE 的权限机制

**理由：**
1. VIEW 在 `mo_tables` 表中存储，与 TABLE 共享 `rel_id`
2. VIEW 通过 `relkind = 'v'` 区分，但权限检查逻辑与 TABLE 相同
3. MySQL 兼容性：MySQL 中 VIEW 的权限（SELECT, INSERT, UPDATE, DELETE）与 TABLE 相同
4. 最小改动原则：复用现有 TABLE 权限逻辑，减少代码改动和测试范围

**实现策略：**
- 在 AST 层保留 `OBJECT_TYPE_VIEW` 用于语法区分
- 在权限处理层将 VIEW 映射到 `objectTypeTable`
- 复用现有的 TABLE 权限检查和存储逻辑

---

## 详细修改方案

### 修改 1：`pkg/sql/parsers/tree/revoke.go`

**位置：** 第 214-228 行，`ObjectType.String()` 方法

**修改内容：** 添加 VIEW 的字符串表示

```go
func (node *ObjectType) String() string {
    switch *node {
    case OBJECT_TYPE_TABLE:
        return "table"
    case OBJECT_TYPE_FUNCTION:
        return "function"
    case OBJECT_TYPE_PROCEDURE:
        return "procedure"
    case OBJECT_TYPE_ACCOUNT:
        return "account"
    case OBJECT_TYPE_DATABASE:
        return "database"
    case OBJECT_TYPE_VIEW:           // 新增
        return "view"                 // 新增
    default:
        return "Unknown ObjectType"
    }
}
```

**修改原因：** 使 `OBJECT_TYPE_VIEW` 能正确转换为字符串 "view"，避免报错 "Unknown ObjectType"。

---

### 修改 2：`pkg/frontend/authenticate.go`

#### 2.1 修改 `convertAstObjectTypeToObjectType` 函数

**位置：** 第 4886-4901 行

**修改内容：** 添加 VIEW 到 TABLE 的映射

```go
func convertAstObjectTypeToObjectType(ctx context.Context, ot tree.ObjectType) (objectType, error) {
    var objType objectType
    switch ot {
    case tree.OBJECT_TYPE_TABLE:
        objType = objectTypeTable
    case tree.OBJECT_TYPE_VIEW:       // 新增：VIEW 映射到 objectTypeTable
        objType = objectTypeTable     // VIEW 复用 TABLE 的权限机制
    case tree.OBJECT_TYPE_DATABASE:
        objType = objectTypeDatabase
    case tree.OBJECT_TYPE_ACCOUNT:
        objType = objectTypeAccount
    default:
        return 0, moerr.NewInternalErrorf(ctx, `the object type "%s" is unsupported`, ot.String())
    }
    return objType, nil
}
```

**修改原因：** VIEW 在权限系统中复用 TABLE 的逻辑，因为：
- VIEW 存储在 `mo_tables` 表中，有相同的 `rel_id`
- VIEW 支持的权限（SELECT, INSERT, UPDATE, DELETE）与 TABLE 相同
- 权限记录存储在 `mo_role_privs` 表中，`obj_type` 字段使用 "table"

---

#### 2.2 修改 `checkPrivilegeObjectTypeAndPrivilegeLevel` 函数

**位置：** 第 4905-4985 行

**修改内容：** 添加 VIEW 的处理分支，复用 TABLE 的逻辑

```go
func checkPrivilegeObjectTypeAndPrivilegeLevel(ctx context.Context, ses FeSession, bh BackgroundExec,
    ot tree.ObjectType, pl tree.PrivilegeLevel) (privilegeLevelType, int64, error) {
    var privLevel privilegeLevelType
    var objId int64
    var err error
    var dbName string

    switch ot {
    case tree.OBJECT_TYPE_TABLE, tree.OBJECT_TYPE_VIEW:  // 修改：添加 VIEW
        switch pl.Level {
        case tree.PRIVILEGE_LEVEL_TYPE_STAR:
            privLevel = privilegeLevelStar
            objId, err = getDatabaseOrTableId(ctx, bh, true, ses.GetDatabaseName(), "")
            if err != nil {
                return 0, 0, err
            }
        case tree.PRIVILEGE_LEVEL_TYPE_STAR_STAR:
            privLevel = privilegeLevelStarStar
            objId = objectIDAll
        case tree.PRIVILEGE_LEVEL_TYPE_DATABASE_STAR:
            privLevel = privilegeLevelDatabaseStar
            objId, err = getDatabaseOrTableId(ctx, bh, true, pl.DbName, "")
            if err != nil {
                return 0, 0, err
            }
        case tree.PRIVILEGE_LEVEL_TYPE_DATABASE_TABLE:
            privLevel = privilegeLevelDatabaseTable
            objId, err = getDatabaseOrTableId(ctx, bh, false, pl.DbName, pl.TabName)
            if err != nil {
                return 0, 0, err
            }
        case tree.PRIVILEGE_LEVEL_TYPE_TABLE:
            privLevel = privilegeLevelTable
            objId, err = getDatabaseOrTableId(ctx, bh, false, ses.GetDatabaseName(), pl.TabName)
            if err != nil {
                return 0, 0, err
            }
        default:
            err = moerr.NewInternalErrorf(ctx, `in the object type "%s" the privilege level "%s" is unsupported`, ot.String(), pl.String())
            return 0, 0, err
        }
    // ... 其他 case 保持不变
    }
    return privLevel, objId, err
}
```

**修改原因：** VIEW 的权限级别处理与 TABLE 完全相同，复用同一分支逻辑。

---

#### 2.3 修改 `formSqlFromGrantPrivilege` 函数

**位置：** 第 7166-7225 行

**修改内容：** 添加 VIEW 的处理分支，复用 TABLE 的 SQL 生成逻辑

```go
func formSqlFromGrantPrivilege(ctx context.Context, ses *Session, gp *tree.GrantPrivilege, priv *tree.Privilege) (string, error) {
    tenant := ses.GetTenantInfo()
    sql := ""
    var privType PrivilegeType
    var err error
    privType, err = convertAstPrivilegeTypeToPrivilegeType(ctx, priv.Type, gp.ObjType)
    if err != nil {
        return "", err
    }
    switch gp.ObjType {
    case tree.OBJECT_TYPE_TABLE, tree.OBJECT_TYPE_VIEW:  // 修改：添加 VIEW
        switch gp.Level.Level {
        case tree.PRIVILEGE_LEVEL_TYPE_STAR:
            sql, err = getSqlForCheckWithGrantOptionForTableDatabaseStar(ctx, int64(tenant.GetDefaultRoleID()), privType, ses.GetDatabaseName())
        case tree.PRIVILEGE_LEVEL_TYPE_STAR_STAR:
            sql = getSqlForCheckWithGrantOptionForTableStarStar(int64(tenant.GetDefaultRoleID()), privType)
        case tree.PRIVILEGE_LEVEL_TYPE_DATABASE_STAR:
            sql, err = getSqlForCheckWithGrantOptionForTableDatabaseStar(ctx, int64(tenant.GetDefaultRoleID()), privType, gp.Level.DbName)
        case tree.PRIVILEGE_LEVEL_TYPE_DATABASE_TABLE:
            sql, err = getSqlForCheckWithGrantOptionForTableDatabaseTable(ctx, int64(tenant.GetDefaultRoleID()), privType, gp.Level.DbName, gp.Level.TabName)
        case tree.PRIVILEGE_LEVEL_TYPE_TABLE:
            sql, err = getSqlForCheckWithGrantOptionForTableDatabaseTable(ctx, int64(tenant.GetDefaultRoleID()), privType, ses.GetDatabaseName(), gp.Level.TabName)
        default:
            return "", moerr.NewInternalErrorf(ctx, "in object type %v privilege level type %v is unsupported", gp.ObjType, gp.Level.Level)
        }
    // ... 其他 case 保持不变
    }
    return sql, err
}
```

**修改原因：** VIEW 的 WITH GRANT OPTION 检查逻辑与 TABLE 相同。

---

#### 2.4 修改 `matchPrivilegeTypeWithObjectType` 函数（可选优化）

**位置：** 第 4990-5010 行

**当前代码：**
```go
func matchPrivilegeTypeWithObjectType(ctx context.Context, privType PrivilegeType, objType objectType) error {
    var err error
    switch privType.Scope() {
    // ...
    case PrivilegeScopeTable:
        if objType != objectTypeTable {
            err = moerr.NewInternalErrorf(ctx, `the privilege "%s" can only be granted to the object type "table"`, privType)
        }
    // ...
    }
    return err
}
```

**说明：** 由于 VIEW 已经在 `convertAstObjectTypeToObjectType` 中映射为 `objectTypeTable`，此函数无需修改。VIEW 的权限检查会自动通过 TABLE 的分支。

---

## 完整代码修改

### 文件 1：`pkg/sql/parsers/tree/revoke.go`

```diff
 func (node *ObjectType) String() string {
     switch *node {
     case OBJECT_TYPE_TABLE:
         return "table"
     case OBJECT_TYPE_FUNCTION:
         return "function"
     case OBJECT_TYPE_PROCEDURE:
         return "procedure"
     case OBJECT_TYPE_ACCOUNT:
         return "account"
     case OBJECT_TYPE_DATABASE:
         return "database"
+    case OBJECT_TYPE_VIEW:
+        return "view"
     default:
         return "Unknown ObjectType"
     }
 }
```

### 文件 2：`pkg/frontend/authenticate.go`

#### 修改 1：`convertAstObjectTypeToObjectType` 函数（约第 4886 行）

```diff
 func convertAstObjectTypeToObjectType(ctx context.Context, ot tree.ObjectType) (objectType, error) {
     var objType objectType
     switch ot {
     case tree.OBJECT_TYPE_TABLE:
         objType = objectTypeTable
+    case tree.OBJECT_TYPE_VIEW:
+        objType = objectTypeTable
     case tree.OBJECT_TYPE_DATABASE:
         objType = objectTypeDatabase
     case tree.OBJECT_TYPE_ACCOUNT:
         objType = objectTypeAccount
     default:
         return 0, moerr.NewInternalErrorf(ctx, `the object type "%s" is unsupported`, ot.String())
     }
     return objType, nil
 }
```

#### 修改 2：`checkPrivilegeObjectTypeAndPrivilegeLevel` 函数（约第 4910 行）

```diff
     switch ot {
-    case tree.OBJECT_TYPE_TABLE:
+    case tree.OBJECT_TYPE_TABLE, tree.OBJECT_TYPE_VIEW:
         switch pl.Level {
         case tree.PRIVILEGE_LEVEL_TYPE_STAR:
```

#### 修改 3：`formSqlFromGrantPrivilege` 函数（约第 7175 行）

```diff
     switch gp.ObjType {
-    case tree.OBJECT_TYPE_TABLE:
+    case tree.OBJECT_TYPE_TABLE, tree.OBJECT_TYPE_VIEW:
         switch gp.Level.Level {
         case tree.PRIVILEGE_LEVEL_TYPE_STAR:
```

---

## 测试用例

### BVT 测试文件

需要创建以下测试文件：
- `test/distributed/cases/zz_accesscontrol/grant_view.sql` - 测试 SQL
- `test/distributed/cases/zz_accesscontrol/grant_view.result` - 预期结果

#### 测试文件：`test/distributed/cases/zz_accesscontrol/grant_view.sql`

```sql
-- =====================================================
-- Test: GRANT/REVOKE privileges on VIEW
-- Issue: https://github.com/matrixorigin/matrixone/issues/23232
-- =====================================================

-- env prepare
drop database if exists grant_view_db;
drop role if exists view_role_1, view_role_2, view_role_3;
drop user if exists view_user_1;

create database grant_view_db;
use grant_view_db;

-- create base table and view
create table t1 (id int, name varchar(100));
insert into t1 values (1, 'store1'), (2, 'store2'), (3, 'store3');
create view v1 as select * from t1 where id = 1;
create view v2 as select * from t1 where id > 1;

-- create role and user
create role view_role_1;
create role view_role_2;
create role view_role_3;
create user view_user_1 identified by '123456';

-- =====================================================
-- Test 1: Basic GRANT SELECT ON VIEW
-- =====================================================
grant select on view grant_view_db.v1 to view_role_1;
select role_name, privilege_name, obj_type, privilege_level from mo_catalog.mo_role_privs where role_name = 'view_role_1';

-- =====================================================
-- Test 2: GRANT multiple privileges on VIEW
-- =====================================================
grant select, insert, update, delete on view grant_view_db.v2 to view_role_2;
select role_name, privilege_name, obj_type, privilege_level from mo_catalog.mo_role_privs where role_name = 'view_role_2' order by privilege_name;

-- =====================================================
-- Test 3: GRANT ALL on VIEW
-- =====================================================
grant all on view grant_view_db.v1 to view_role_3;
select role_name, privilege_name, obj_type, privilege_level from mo_catalog.mo_role_privs where role_name = 'view_role_3' order by privilege_name;

-- =====================================================
-- Test 4: GRANT with WITH GRANT OPTION
-- =====================================================
grant select on view grant_view_db.v1 to view_role_1 with grant option;
select role_name, privilege_name, obj_type, privilege_level, with_grant_option from mo_catalog.mo_role_privs where role_name = 'view_role_1' and privilege_name = 'select';

-- =====================================================
-- Test 5: GRANT on VIEW using different privilege levels
-- =====================================================
-- Test: grant on view * (current database)
drop role if exists view_role_star;
create role view_role_star;
grant select on view * to view_role_star;
select role_name, privilege_name, obj_type, privilege_level from mo_catalog.mo_role_privs where role_name = 'view_role_star';
drop role view_role_star;

-- Test: grant on view *.* (all databases)
drop role if exists view_role_star_star;
create role view_role_star_star;
grant select on view *.* to view_role_star_star;
select role_name, privilege_name, obj_type, privilege_level from mo_catalog.mo_role_privs where role_name = 'view_role_star_star';
drop role view_role_star_star;

-- Test: grant on view db.*
drop role if exists view_role_db_star;
create role view_role_db_star;
grant select on view grant_view_db.* to view_role_db_star;
select role_name, privilege_name, obj_type, privilege_level from mo_catalog.mo_role_privs where role_name = 'view_role_db_star';
drop role view_role_db_star;

-- =====================================================
-- Test 6: REVOKE privileges from VIEW
-- =====================================================
revoke select on view grant_view_db.v1 from view_role_1;
select role_name, privilege_name, obj_type, privilege_level from mo_catalog.mo_role_privs where role_name = 'view_role_1' and privilege_name = 'select';

-- =====================================================
-- Test 7: REVOKE ALL from VIEW
-- =====================================================
revoke all on view grant_view_db.v1 from view_role_3;
select count(*) from mo_catalog.mo_role_privs where role_name = 'view_role_3';

-- =====================================================
-- Test 8: Error cases
-- =====================================================
-- grant on non-existent view
grant select on view grant_view_db.non_existent_view to view_role_1;

-- grant to non-existent role
grant select on view grant_view_db.v1 to non_existent_role;

-- grant database-level privilege on view (should fail)
grant create table on view grant_view_db.v1 to view_role_1;

-- =====================================================
-- Test 9: Verify user can access view after grant
-- =====================================================
grant view_role_2 to view_user_1;
-- @session:id=1&user=sys:view_user_1:view_role_2&password=123456
use grant_view_db;
select * from v2;
-- @session

-- =====================================================
-- Cleanup
-- =====================================================
drop view if exists v1;
drop view if exists v2;
drop table if exists t1;
drop role if exists view_role_1, view_role_2, view_role_3;
drop user if exists view_user_1;
drop database if exists grant_view_db;
```

#### 预期结果文件：`test/distributed/cases/zz_accesscontrol/grant_view.result`

```
drop database if exists grant_view_db;
drop role if exists view_role_1, view_role_2, view_role_3;
drop user if exists view_user_1;
create database grant_view_db;
use grant_view_db;
create table t1 (id int, name varchar(100));
insert into t1 values (1, 'store1'), (2, 'store2'), (3, 'store3');
create view v1 as select * from t1 where id = 1;
create view v2 as select * from t1 where id > 1;
create role view_role_1;
create role view_role_2;
create role view_role_3;
create user view_user_1 identified by '123456';
grant select on view grant_view_db.v1 to view_role_1;
select role_name, privilege_name, obj_type, privilege_level from mo_catalog.mo_role_privs where role_name = 'view_role_1';
role_name    privilege_name    obj_type    privilege_level
view_role_1    select    table    d.t
grant select, insert, update, delete on view grant_view_db.v2 to view_role_2;
select role_name, privilege_name, obj_type, privilege_level from mo_catalog.mo_role_privs where role_name = 'view_role_2' order by privilege_name;
role_name    privilege_name    obj_type    privilege_level
view_role_2    delete    table    d.t
view_role_2    insert    table    d.t
view_role_2    select    table    d.t
view_role_2    update    table    d.t
grant all on view grant_view_db.v1 to view_role_3;
select role_name, privilege_name, obj_type, privilege_level from mo_catalog.mo_role_privs where role_name = 'view_role_3' order by privilege_name;
role_name    privilege_name    obj_type    privilege_level
view_role_3    delete    table    d.t
view_role_3    insert    table    d.t
view_role_3    reference    table    d.t
view_role_3    select    table    d.t
view_role_3    table all    table    d.t
view_role_3    truncate    table    d.t
view_role_3    update    table    d.t
grant select on view grant_view_db.v1 to view_role_1 with grant option;
select role_name, privilege_name, obj_type, privilege_level, with_grant_option from mo_catalog.mo_role_privs where role_name = 'view_role_1' and privilege_name = 'select';
role_name    privilege_name    obj_type    privilege_level    with_grant_option
view_role_1    select    table    d.t    true
drop role if exists view_role_star;
create role view_role_star;
grant select on view * to view_role_star;
select role_name, privilege_name, obj_type, privilege_level from mo_catalog.mo_role_privs where role_name = 'view_role_star';
role_name    privilege_name    obj_type    privilege_level
view_role_star    select    table    *
drop role view_role_star;
drop role if exists view_role_star_star;
create role view_role_star_star;
grant select on view *.* to view_role_star_star;
select role_name, privilege_name, obj_type, privilege_level from mo_catalog.mo_role_privs where role_name = 'view_role_star_star';
role_name    privilege_name    obj_type    privilege_level
view_role_star_star    select    table    *.*
drop role view_role_star_star;
drop role if exists view_role_db_star;
create role view_role_db_star;
grant select on view grant_view_db.* to view_role_db_star;
select role_name, privilege_name, obj_type, privilege_level from mo_catalog.mo_role_privs where role_name = 'view_role_db_star';
role_name    privilege_name    obj_type    privilege_level
view_role_db_star    select    table    d.*
drop role view_role_db_star;
revoke select on view grant_view_db.v1 from view_role_1;
select role_name, privilege_name, obj_type, privilege_level from mo_catalog.mo_role_privs where role_name = 'view_role_1' and privilege_name = 'select';
role_name    privilege_name    obj_type    privilege_level
revoke all on view grant_view_db.v1 from view_role_3;
select count(*) from mo_catalog.mo_role_privs where role_name = 'view_role_3';
count(*)
0
grant select on view grant_view_db.non_existent_view to view_role_1;
internal error: there is no table "non_existent_view" in database "grant_view_db"
grant select on view grant_view_db.v1 to non_existent_role;
internal error: there is no role non_existent_role
grant create table on view grant_view_db.v1 to view_role_1;
internal error: the privilege "create table" can only be granted to the object type "database"
grant view_role_2 to view_user_1;
use grant_view_db;
select * from v2;
id    name
2    store2
3    store3
drop view if exists v1;
drop view if exists v2;
drop table if exists t1;
drop role if exists view_role_1, view_role_2, view_role_3;
drop user if exists view_user_1;
drop database if exists grant_view_db;
```

---

## 影响范围分析

### 受影响的功能
1. `GRANT ... ON VIEW ... TO ...` 语句
2. `REVOKE ... ON VIEW ... FROM ...` 语句
3. `SHOW GRANTS` 显示（已有逻辑，无需修改）

### 不受影响的功能
1. `GRANT ... ON TABLE ...` 语句（保持不变）
2. `GRANT ... ON DATABASE ...` 语句（保持不变）
3. `GRANT ... ON ACCOUNT ...` 语句（保持不变）
4. 权限验证逻辑（VIEW 复用 TABLE 的验证逻辑）
5. `mo_role_privs` 表结构（无需修改）

### 兼容性
- MySQL 兼容：MySQL 支持 `GRANT ... ON view_name TO ...`，本实现与 MySQL 行为一致
- 向后兼容：不影响现有的 TABLE/DATABASE/ACCOUNT 权限功能

---

## 实施步骤

1. **修改 `pkg/sql/parsers/tree/revoke.go`**
   - 在 `ObjectType.String()` 方法中添加 `OBJECT_TYPE_VIEW` 的处理

2. **修改 `pkg/frontend/authenticate.go`**
   - 在 `convertAstObjectTypeToObjectType` 函数中添加 VIEW 到 TABLE 的映射
   - 在 `checkPrivilegeObjectTypeAndPrivilegeLevel` 函数中添加 VIEW 的处理
   - 在 `formSqlFromGrantPrivilege` 函数中添加 VIEW 的处理

3. **编写单元测试**
   - 在 `pkg/frontend/authenticate_test.go` 中添加 VIEW 权限相关的测试用例

4. **编写集成测试**
   - 添加 BVT 测试用例验证完整的 GRANT/REVOKE VIEW 流程

5. **代码审查和合并**

---

## 预估工作量

- 代码修改：约 10 行代码
- 单元测试：约 50 行代码
- 集成测试：约 30 行 SQL
- 总计：约 2-4 小时

---

## 风险评估

### 低风险
- 修改范围小，仅涉及 2 个文件
- 复用现有 TABLE 权限逻辑，不引入新的存储结构
- 不影响现有功能

### 需要注意
- 确保 VIEW 的 `rel_id` 查询正确（通过 `mo_tables` 表）
- 确保错误信息友好（如 "there is no view xxx" 而不是 "there is no table xxx"）

---

## 附录：相关代码位置

| 文件 | 函数/变量 | 行号 | 说明 |
|------|----------|------|------|
| `pkg/sql/parsers/tree/revoke.go` | `ObjectType.String()` | 214-228 | 对象类型转字符串 |
| `pkg/sql/parsers/tree/revoke.go` | `OBJECT_TYPE_VIEW` | 237 | VIEW 常量定义 |
| `pkg/frontend/authenticate.go` | `convertAstObjectTypeToObjectType` | 4886-4901 | AST 对象类型转内部类型 |
| `pkg/frontend/authenticate.go` | `checkPrivilegeObjectTypeAndPrivilegeLevel` | 4905-4985 | 检查权限级别 |
| `pkg/frontend/authenticate.go` | `formSqlFromGrantPrivilege` | 7166-7225 | 生成 WITH GRANT OPTION 检查 SQL |
| `pkg/frontend/authenticate.go` | `doGrantPrivilege` | 5013-5160 | GRANT 执行逻辑 |
| `pkg/frontend/authenticate.go` | `doRevokePrivilege` | 4759-4843 | REVOKE 执行逻辑 |
