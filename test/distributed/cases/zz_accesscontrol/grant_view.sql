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
