package migrate

import (
	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type dummyDataFactory struct{}

func (d *dummyDataFactory) MakeTableFactory() catalog.TableDataFactory {
	return func(meta *catalog.TableEntry) data.Table { return nil }
}
func (d *dummyDataFactory) MakeObjectFactory() catalog.ObjectDataFactory {
	return func(meta *catalog.ObjectEntry) data.Object { return nil }
}

const (
	MetaIDX = iota

	DBInsertIDX
	DBInsertTxnIDX
	DBDeleteIDX
	DBDeleteTxnIDX

	TBLInsertIDX
	TBLInsertTxnIDX
	TBLDeleteIDX
	TBLDeleteTxnIDX
	TBLColInsertIDX
	TBLColDeleteIDX

	SEGInsertIDX
	SEGInsertTxnIDX
	SEGDeleteIDX
	SEGDeleteTxnIDX

	BLKMetaInsertIDX
	BLKMetaInsertTxnIDX
	BLKMetaDeleteIDX
	BLKMetaDeleteTxnIDX

	BLKTNMetaInsertIDX
	BLKTNMetaInsertTxnIDX
	BLKTNMetaDeleteIDX
	BLKTNMetaDeleteTxnIDX

	BLKCNMetaInsertIDX

	TNMetaIDX

	// supporting `show accounts` by recording extra
	// account related info in checkpoint

	StorageUsageInsIDX

	ObjectInfoIDX
	TNObjectInfoIDX

	StorageUsageDelIDX

	MaxIDX = StorageUsageDelIDX + 1
)

const (
	SnapshotAttr_TID                            = catalog.SnapshotAttr_TID
	SnapshotAttr_DBID                           = catalog.SnapshotAttr_DBID
	ObjectAttr_ID                               = "id"
	ObjectAttr_CreateAt                         = "craete_at"
	ObjectAttr_SegNode                          = "seg_node"
	SnapshotAttr_BlockMaxRow                    = "block_max_row"
	SnapshotAttr_ObjectMaxBlock                 = "object_max_block"
	ObjectAttr_ObjectStats                      = catalog.ObjectAttr_ObjectStats
	ObjectAttr_State                            = "state"
	ObjectAttr_Sorted                           = "sorted"
	EntryNode_CreateAt                          = catalog.EntryNode_CreateAt
	EntryNode_DeleteAt                          = catalog.EntryNode_DeleteAt
	SnapshotMetaAttr_BlockInsertBatchStart      = "block_insert_batch_start"
	SnapshotMetaAttr_BlockInsertBatchEnd        = "block_insert_batch_end"
	SnapshotMetaAttr_BlockInsertBatchLocation   = "block_insert_batch_location"
	SnapshotMetaAttr_BlockDeleteBatchStart      = "block_delete_batch_start"
	SnapshotMetaAttr_BlockDeleteBatchEnd        = "block_delete_batch_end"
	SnapshotMetaAttr_BlockDeleteBatchLocation   = "block_delete_batch_location"
	SnapshotMetaAttr_BlockCNInsertBatchLocation = "block_cn_insert_batch_location"
	SnapshotMetaAttr_SegDeleteBatchStart        = "seg_delete_batch_start"
	SnapshotMetaAttr_SegDeleteBatchEnd          = "seg_delete_batch_end"
	SnapshotMetaAttr_SegDeleteBatchLocation     = "seg_delete_batch_location"
	CheckpointMetaAttr_BlockLocation            = "checkpoint_meta_block_location"
	CheckpointMetaAttr_SchemaType               = "checkpoint_meta_schema_type"
	CheckpointMetaAttr_ObjectSize               = "checkpoint_meta_object_size"
	CheckpointMetaAttr_ObjectID                 = "checkpoint_meta_object_id"
	CheckpointMetaAttr_StorageUsageInsLocation  = "checkpoint_meta_storage_usage_ins_location"
	CheckpointMetaAttr_StorageUsageDelLocation  = "checkpoint_meta_storage_usage_del_location"
	SnapshotAttr_SchemaExtra                    = "schema_extra"

	BlockMeta_DeltaLoc = "delta_loc"
)

var (
	checkpointDataSchemas_V11 [MaxIDX]*catalog.Schema

	// for blk meta response
	BlkMetaSchema      *catalog.Schema // latest version
	DelSchema          *catalog.Schema
	SegSchema          *catalog.Schema
	TxnNodeSchema      *catalog.Schema
	DBTNSchema         *catalog.Schema
	TblTNSchema        *catalog.Schema
	SegTNSchema        *catalog.Schema
	BlkTNSchema        *catalog.Schema
	MetaSchema         *catalog.Schema
	DBDelSchema        *catalog.Schema
	TblDelSchema       *catalog.Schema
	ColumnDelSchema    *catalog.Schema
	TNMetaSchema       *catalog.Schema
	ObjectInfoSchema   *catalog.Schema
	StorageUsageSchema *catalog.Schema
	DBSchema11         *catalog.Schema
	TblSchema11        *catalog.Schema
	ColumnSchema11     *catalog.Schema

	ObjectListSchema *catalog.Schema
)

var (
	MoDatabaseSchema = []string{
		pkgcatalog.SystemDBAttr_ID,
		pkgcatalog.SystemDBAttr_Name,
		pkgcatalog.SystemDBAttr_CatalogName,
		pkgcatalog.SystemDBAttr_CreateSQL,
		pkgcatalog.SystemDBAttr_Owner,
		pkgcatalog.SystemDBAttr_Creator,
		pkgcatalog.SystemDBAttr_CreateAt,
		pkgcatalog.SystemDBAttr_AccID,
		pkgcatalog.SystemDBAttr_Type,
	}
	MoDatabaseTypes = []types.Type{
		types.New(types.T_uint64, 0, 0),     // dat_id
		types.New(types.T_varchar, 5000, 0), // datname
		types.New(types.T_varchar, 5000, 0), // dat_catalog_name
		types.New(types.T_varchar, 5000, 0), // dat_createsql
		types.New(types.T_uint32, 0, 0),     // owner
		types.New(types.T_uint32, 0, 0),     // creator
		types.New(types.T_timestamp, 0, 0),  // created_time
		types.New(types.T_uint32, 0, 0),     // account_id
		types.New(types.T_varchar, 32, 0),   // dat_type
	}
	MoTablesSchema = []string{
		pkgcatalog.SystemRelAttr_ID,
		pkgcatalog.SystemRelAttr_Name,
		pkgcatalog.SystemRelAttr_DBName,
		pkgcatalog.SystemRelAttr_DBID,
		pkgcatalog.SystemRelAttr_Persistence,
		pkgcatalog.SystemRelAttr_Kind,
		pkgcatalog.SystemRelAttr_Comment,
		pkgcatalog.SystemRelAttr_CreateSQL,
		pkgcatalog.SystemRelAttr_CreateAt,
		pkgcatalog.SystemRelAttr_Creator,
		pkgcatalog.SystemRelAttr_Owner,
		pkgcatalog.SystemRelAttr_AccID,
		pkgcatalog.SystemRelAttr_Partitioned,
		pkgcatalog.SystemRelAttr_Partition,
		pkgcatalog.SystemRelAttr_ViewDef,
		pkgcatalog.SystemRelAttr_Constraint,
		pkgcatalog.SystemRelAttr_Version,
		pkgcatalog.SystemRelAttr_CatalogVersion,
	}
	MoTablesTypes = []types.Type{
		types.New(types.T_uint64, 0, 0),     // rel_id
		types.New(types.T_varchar, 5000, 0), // relname
		types.New(types.T_varchar, 5000, 0), // reldatabase
		types.New(types.T_uint64, 0, 0),     // reldatabase_id
		types.New(types.T_varchar, 5000, 0), // relpersistence
		types.New(types.T_varchar, 5000, 0), // relkind
		types.New(types.T_varchar, 5000, 0), // rel_comment
		types.New(types.T_text, 0, 0),       // rel_createsql
		types.New(types.T_timestamp, 0, 0),  // created_time
		types.New(types.T_uint32, 0, 0),     // creator
		types.New(types.T_uint32, 0, 0),     // owner
		types.New(types.T_uint32, 0, 0),     // account_id
		types.New(types.T_int8, 0, 0),       // partitioned
		types.New(types.T_blob, 0, 0),       // partition_info
		types.New(types.T_varchar, 5000, 0), // viewdef
		types.New(types.T_varchar, 5000, 0), // constraint
		types.New(types.T_uint32, 0, 0),     // schema_version
		types.New(types.T_uint32, 0, 0),     // schema_catalog_version
	}
	MoColumnsSchema = []string{
		pkgcatalog.SystemColAttr_UniqName,
		pkgcatalog.SystemColAttr_AccID,
		pkgcatalog.SystemColAttr_DBID,
		pkgcatalog.SystemColAttr_DBName,
		pkgcatalog.SystemColAttr_RelID,
		pkgcatalog.SystemColAttr_RelName,
		pkgcatalog.SystemColAttr_Name,
		pkgcatalog.SystemColAttr_Type,
		pkgcatalog.SystemColAttr_Num,
		pkgcatalog.SystemColAttr_Length,
		pkgcatalog.SystemColAttr_NullAbility,
		pkgcatalog.SystemColAttr_HasExpr,
		pkgcatalog.SystemColAttr_DefaultExpr,
		pkgcatalog.SystemColAttr_IsDropped,
		pkgcatalog.SystemColAttr_ConstraintType,
		pkgcatalog.SystemColAttr_IsUnsigned,
		pkgcatalog.SystemColAttr_IsAutoIncrement,
		pkgcatalog.SystemColAttr_Comment,
		pkgcatalog.SystemColAttr_IsHidden,
		pkgcatalog.SystemColAttr_HasUpdate,
		pkgcatalog.SystemColAttr_Update,
		pkgcatalog.SystemColAttr_IsClusterBy,
		pkgcatalog.SystemColAttr_Seqnum,
		pkgcatalog.SystemColAttr_EnumValues,
	}
	MoColumnsTypes = []types.Type{
		types.New(types.T_varchar, 256, 0),                 // att_uniq_name
		types.New(types.T_uint32, 0, 0),                    // account_id
		types.New(types.T_uint64, 0, 0),                    // att_database_id
		types.New(types.T_varchar, 256, 0),                 // att_database
		types.New(types.T_uint64, 0, 0),                    // att_relname_id
		types.New(types.T_varchar, 256, 0),                 // att_relname
		types.New(types.T_varchar, 256, 0),                 // attname
		types.New(types.T_varchar, 256, 0),                 // atttyp
		types.New(types.T_int32, 0, 0),                     // attnum
		types.New(types.T_int32, 0, 0),                     // att_length
		types.New(types.T_int8, 0, 0),                      // attnotnull
		types.New(types.T_int8, 0, 0),                      // atthasdef
		types.New(types.T_varchar, 2048, 0),                // att_default
		types.New(types.T_int8, 0, 0),                      // attisdropped
		types.New(types.T_char, 1, 0),                      // att_constraint_type
		types.New(types.T_int8, 0, 0),                      // att_is_unsigned
		types.New(types.T_int8, 0, 0),                      // att_is_auto_increment
		types.New(types.T_varchar, 2048, 0),                // att_comment
		types.New(types.T_int8, 0, 0),                      // att_is_hidden
		types.New(types.T_int8, 0, 0),                      // att_has_update
		types.New(types.T_varchar, 2048, 0),                // att_update
		types.New(types.T_int8, 0, 0),                      // att_is_clusterby
		types.New(types.T_uint16, 0, 0),                    // att_seqnum
		types.New(types.T_varchar, types.MaxVarcharLen, 0), // att_enum
	}

	ObjectSchemaAttr = []string{
		ObjectAttr_ID,
		ObjectAttr_CreateAt,
		ObjectAttr_SegNode,
	}
	ObjectSchemaTypes = []types.Type{
		types.New(types.T_uuid, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_blob, 0, 0),
	}
	TxnNodeSchemaAttr = []string{
		txnbase.SnapshotAttr_LogIndex_LSN,
		txnbase.SnapshotAttr_StartTS,
		txnbase.SnapshotAttr_PrepareTS,
		txnbase.SnapshotAttr_CommitTS,
		txnbase.SnapshotAttr_LogIndex_CSN,
		txnbase.SnapshotAttr_LogIndex_Size,
	}
	TxnNodeSchemaTypes = []types.Type{
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_uint32, 0, 0),
		types.New(types.T_uint32, 0, 0),
	}
	DBTNSchemaAttr = []string{
		txnbase.SnapshotAttr_LogIndex_LSN,
		txnbase.SnapshotAttr_StartTS,
		txnbase.SnapshotAttr_PrepareTS,
		txnbase.SnapshotAttr_CommitTS,
		txnbase.SnapshotAttr_LogIndex_CSN,
		txnbase.SnapshotAttr_LogIndex_Size,
		SnapshotAttr_DBID,
		SnapshotAttr_TID,
	}
	DBTNSchemaType = []types.Type{
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_uint32, 0, 0),
		types.New(types.T_uint32, 0, 0),
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_uint64, 0, 0),
	}
	TblTNSchemaAttr = []string{
		txnbase.SnapshotAttr_LogIndex_LSN,
		txnbase.SnapshotAttr_StartTS,
		txnbase.SnapshotAttr_PrepareTS,
		txnbase.SnapshotAttr_CommitTS,
		txnbase.SnapshotAttr_LogIndex_CSN,
		txnbase.SnapshotAttr_LogIndex_Size,
		SnapshotAttr_DBID,
		SnapshotAttr_TID,
		SnapshotAttr_BlockMaxRow,
		SnapshotAttr_ObjectMaxBlock,
		SnapshotAttr_SchemaExtra,
	}
	TblTNSchemaType = []types.Type{
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_uint32, 0, 0),
		types.New(types.T_uint32, 0, 0),
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_uint32, 0, 0),
		types.New(types.T_uint16, 0, 0),
		types.New(types.T_varchar, 0, 0),
	}
	ObjectTNSchemaAttr = []string{
		txnbase.SnapshotAttr_LogIndex_LSN,
		txnbase.SnapshotAttr_StartTS,
		txnbase.SnapshotAttr_PrepareTS,
		txnbase.SnapshotAttr_CommitTS,
		txnbase.SnapshotAttr_LogIndex_CSN,
		txnbase.SnapshotAttr_LogIndex_Size,
		SnapshotAttr_DBID,
		SnapshotAttr_TID,
	}
	ObjectTNSchemaTypes = []types.Type{
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_uint32, 0, 0),
		types.New(types.T_uint32, 0, 0),
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_uint64, 0, 0),
	}
	BlockTNSchemaAttr = []string{
		txnbase.SnapshotAttr_LogIndex_LSN,
		txnbase.SnapshotAttr_StartTS,
		txnbase.SnapshotAttr_PrepareTS,
		txnbase.SnapshotAttr_CommitTS,
		txnbase.SnapshotAttr_LogIndex_CSN,
		txnbase.SnapshotAttr_LogIndex_Size,
		SnapshotAttr_DBID,
		SnapshotAttr_TID,
		pkgcatalog.BlockMeta_MetaLoc,
		pkgcatalog.BlockMeta_DeltaLoc,
	}
	BlockTNSchemaTypes = []types.Type{
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_uint32, 0, 0),
		types.New(types.T_uint32, 0, 0),
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
	}
	MetaSchemaAttr = []string{
		SnapshotAttr_TID,
		SnapshotMetaAttr_BlockInsertBatchLocation,
		SnapshotMetaAttr_BlockCNInsertBatchLocation,
		SnapshotMetaAttr_BlockDeleteBatchLocation,
		SnapshotMetaAttr_SegDeleteBatchLocation,
		CheckpointMetaAttr_StorageUsageInsLocation,
		CheckpointMetaAttr_StorageUsageDelLocation,
	}

	MetaShcemaTypes = []types.Type{
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
	}
	DBDelSchemaAttr = []string{
		pkgcatalog.SystemDBAttr_ID,
	}
	DBDelSchemaTypes = []types.Type{
		types.T_uint64.ToType(),
	}
	TblDelSchemaAttr = []string{
		pkgcatalog.SystemRelAttr_ID,
	}
	TblDelSchemaTypes = []types.Type{
		types.T_uint64.ToType(),
	}
	ColumnDelSchemaAttr = []string{
		pkgcatalog.SystemColAttr_UniqName,
	}
	ColumnDelSchemaTypes = []types.Type{
		types.T_varchar.ToType(),
	}
	TNMetaSchemaAttr = []string{
		CheckpointMetaAttr_BlockLocation,
		CheckpointMetaAttr_SchemaType,
	}
	TNMetaShcemaTypes = []types.Type{
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		types.New(types.T_uint16, 0, 0),
	}

	ObjectInfoAttr = []string{
		ObjectAttr_ObjectStats,
		ObjectAttr_State, // entry_state, true for appendable
		ObjectAttr_Sorted,
		SnapshotAttr_DBID,
		SnapshotAttr_TID,
		EntryNode_CreateAt,
		EntryNode_DeleteAt,
		txnbase.SnapshotAttr_StartTS,
		txnbase.SnapshotAttr_PrepareTS,
		txnbase.SnapshotAttr_CommitTS,
	}
	ObjectInfoTypes = []types.Type{
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		types.New(types.T_bool, 0, 0),
		types.New(types.T_bool, 0, 0),
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_TS, 0, 0),
	}

	StorageUsageSchemaAttrs = []string{
		pkgcatalog.SystemColAttr_AccID,
		SnapshotAttr_DBID,
		SnapshotAttr_TID,
		CheckpointMetaAttr_ObjectID,
		CheckpointMetaAttr_ObjectSize,
	}

	StorageUsageSchemaTypes = []types.Type{
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_uuid, 0, 0),
		types.New(types.T_uint64, 0, 0),
	}

	ObjectListSchemaAttr = []string{
		ObjectAttr_ID,
	}
	ObjectListSchemaTypes = []types.Type{
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
	}
)

func init() {
	DBSchema11 = catalog.NewEmptySchema(pkgcatalog.MO_DATABASE)
	for i, colname := range MoDatabaseSchema {
		if i == 0 {
			if err := DBSchema11.AppendPKCol(colname, MoDatabaseTypes[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := DBSchema11.AppendCol(colname, MoDatabaseTypes[i]); err != nil {
				panic(err)
			}
		}
	}
	if err := DBSchema11.Finalize(true); err != nil {
		panic(err)
	}

	TblSchema11 = catalog.NewEmptySchema(pkgcatalog.MO_TABLES)
	for i, colname := range MoTablesSchema {
		if i == 0 {
			if err := TblSchema11.AppendPKCol(colname, MoTablesTypes[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := TblSchema11.AppendCol(colname, MoTablesTypes[i]); err != nil {
				panic(err)
			}
		}
	}
	if err := TblSchema11.Finalize(true); err != nil {
		panic(err)
	}

	ColumnSchema11 = catalog.NewEmptySchema(pkgcatalog.MO_COLUMNS)
	for i, colname := range MoColumnsSchema {
		if i == 0 {
			if err := ColumnSchema11.AppendPKCol(colname, MoColumnsTypes[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := ColumnSchema11.AppendCol(colname, MoColumnsTypes[i]); err != nil {
				panic(err)
			}
		}
	}
	if err := ColumnSchema11.Finalize(true); err != nil {
		panic(err)
	}

	BlkMetaSchema = catalog.NewEmptySchema("blkMeta")

	for i, colname := range pkgcatalog.MoTableMetaSchema {
		if i == 0 {
			if err := BlkMetaSchema.AppendPKCol(colname, pkgcatalog.MoTableMetaTypes[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := BlkMetaSchema.AppendCol(colname, pkgcatalog.MoTableMetaTypes[i]); err != nil {
				panic(err)
			}
		}
	}
	if err := BlkMetaSchema.Finalize(true); err != nil { // no phyaddr column
		panic(err)
	}

	// empty schema, no finalize, makeRespBatchFromSchema will add necessary colunms
	DelSchema = catalog.NewEmptySchema("del")
	SegSchema = catalog.NewEmptySchema("Object")
	for i, colname := range ObjectSchemaAttr {
		if i == 0 {
			if err := SegSchema.AppendPKCol(colname, ObjectSchemaTypes[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := SegSchema.AppendCol(colname, ObjectSchemaTypes[i]); err != nil {
				panic(err)
			}
		}
	}

	TxnNodeSchema = catalog.NewEmptySchema("txn_node")
	for i, colname := range TxnNodeSchemaAttr {
		if i == 0 {
			if err := TxnNodeSchema.AppendPKCol(colname, TxnNodeSchemaTypes[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := TxnNodeSchema.AppendCol(colname, TxnNodeSchemaTypes[i]); err != nil {
				panic(err)
			}
		}
	}

	DBTNSchema = catalog.NewEmptySchema("db_dn")
	for i, colname := range DBTNSchemaAttr {
		if i == 0 {
			if err := DBTNSchema.AppendPKCol(colname, DBTNSchemaType[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := DBTNSchema.AppendCol(colname, DBTNSchemaType[i]); err != nil {
				panic(err)
			}
		}
	}

	TblTNSchema = catalog.NewEmptySchema("table_dn")
	for i, colname := range TblTNSchemaAttr {
		if i == 0 {
			if err := TblTNSchema.AppendPKCol(colname, TblTNSchemaType[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := TblTNSchema.AppendCol(colname, TblTNSchemaType[i]); err != nil {
				panic(err)
			}
		}
	}

	SegTNSchema = catalog.NewEmptySchema("Object_dn")
	for i, colname := range ObjectTNSchemaAttr {
		if i == 0 {
			if err := SegTNSchema.AppendPKCol(colname, ObjectTNSchemaTypes[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := SegTNSchema.AppendCol(colname, ObjectTNSchemaTypes[i]); err != nil {
				panic(err)
			}
		}
	}

	BlkTNSchema = catalog.NewEmptySchema("block_dn")
	for i, colname := range BlockTNSchemaAttr {
		if i == 0 {
			if err := BlkTNSchema.AppendPKCol(colname, BlockTNSchemaTypes[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := BlkTNSchema.AppendCol(colname, BlockTNSchemaTypes[i]); err != nil {
				panic(err)
			}
		}
	}

	MetaSchema = catalog.NewEmptySchema("meta")
	for i, colname := range MetaSchemaAttr {
		if i == 0 {
			if err := MetaSchema.AppendPKCol(colname, MetaShcemaTypes[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := MetaSchema.AppendCol(colname, MetaShcemaTypes[i]); err != nil {
				panic(err)
			}
		}
	}

	DBDelSchema = catalog.NewEmptySchema("meta")
	for i, colname := range DBDelSchemaAttr {
		if i == 0 {
			if err := DBDelSchema.AppendPKCol(colname, DBDelSchemaTypes[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := DBDelSchema.AppendCol(colname, DBDelSchemaTypes[i]); err != nil {
				panic(err)
			}
		}
	}

	TblDelSchema = catalog.NewEmptySchema("meta")
	for i, colname := range TblDelSchemaAttr {
		if i == 0 {
			if err := TblDelSchema.AppendPKCol(colname, TblDelSchemaTypes[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := TblDelSchema.AppendCol(colname, TblDelSchemaTypes[i]); err != nil {
				panic(err)
			}
		}
	}

	ColumnDelSchema = catalog.NewEmptySchema("meta")
	for i, colname := range ColumnDelSchemaAttr {
		if i == 0 {
			if err := ColumnDelSchema.AppendPKCol(colname, ColumnDelSchemaTypes[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := ColumnDelSchema.AppendCol(colname, ColumnDelSchemaTypes[i]); err != nil {
				panic(err)
			}
		}
	}

	TNMetaSchema = catalog.NewEmptySchema("meta")
	for i, colname := range TNMetaSchemaAttr {
		if i == 0 {
			if err := TNMetaSchema.AppendPKCol(colname, TNMetaShcemaTypes[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := TNMetaSchema.AppendCol(colname, TNMetaShcemaTypes[i]); err != nil {
				panic(err)
			}
		}
	}

	ObjectInfoSchema = catalog.NewEmptySchema("object_info")
	for i, colname := range ObjectInfoAttr {
		if i == 0 {
			if err := ObjectInfoSchema.AppendPKCol(colname, ObjectInfoTypes[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := ObjectInfoSchema.AppendCol(colname, ObjectInfoTypes[i]); err != nil {
				panic(err)
			}
		}
	}

	StorageUsageSchema = catalog.NewEmptySchema("storage_usage")
	for i, colname := range StorageUsageSchemaAttrs {
		if i == 0 {
			if err := StorageUsageSchema.AppendPKCol(colname, StorageUsageSchemaTypes[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := StorageUsageSchema.AppendCol(colname, StorageUsageSchemaTypes[i]); err != nil {
				panic(err)
			}
		}
	}

	ObjectListSchema = catalog.NewEmptySchema("object_list")
	for i, colname := range ObjectListSchemaAttr {
		if i == 0 {
			if err := ObjectListSchema.AppendPKCol(colname, ObjectListSchemaTypes[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := ObjectListSchema.AppendCol(colname, ObjectListSchemaTypes[i]); err != nil {
				panic(err)
			}
		}
	}
	if err := ObjectListSchema.Finalize(true); err != nil { // no phyaddr column
		panic(err)
	}

	// v11 add storage usage del bat
	checkpointDataSchemas_V11 = [MaxIDX]*catalog.Schema{
		MetaSchema,
		DBSchema11,
		TxnNodeSchema,
		DBDelSchema, // 3
		DBTNSchema,
		TblSchema11,
		TblTNSchema,
		TblDelSchema, // 7
		TblTNSchema,
		ColumnSchema11,
		ColumnDelSchema,
		SegSchema, // 11
		SegTNSchema,
		DelSchema,
		SegTNSchema,
		BlkMetaSchema, // 15
		BlkTNSchema,
		DelSchema,
		BlkTNSchema,
		BlkMetaSchema, // 19
		BlkTNSchema,
		DelSchema,
		BlkTNSchema,
		BlkMetaSchema, // 23
		TNMetaSchema,
		StorageUsageSchema, // 25
		ObjectInfoSchema,
		ObjectInfoSchema,
		StorageUsageSchema,
	}
}
