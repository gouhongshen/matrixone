package db

import (
	"context"
	"fmt"
	"path"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/engine_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnimpl"
)

var (
	rootDir    = "/Users/ghs-mo/MOWorkSpace/matrixone-debug/mo-data/"
	newDataDir = path.Join(rootDir, "rewritten")
)

func NewFileFs(path string) fileservice.FileService {
	fs := objectio.TmpNewFileservice(context.Background(), path)
	return fs
}

func ListCkpFiles(fs fileservice.FileService) {
	entires, err := fs.List(context.Background(), "ckp/")
	if err != nil {
		panic(err)
	}
	for _, entry := range entires {
		println(entry.Name)
	}
}

func ReadCkp11File(fs fileservice.FileService, filepath string) (*checkpoint.CheckpointEntry, []*containers.Batch) {
	ctx := context.Background()
	reader, err := blockio.NewFileReader("", fs, filepath)
	if err != nil {
		panic(err)
	}
	mp := common.CheckpointAllocator
	bats, closeCB, err := reader.LoadAllColumns(ctx, nil, mp)
	if err != nil {
		panic(err)
	}
	defer func() {
		if closeCB != nil {
			closeCB()
		}
	}()

	if len(bats) != 1 {
		panic("invalid checkpoint file")
	}

	var checkpointVersion int = 3
	bat := containers.NewBatch()
	defer bat.Close()
	{
		// convert to TN Batch
		colNames := checkpoint.CheckpointSchema.Attrs()
		colTypes := checkpoint.CheckpointSchema.Types()
		for i := range bats[0].Vecs {
			var vec containers.Vector
			if bats[0].Vecs[i].Length() == 0 {
				vec = containers.MakeVector(colTypes[i], mp)
			} else {
				vec = containers.ToTNVector(bats[0].Vecs[i], mp)
			}
			bat.AddVector(colNames[i], vec)
		}
	}

	entries, maxEnd := checkpoint.ReplayCheckpointEntries(bat, checkpointVersion)
	var baseEntry *checkpoint.CheckpointEntry
	for _, entry := range entries {
		end := entry.GetEnd()
		if end.Less(&maxEnd) {
			continue
		}
		if baseEntry == nil {
			baseEntry = entry
		} else {
			panic("not global checkpoint?")
		}
	}
	var ckpData = make([]*containers.Batch, MaxIDX)
	for idx, schema := range checkpointDataSchemas_V11 {
		ckpData[idx] = makeRespBatchFromSchema(schema, mp)
	}

	reader1, err := blockio.NewObjectReader("", fs, baseEntry.GetTNLocation())
	if err != nil {
		panic(err)
	}
	// read meta
	typs := append([]types.Type{types.T_Rowid.ToType(), types.T_TS.ToType()}, MetaSchema.Types()...)
	attrs := append([]string{pkgcatalog.Row_ID, pkgcatalog.TableTailAttrCommitTs}, MetaSchema.Attrs()...)
	metaBats, err := logtail.LoadBlkColumnsByMeta(11, ctx, typs, attrs, uint16(MetaIDX), reader1, mp)
	if err != nil {
		panic(err)
	}
	metaBat := metaBats[0]
	println(baseEntry.GetTNLocation().Name().String(), len(metaBats), metaBat.Length())
	ckpData[MetaIDX] = metaBat

	locations := make(map[string]objectio.Location)
	{ // read data locations
		tidVec := vector.MustFixedColNoTypeCheck[uint64](metaBat.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector())
		insVec := metaBat.GetVectorByName(SnapshotMetaAttr_BlockInsertBatchLocation).GetDownstreamVector()
		delVec := metaBat.GetVectorByName(SnapshotMetaAttr_BlockCNInsertBatchLocation).GetDownstreamVector()
		delCNVec := metaBat.GetVectorByName(SnapshotMetaAttr_BlockDeleteBatchLocation).GetDownstreamVector()
		segVec := metaBat.GetVectorByName(SnapshotMetaAttr_SegDeleteBatchLocation).GetDownstreamVector()
		usageInsVec := metaBat.GetVectorByName(CheckpointMetaAttr_StorageUsageInsLocation).GetDownstreamVector()
		usageDelVec := metaBat.GetVectorByName(CheckpointMetaAttr_StorageUsageDelLocation).GetDownstreamVector()

		insertLoc := func(loc []byte) {
			bl := logtail.BlockLocations(loc)
			it := bl.MakeIterator()
			for it.HasNext() {
				block := it.Next()
				if !block.GetLocation().IsEmpty() {
					locations[block.GetLocation().Name().String()] = block.GetLocation()
				}
			}
		}

		for i := 0; i < len(tidVec); i++ {
			tid := tidVec[i]
			if tid == 0 {
				insertLoc(insVec.GetBytesAt(i))
				continue
			}
			insLocation := insVec.GetBytesAt(i)
			delLocation := delVec.GetBytesAt(i)
			delCNLocation := delCNVec.GetBytesAt(i)
			segLocation := segVec.GetBytesAt(i)
			tmp := [][]byte{insLocation, delLocation, delCNLocation, segLocation}
			tmp = append(tmp, usageInsVec.GetBytesAt(i))
			tmp = append(tmp, usageDelVec.GetBytesAt(i))
			for _, loc := range tmp {
				insertLoc(loc)
			}
		}
	}

	// read data
	for _, val := range locations {
		reader, err := blockio.NewObjectReader("", fs, val)
		if err != nil {
			panic(err)
		}

		for idx := 1; idx < MaxIDX; idx++ {
			typs := append([]types.Type{types.T_Rowid.ToType(), types.T_TS.ToType()}, checkpointDataSchemas_V11[idx].Types()...)
			attrs := append([]string{pkgcatalog.Row_ID, pkgcatalog.TableTailAttrCommitTs}, checkpointDataSchemas_V11[idx].Attrs()...)
			bats, err := logtail.LoadBlkColumnsByMeta(11, ctx, typs, attrs, uint16(idx), reader, mp)
			if err != nil {
				panic(err)
			}
			for i := range bats {
				ckpData[idx].Append(bats[i])
			}
		}
	}
	return baseEntry, ckpData
}

func ReplayCatalogFromCkpData11(bats []*containers.Batch) *catalog.Catalog {
	cata, _ := catalog.OpenCatalog(nil)
	ReplayDB(cata, bats[DBInsertIDX], bats[DBInsertTxnIDX], bats[DBDeleteIDX], bats[DBDeleteTxnIDX])
	ReplayTable(cata, bats[TBLInsertIDX], bats[TBLInsertTxnIDX], bats[TBLColInsertIDX], bats[TBLDeleteIDX], bats[TBLDeleteTxnIDX])
	return cata
}

func ReplayDB(cata *catalog.Catalog, ins, insTxn, del, delTxn *containers.Batch) {
	for i := 0; i < ins.Length(); i++ {
		dbid := ins.GetVectorByName(pkgcatalog.SystemDBAttr_ID).Get(i).(uint64)
		name := string(ins.GetVectorByName(pkgcatalog.SystemDBAttr_Name).Get(i).([]byte))
		txnNode := txnbase.ReadTuple(insTxn, i)
		tenantID := ins.GetVectorByName(pkgcatalog.SystemDBAttr_AccID).Get(i).(uint32)
		userID := ins.GetVectorByName(pkgcatalog.SystemDBAttr_Creator).Get(i).(uint32)
		roleID := ins.GetVectorByName(pkgcatalog.SystemDBAttr_Owner).Get(i).(uint32)
		createAt := ins.GetVectorByName(pkgcatalog.SystemDBAttr_CreateAt).Get(i).(types.Timestamp)
		createSql := string(ins.GetVectorByName(pkgcatalog.SystemDBAttr_CreateSQL).Get(i).([]byte))
		datType := string(ins.GetVectorByName(pkgcatalog.SystemDBAttr_Type).Get(i).([]byte))
		cata.OnReplayCreateDB(dbid, name, txnNode, tenantID, userID, roleID, createAt, createSql, datType)
	}
	for i := 0; i < del.Length(); i++ {
		dbid := delTxn.GetVectorByName(SnapshotAttr_DBID).Get(i).(uint64)
		txnNode := txnbase.ReadTuple(delTxn, i)
		cata.OnReplayDeleteDB(dbid, txnNode)
	}
}

func ReplayTable(cata *catalog.Catalog, ins, insTxn, insCol, del, delTxn *containers.Batch) {
	schemaOffset := 0
	for i := 0; i < ins.Length(); i++ {
		tid := ins.GetVectorByName(pkgcatalog.SystemRelAttr_ID).Get(i).(uint64)
		dbid := ins.GetVectorByName(pkgcatalog.SystemRelAttr_DBID).Get(i).(uint64)
		name := string(ins.GetVectorByName(pkgcatalog.SystemRelAttr_Name).Get(i).([]byte))
		schema := catalog.NewEmptySchema(name)
		schemaOffset = schema.ReadFromBatch(insCol, schemaOffset, tid)
		schema.Comment = string(ins.GetVectorByName(pkgcatalog.SystemRelAttr_Comment).Get(i).([]byte))
		schema.Version = ins.GetVectorByName(pkgcatalog.SystemRelAttr_Version).Get(i).(uint32)
		schema.CatalogVersion = ins.GetVectorByName(pkgcatalog.SystemRelAttr_CatalogVersion).Get(i).(uint32)
		schema.Partitioned = ins.GetVectorByName(pkgcatalog.SystemRelAttr_Partitioned).Get(i).(int8)
		schema.Partition = string(ins.GetVectorByName(pkgcatalog.SystemRelAttr_Partition).Get(i).([]byte))
		schema.Relkind = string(ins.GetVectorByName(pkgcatalog.SystemRelAttr_Kind).Get(i).([]byte))
		schema.Createsql = string(ins.GetVectorByName(pkgcatalog.SystemRelAttr_CreateSQL).Get(i).([]byte))
		schema.View = string(ins.GetVectorByName(pkgcatalog.SystemRelAttr_ViewDef).Get(i).([]byte))
		schema.Constraint = ins.GetVectorByName(pkgcatalog.SystemRelAttr_Constraint).Get(i).([]byte)

		schema.AcInfo.RoleID = ins.GetVectorByName(pkgcatalog.SystemRelAttr_Owner).Get(i).(uint32)
		schema.AcInfo.UserID = ins.GetVectorByName(pkgcatalog.SystemRelAttr_Creator).Get(i).(uint32)
		schema.AcInfo.CreateAt = ins.GetVectorByName(pkgcatalog.SystemRelAttr_CreateAt).Get(i).(types.Timestamp)
		schema.AcInfo.TenantID = ins.GetVectorByName(pkgcatalog.SystemRelAttr_AccID).Get(i).(uint32)
		extra := insTxn.GetVectorByName(SnapshotAttr_SchemaExtra).Get(i).([]byte)
		schema.MustRestoreExtra(extra)
		schema.Extra.ObjectMaxBlocks = uint32(insTxn.GetVectorByName(SnapshotAttr_ObjectMaxBlock).Get(i).(uint16))
		schema.Extra.BlockMaxRows = insTxn.GetVectorByName(SnapshotAttr_BlockMaxRow).Get(i).(uint32)
		if err := schema.Finalize(true); err != nil {
			panic(err)
		}
		txnNode := txnbase.ReadTuple(insTxn, i)
		cata.OnReplayCreateTable(dbid, tid, schema, txnNode, &dummyDataFactory{})
	}
	for i := 0; i < del.Length(); i++ {
		dbid := delTxn.GetVectorByName(SnapshotAttr_DBID).Get(i).(uint64)
		tid := delTxn.GetVectorByName(SnapshotAttr_TID).Get(i).(uint64)
		txnNode := txnbase.ReadTuple(delTxn, i)
		cata.OnReplayDeleteTable(dbid, tid, txnNode)
	}
}

func DumpCatalogToBatches(cata *catalog.Catalog) (bDbs, bTables, bCols *containers.Batch) {
	bDbs = makeBasicRespBatchFromSchema(catalog.SystemDBSchema, common.CheckpointAllocator, nil)
	bTables = makeBasicRespBatchFromSchema(catalog.SystemTableSchema, common.CheckpointAllocator, nil)
	bCols = makeBasicRespBatchFromSchema(catalog.SystemColumnSchema, common.CheckpointAllocator, nil)
	visitor := &catalog.LoopProcessor{}
	visitor.DatabaseFn = func(db *catalog.DBEntry) error {
		if db.IsSystemDB() {
			return nil
		}
		node := db.GetLatestCommittedNodeLocked()
		if node.HasDropCommitted() {
			return nil
		}
		for _, def := range catalog.SystemDBSchema.ColDefs {
			if def.IsPhyAddr() {
				continue
			}
			txnimpl.FillDBRow(db, def.Name, bDbs.Vecs[def.Idx])
		}
		return nil
	}

	visitor.TableFn = func(table *catalog.TableEntry) error {
		if pkgcatalog.IsSystemTable(table.GetID()) {
			return nil
		}
		node := table.GetLatestCommittedNodeLocked()
		if node.HasDropCommitted() {
			return nil
		}
		for _, def := range catalog.SystemTableSchema.ColDefs {
			if def.IsPhyAddr() {
				continue
			}
			txnimpl.FillTableRow(table, node.BaseNode.Schema, def.Name, bTables.Vecs[def.Idx])
		}

		for _, def := range catalog.SystemColumnSchema.ColDefs {
			if def.IsPhyAddr() {
				continue
			}
			txnimpl.FillColumnRow(table, node.BaseNode.Schema, def.Name, bCols.Vecs[def.Idx])
		}
		return nil
	}

	if err := cata.RecurLoop(visitor); err != nil {
		panic(err)
	}

	return
}

func SinkBatch(schema *catalog.Schema, bat *containers.Batch, fs fileservice.FileService) []objectio.ObjectStats {
	seqnums := make([]uint16, len(schema.Attrs()))
	for i := range schema.Attrs() {
		seqnums[i] = schema.GetSeqnum(schema.Attrs()[i])
	}

	factory := engine_util.NewFSinkerImplFactory(
		seqnums,
		schema.GetPrimaryKey().Idx,
		true,
		false,
		schema.Version,
	)

	sinker := engine_util.NewSinker(
		schema.GetPrimaryKey().Idx,
		schema.Attrs(),
		schema.Types(),
		factory,
		common.CheckpointAllocator,
		fs,
		engine_util.WithAllMergeSorted(),
		engine_util.WithDedupAll(),
		engine_util.WithTailSizeCap(0),
	)
	if err := sinker.Write(context.Background(), containers.ToCNBatch(bat)); err != nil {
		panic(err)
	}
	if err := sinker.Sync(context.Background()); err != nil {
		panic(err)
	}
	objStats, mem := sinker.GetResult()
	if len(mem) > 0 {
		panic("memory left")
	}
	return objStats
}

func RewriteCkp(
	oldDataFS, newDataFS fileservice.FileService,
	oldCkpEntry *checkpoint.CheckpointEntry,
	oldCkpBats []*containers.Batch,
	txnMVCCNode *txnbase.TxnMVCCNode,
	entryMVCCNode *catalog.EntryMVCCNode,
	dbs, tbls, cols []objectio.ObjectStats,
) {
	ckpData := logtail.NewCheckpointData("", common.CheckpointAllocator)
	dataObjectBatch := ckpData.GetObjectBatchs()
	tombstoneObjectBatch := ckpData.GetTombstoneObjectBatchs()

	fillObjStats := func(objs []objectio.ObjectStats, tid uint64) {
		for _, obj := range objs {
			// padding rowid + committs
			dataObjectBatch.GetVectorByName(catalog.PhyAddrColumnName).Append(objectio.HackObjid2Rowid(objectio.NewObjectid()), false)
			dataObjectBatch.GetVectorByName(objectio.DefaultCommitTS_Attr).Append(txnMVCCNode.End, false)
			dataObjectBatch.GetVectorByName(ObjectAttr_ObjectStats).Append(obj[:], false)
			txnMVCCNode.AppendTuple(dataObjectBatch)
			entryMVCCNode.AppendObjectTuple(dataObjectBatch, true)
			dataObjectBatch.GetVectorByName(SnapshotAttr_DBID).Append(uint64(pkgcatalog.MO_CATALOG_ID), false)
			dataObjectBatch.GetVectorByName(SnapshotAttr_TID).Append(tid, false)
		}
		ckpData.UpdateDataObjectMeta(tid, 0, int32(len(objs)))
	}

	// write three table
	fillObjStats(dbs, pkgcatalog.MO_DATABASE_ID)
	fillObjStats(tbls, pkgcatalog.MO_TABLES_ID)
	fillObjStats(cols, pkgcatalog.MO_COLUMNS_ID)

	// write object stats
	ReplayObjectBatch(oldCkpBats[ObjectInfoIDX], dataObjectBatch)
	ReplayObjectBatch(oldCkpBats[TNObjectInfoIDX], dataObjectBatch)

	// write delta location
	ReplayDeletes(
		txnMVCCNode.End,
		newDataFS, oldDataFS,
		oldCkpBats[BLKMetaInsertIDX],
		oldCkpBats[BLKMetaInsertTxnIDX],
		tombstoneObjectBatch)

	cnLocation, tnLocation, files, err := ckpData.WriteTo(newDataFS, logtail.DefaultCheckpointBlockRows, logtail.DefaultCheckpointSize)
	if err != nil {
		panic(err)
	}
	files = append(files, cnLocation.Name().String())
	logutil.Infof("write files %v", files)
	oldCkpEntry.SetLocation(cnLocation, tnLocation) // update location
	oldCkpEntry.SetVersion(logtail.CheckpointCurrentVersion)

	newCkpMetaBat := makeBasicRespBatchFromSchema(checkpoint.CheckpointSchema, common.CheckpointAllocator, nil)
	newCkpMetaBat.GetVectorByName(checkpoint.CheckpointAttr_StartTS).Append(oldCkpEntry.GetStart(), false)
	newCkpMetaBat.GetVectorByName(checkpoint.CheckpointAttr_EndTS).Append(oldCkpEntry.GetEnd(), false)
	newCkpMetaBat.GetVectorByName(checkpoint.CheckpointAttr_MetaLocation).Append([]byte(oldCkpEntry.GetLocation()), false)
	newCkpMetaBat.GetVectorByName(checkpoint.CheckpointAttr_EntryType).Append(false, false)
	newCkpMetaBat.GetVectorByName(checkpoint.CheckpointAttr_Version).Append(oldCkpEntry.GetVersion(), false)
	newCkpMetaBat.GetVectorByName(checkpoint.CheckpointAttr_AllLocations).Append([]byte(oldCkpEntry.GetTNLocation()), false)
	newCkpMetaBat.GetVectorByName(checkpoint.CheckpointAttr_CheckpointLSN).Append(oldCkpEntry.LSN(), false)
	newCkpMetaBat.GetVectorByName(checkpoint.CheckpointAttr_TruncateLSN).Append(oldCkpEntry.TrunateLSN(), false)
	newCkpMetaBat.GetVectorByName(checkpoint.CheckpointAttr_Type).Append(int8(checkpoint.ET_Global), false)

	name := blockio.EncodeCheckpointMetadataFileName(checkpoint.CheckpointDir, checkpoint.PrefixMetadata, oldCkpEntry.GetStart(), oldCkpEntry.GetEnd())
	writer, err := objectio.NewObjectWriterSpecial(objectio.WriterCheckpoint, name, newDataFS)
	if err != nil {
		panic(err)
	}
	if _, err = writer.Write(containers.ToCNBatch(newCkpMetaBat)); err != nil {
		panic(err)
	}

	_, err = writer.WriteEnd(context.Background())
	if err != nil {
		panic(err)
	}

}

const (
	ObjectFlag_Appendable = 1 << iota
	ObjectFlag_Sorted
	ObjectFlag_CNCreated
)

func ReplayObjectBatch(objects, data *containers.Batch) {
	objectStats := objects.GetVectorByName(ObjectAttr_ObjectStats)
	sortedVec := objects.GetVectorByName(ObjectAttr_Sorted)
	appendableVec := objects.GetVectorByName(ObjectAttr_State)
	dbidVec := objects.GetVectorByName(SnapshotAttr_DBID)
	tidVec := objects.GetVectorByName(SnapshotAttr_TID)
	createAtVec := objects.GetVectorByName(EntryNode_CreateAt)
	deleteAtVec := objects.GetVectorByName(EntryNode_DeleteAt)
	startTSVec := objects.GetVectorByName(txnbase.SnapshotAttr_StartTS)
	prepareTSVec := objects.GetVectorByName(txnbase.SnapshotAttr_PrepareTS)
	commitTSVec := objects.GetVectorByName(txnbase.SnapshotAttr_CommitTS)

	for i := 0; i < objectStats.Length(); i++ {
		obj := objectStats.Get(i).([]byte)
		sorted := sortedVec.Get(i).(bool)
		appendable := appendableVec.Get(i).(bool)
		var reserved byte
		if appendable {
			reserved |= ObjectFlag_Appendable
		} else {
			reserved |= ObjectFlag_CNCreated
		}
		if sorted {
			reserved |= ObjectFlag_Sorted
		}
		obj = append(obj, reserved)

		data.GetVectorByName(ObjectAttr_ObjectStats).Append(obj, false)
		data.GetVectorByName(SnapshotAttr_DBID).Append(dbidVec.Get(i), false)
		data.GetVectorByName(SnapshotAttr_TID).Append(tidVec.Get(i), false)
		data.GetVectorByName(EntryNode_CreateAt).Append(createAtVec.Get(i), false)
		data.GetVectorByName(EntryNode_DeleteAt).Append(deleteAtVec.Get(i), false)
		data.GetVectorByName(txnbase.SnapshotAttr_StartTS).Append(startTSVec.Get(i), false)
		data.GetVectorByName(txnbase.SnapshotAttr_PrepareTS).Append(prepareTSVec.Get(i), false)
		data.GetVectorByName(txnbase.SnapshotAttr_CommitTS).Append(commitTSVec.Get(i), false)
	}
}

func ReplayDeltaLocation(objects, data *containers.Batch) {
	objectStats := objects.GetVectorByName(ObjectAttr_ObjectStats)
	sortedVec := objects.GetVectorByName(ObjectAttr_Sorted)
	appendableVec := objects.GetVectorByName(ObjectAttr_State)
	var ss objectio.ObjectStats
	for i := 0; i < objectStats.Length(); i++ {
		obj := objectStats.Get(i).([]byte)
		sorted := sortedVec.Get(i).(bool)
		appendable := appendableVec.Get(i).(bool)
		var reserved byte
		if appendable {
			reserved |= ObjectFlag_Appendable
		} else {
			reserved |= ObjectFlag_CNCreated
		}
		if sorted {
			reserved |= ObjectFlag_Sorted
		}
		obj = append(obj, reserved)
		ss = objectio.ObjectStats(obj)

		data.GetVectorByName(ObjectAttr_ObjectStats).Append(ss, false)
		data.GetVectorByName(SnapshotAttr_DBID).Append(objects.GetVectorByName(SnapshotAttr_DBID), false)
		data.GetVectorByName(SnapshotAttr_TID).Append(objects.GetVectorByName(SnapshotAttr_TID), false)
		data.GetVectorByName(EntryNode_CreateAt).Append(objects.GetVectorByName(EntryNode_CreateAt), false)
		data.GetVectorByName(EntryNode_DeleteAt).Append(objects.GetVectorByName(EntryNode_DeleteAt), false)
		data.GetVectorByName(txnbase.SnapshotAttr_StartTS).Append(objects.GetVectorByName(txnbase.SnapshotAttr_StartTS), false)
		data.GetVectorByName(txnbase.SnapshotAttr_PrepareTS).Append(objects.GetVectorByName(txnbase.SnapshotAttr_PrepareTS), false)
		data.GetVectorByName(txnbase.SnapshotAttr_CommitTS).Append(objects.GetVectorByName(txnbase.SnapshotAttr_CommitTS), false)
	}
}

func ReplayDeletes(
	ts types.TS,
	newDataFS, oldDataFS fileservice.FileService,
	srcBat, srcTxnBat, destBat *containers.Batch) {

	var (
		err            error
		locColIdx      = 6
		blkIdColIdx    = 2
		commitTSColIdx = 7
	)

	srcCNBat := containers.ToCNBatch(srcBat)
	blkIdCol := vector.MustFixedColWithTypeCheck[types.Blockid](srcCNBat.Vecs[blkIdColIdx])

	tblBlks := make(map[[2]uint64][]types.Blockid)
	for i := range srcCNBat.RowCount() {
		dbid := srcTxnBat.GetVectorByName(SnapshotAttr_DBID).Get(i).(uint64)
		tid := srcTxnBat.GetVectorByName(SnapshotAttr_TID).Get(i).(uint64)
		tblBlks[[2]uint64{dbid, tid}] = append(tblBlks[[2]uint64{dbid, tid}], blkIdCol[i])
	}

	// sort bat rows by commits column,
	// to get the latest delta loc for each blk
	err = mergesort.SortColumnsByIndex(srcCNBat.Vecs, commitTSColIdx, common.CheckpointAllocator)
	if err != nil {
		panic(err)
	}

	blkDeltaLocs := make(map[types.Blockid]objectio.Location)
	for i := range blkIdCol {
		deltaLoc := objectio.Location(srcCNBat.Vecs[locColIdx].GetBytesAt(i))
		blkDeltaLocs[blkIdCol[i]] = deltaLoc
	}

	var (
		bat     *batch.Batch
		release func()
		cols    = []uint16{0, 1}
		ctx     = context.Background()
	)

	for tblId, blks := range tblBlks {
		var sinker *engine_util.Sinker
		for _, blk := range blks {
			loc := blkDeltaLocs[blk]
			fmt.Println(loc.Name())
			bat, _, release, err = blockio.LoadTombstoneColumns(
				ctx, cols, nil, oldDataFS, loc, common.CheckpointAllocator, 0)
			if err != nil {
				panic(err)
			}

			if sinker == nil {
				sinker = engine_util.NewTombstoneSinker(
					false,
					*bat.Vecs[1].GetType(),
					common.CheckpointAllocator, newDataFS,
					engine_util.WithTailSizeCap(0),
					engine_util.WithDedupAll())
			}

			if err = sinker.Write(ctx, bat); err != nil {
				panic(err)
			}

			release()
		}

		if sinker == nil {
			continue
		}

		if err = sinker.Sync(ctx); err != nil {
			panic(err)
		}

		ss, _ := sinker.GetResult()
		for _, s := range ss {
			destBat.GetVectorByName(ObjectAttr_ObjectStats).Append(s, false)
			destBat.GetVectorByName(SnapshotAttr_DBID).Append(tblId[0], false)
			destBat.GetVectorByName(SnapshotAttr_TID).Append(tblId[1], false)
			destBat.GetVectorByName(EntryNode_CreateAt).Append(ts, false)
			destBat.GetVectorByName(EntryNode_DeleteAt).Append(types.TS{}, false)
			destBat.GetVectorByName(txnbase.SnapshotAttr_CommitTS).Append(ts, false)
		}

		if err = sinker.Close(); err != nil {
			panic(err)
		}
	}
}
