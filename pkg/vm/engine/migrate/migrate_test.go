package db

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
)

func TestXxx(t *testing.T) {
	blockio.Start("")
	defer blockio.Stop("")
	fs := NewFileFs("/home/v/mo/matrixone/mo-data/shared")
	// 1. ReadCkp11File
	fromEntry, ckpbats := ReadCkp11File(fs, "ckp11/meta_0-0_1727659886661208596-1.ckp")
	t.Log(fromEntry.String())

	// 2. Replay To 1.3 catalog
	cata := ReplayCatalogFromCkpData11(ckpbats)

	// 3. Dump catalog to 3 tables batch
	bDb, bTbl, bCol := DumpCatalogToBatches(cata)

	// 4. Sink and get object stats
	objDB := SinkBatch(catalog.SystemDBSchema, bDb, fs)
	objTbl := SinkBatch(catalog.SystemTableSchema, bTbl, fs)
	objCol := SinkBatch(catalog.SystemColumnSchema, bCol, fs)

	//5. Write 1.3 Global Ckp
	txnNode := &txnbase.TxnMVCCNode{
		Start:   types.BuildTS(42424242, 0),
		Prepare: types.BuildTS(42424243, 0),
		End:     types.BuildTS(42424243, 0),
	}
	entryNode := &catalog.EntryMVCCNode{
		CreatedAt: types.BuildTS(42424243, 0),
	}
	RewriteCkp(fs, fromEntry, ckpbats, txnNode, entryNode, objDB, objTbl, objCol)

	for _, v := range objDB {
		t.Log(v.String())
	}
	for _, v := range objTbl {
		t.Log(v.String())
	}
	for _, v := range objCol {
		t.Log(v.String())
	}
}
