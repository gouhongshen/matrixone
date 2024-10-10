package db

import (
	"fmt"
	"path"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

func TestXxx(t *testing.T) {
	blockio.Start("")
	defer blockio.Stop("")

	oldDataFS := NewFileFs(path.Join(rootDir, "shared"))
	newDataFS := NewFileFs(path.Join(rootDir, "rewritten"))

	// 1. ReadCkp11File
	fromEntry, ckpbats := ReadCkp11File(oldDataFS, "ckp/meta_0-0_1728551542479837000-1.ckp")
	t.Log(fromEntry.String())

	// 2. Replay To 1.3 catalog
	cata := ReplayCatalogFromCkpData11(ckpbats)

	dbIt := cata.MakeDBIt(false)
	for ; dbIt.Valid(); dbIt.Next() {
		dbEntry := dbIt.Get().GetPayload()
		tblIt := dbEntry.MakeTableIt(false)
		for ; tblIt.Valid(); tblIt.Next() {
			tblEntry := tblIt.Get().GetPayload()
			fmt.Println(dbEntry.GetFullName(), tblEntry.GetFullName(), tblEntry.GetID())
		}
	}

	// 3. Dump catalog to 3 tables batch
	bDb, bTbl, bCol := DumpCatalogToBatches(cata)

	// 4. Sink and get object stats
	objDB := SinkBatch(catalog.SystemDBSchema, bDb, newDataFS)
	objTbl := SinkBatch(catalog.SystemTableSchema, bTbl, newDataFS)
	objCol := SinkBatch(catalog.SystemColumnSchema, bCol, newDataFS)

	//5. Write 1.3 Global Ckp
	txnNode := &txnbase.TxnMVCCNode{
		Start:   types.BuildTS(42424242, 0),
		Prepare: types.BuildTS(42424243, 0),
		End:     types.BuildTS(42424243, 0),
	}
	entryNode := &catalog.EntryMVCCNode{
		CreatedAt: types.BuildTS(42424243, 0),
	}

	RewriteCkp(cata, oldDataFS, newDataFS, fromEntry, ckpbats, txnNode, entryNode, objDB, objTbl, objCol)

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
