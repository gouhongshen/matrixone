package migrate

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/gc/v3"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"path"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

func TestXxx(t *testing.T) {
	blockio.Start("")
	defer blockio.Stop("")

	var (
		rootDir = "/Users/ghs-mo/MOWorkSpace/matrixone-debug/mo-data/"
	)

	dataFS := NewFileFs(path.Join(rootDir, "shared"))
	newObjFS := NewFileFs(path.Join(rootDir, "rewritten/obj"))

	// 0. ListCkpFiles
	ctx := context.Background()
	entries := ListCkpFiles(dataFS)
	sinker := NewSinker(ObjectListSchema, dataFS)
	defer sinker.Close()
	for _, entry := range entries {
		DumpCkpFiles(ctx, dataFS, entry, sinker)
	}
	err := sinker.Sync(ctx)
	if err != nil {
		panic(err)
	}
	objlist, _ := sinker.GetResult()

	// 1. ReadCkp11File
	fromEntry, ckpbats := ReadCkp11File(dataFS, "ckp/meta_0-0_1728882342106913000-1.ckp")
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
	bDb, bTbl, bCol, snapshotMeta := DumpCatalogToBatches(cata)

	// 4. Sink and get object stats
	objDB := SinkBatch(catalog.SystemDBSchema, bDb, dataFS)
	objTbl := SinkBatch(catalog.SystemTableSchema, bTbl, dataFS)
	objCol := SinkBatch(catalog.SystemColumnSchema, bCol, dataFS)

	//5. Write 1.3 Global Ckp
	txnNode := &txnbase.TxnMVCCNode{
		Start:   types.BuildTS(42424242, 0),
		Prepare: types.BuildTS(42424243, 0),
		End:     types.BuildTS(42424243, 0),
	}
	entryNode := &catalog.EntryMVCCNode{
		CreatedAt: types.BuildTS(42424243, 0),
	}

	RewriteCkp(cata, dataFS, newObjFS, fromEntry, ckpbats, txnNode, entryNode, objDB, objTbl, objCol)

	for _, v := range objDB {
		t.Log(v.String())
	}
	for _, v := range objTbl {
		t.Log(v.String())
	}
	for _, v := range objCol {
		t.Log(v.String())
	}
	for _, v := range objlist {
		t.Log(v.String())
	}
	name := blockio.EncodeTableMetadataFileName(
		gc.PrefixAcctMeta,
		fromEntry.GetStart(),
		fromEntry.GetEnd(),
	)
	_, err = snapshotMeta.SaveTableInfo(gc.GCMetaDir+name, newObjFS)
	if err != nil {
		println(err.Error())
	}
}

//func TestBackCkp(t *testing.T) {
//	blockio.Start("")
//	defer blockio.Stop("")
//	fs := NewFileFs("/root/matrixone/mo-data/shared")
//
//	BackupCkpDir(fs, "ckp")
//}

func TestS3Fs(t *testing.T) {
	ctx := context.Background()

	// prod
	// arg := fileservice.ObjectStorageArguments{
	// 	Name:      defines.SharedFileServiceName,
	// 	Endpoint:  "https://oss-cn-hangzhou-internal.aliyuncs.com",
	// 	Bucket:    "mo-bucket-1008",
	// 	KeyPrefix: "mo-20231112/data",
	// }

	// ci
	arg := fileservice.ObjectStorageArguments{
		Name:      defines.SharedFileServiceName,
		Endpoint:  "https://cos.ap-guangzhou.myqcloud.com",
		Bucket:    "mo-nightly-gz-1308875761",
		KeyPrefix: "mo-benchmark-11303890162/data",
	}
	fs, err := fileservice.NewS3FS(ctx, arg, fileservice.DisabledCacheConfig, nil, false, false)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := fs.List(ctx, ckpDir)
	// BackupCkpDir(fs, "ckp")
	t.Log(entries)
	for _, entry := range entries {
		t.Log(entry.Name, entry.IsDir, entry.Size)
	}
}
