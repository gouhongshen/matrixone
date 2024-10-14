package main

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"os"
	"path"
	"path/filepath"

	jsoniter "github.com/json-iterator/go"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/migrate"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/spf13/cobra"
)

type migrateArg struct {
}

func (c *migrateArg) PrepareCommand() *cobra.Command {
	migrateCmd := &cobra.Command{
		Use:   "migrate",
		Short: "migrate ckp",
		Run:   RunFactory(c),
	}

	replay := replayArg{}
	migrateCmd.AddCommand(replay.PrepareCommand())

	gc := gcArg{}
	migrateCmd.AddCommand(gc.PrepareCommand())

	return migrateCmd
}

func (c *migrateArg) FromCommand(cmd *cobra.Command) (err error) {
	return nil
}

func (c *migrateArg) String() string {
	return ""
}

func (c *migrateArg) Run() error {
	return nil
}

type fsArg struct {
	Name      string `json:"name"`
	Endpoint  string `json:"endpoint"`
	Bucket    string `json:"bucket"`
	KeyPrefix string `json:"key_prefix"`
}

func getFsArg(input string) (arg fsArg, err error) {
	var data []byte
	if data, err = os.ReadFile(input); err != nil {
		return
	}
	if err = jsoniter.Unmarshal(data, &arg); err != nil {
		return
	}
	return
}

type replayArg struct {
	arg       fsArg
	cfg, meta string
	rootDir   string

	objectList []objectio.ObjectStats
}

func (c *replayArg) PrepareCommand() *cobra.Command {
	replayCmd := &cobra.Command{
		Use:   "replay",
		Short: "replay ckp",
		Run:   RunFactory(c),
	}

	replayCmd.Flags().StringP("cfg", "c", "", "config")
	replayCmd.Flags().StringP("root", "r", "", "root")

	return replayCmd
}

func (c *replayArg) FromCommand(cmd *cobra.Command) (err error) {
	c.rootDir = cmd.Flag("root").Value.String()
	cfg := cmd.Flag("cfg").Value.String()
	if c.rootDir == "" {
		c.arg, err = getFsArg(cfg)
		if err != nil {
			panic(err)
		}
	}
	return nil
}

func (c *replayArg) String() string {
	return ""
}

const (
	dataDir   = "shared"
	ckpDir    = "ckp"
	ckpBakDir = "ckp-bak"
	gcDir     = "gc"

	oldObjDir = "rewritten/old"
	newObjDir = "rewritten/new"
)

func cleanDir(fs fileservice.FileService, dir string) {
	ctx := context.Background()
	entries, _ := fs.List(ctx, dir)
	for _, entry := range entries {
		err := fs.Delete(ctx, dir+"/"+entry.Name)
		if err != nil {
			logutil.Infof("delete %s/%s failed", dir, entry.Name)
		}
	}
}

const (
	rootDir = "/home/mo/wenbin/matrixone/mo-data"
)

func getLatestCkpMeta(fs fileservice.FileService, dir string) (res string) {
	dirs, _ := fs.List(context.Background(), dir)
	var name string
	maxTs := types.BuildTS(0, 0)
	minTs := types.BuildTS(0, 0)
	for _, dir := range dirs {
		start, end, _ := blockio.DecodeCheckpointMetadataFileName(dir.Name)
		if start.EQ(&minTs) && end.GT(&maxTs) {
			maxTs = end
			name = dir.Name
		}
	}
	return name
}

func (c *replayArg) Run() error {
	blockio.Start("")
	defer blockio.Stop("")

	var dataFs, oldObjFS, newObjFS fileservice.FileService

	ctx := context.Background()
	if c.rootDir != "" {
		dataFs = migrate.NewFileFs(path.Join(c.rootDir, dataDir))
		oldObjFS = migrate.NewFileFs(path.Join(c.rootDir, oldObjDir))
		newObjFS = migrate.NewFileFs(path.Join(c.rootDir, newObjDir))
	} else {
		dataFs = migrate.NewS3Fs(ctx, c.arg.Name, c.arg.Endpoint, c.arg.Bucket, c.arg.KeyPrefix)
		oldObjFS = migrate.NewS3Fs(ctx, c.arg.Name, c.arg.Endpoint, c.arg.Bucket, path.Join(c.arg.KeyPrefix, oldObjDir))
		newObjFS = migrate.NewS3Fs(ctx, c.arg.Name, c.arg.Endpoint, c.arg.Bucket, path.Join(c.arg.KeyPrefix, newObjDir))
	}

	c.meta = getLatestCkpMeta(dataFs, ckpDir)

	// 1. Backup ckp meta files
	cleanDir(dataFs, ckpBakDir)
	migrate.BackupCkpDir(dataFs, ckpDir)

	// 2. Clean ckp and gc dir
	cleanDir(dataFs, ckpDir)
	cleanDir(dataFs, gcDir)

	// 3. ListCkpFiles
	c.objectList = migrate.GetCkpFiles(ctx, dataFs, oldObjFS)

	// 4. ReadCkp11File
	fromEntry, ckpbats := migrate.ReadCkp11File(dataFs, filepath.Join(ckpBakDir, c.meta))

	// 5. Replay To 1.3 catalog
	cata := migrate.ReplayCatalogFromCkpData11(ckpbats)

	//dbIt := cata.MakeDBIt(false)
	//for ; dbIt.Valid(); dbIt.Next() {
	//	dbEntry := dbIt.Get().GetPayload()
	//	tblIt := dbEntry.MakeTableIt(false)
	//	for ; tblIt.Valid(); tblIt.Next() {
	//		tblEntry := tblIt.Get().GetPayload()
	//		fmt.Println(dbEntry.GetFullName(), tblEntry.GetFullName(), tblEntry.GetID())
	//	}
	//}

	// 6. Dump catalog to 3 tables batch
	bDb, bTbl, bCol := migrate.DumpCatalogToBatches(cata)

	// 7. Sink and get object stats
	objDB := migrate.SinkBatch(catalog.SystemDBSchema, bDb, dataFs)
	objTbl := migrate.SinkBatch(catalog.SystemTableSchema, bTbl, dataFs)
	objCol := migrate.SinkBatch(catalog.SystemColumnSchema, bCol, dataFs)

	// 8. Write 1.3 Global Ckp
	txnNode := &txnbase.TxnMVCCNode{
		Start:   types.BuildTS(42424242, 0),
		Prepare: types.BuildTS(42424243, 0),
		End:     types.BuildTS(42424243, 0),
	}
	entryNode := &catalog.EntryMVCCNode{
		CreatedAt: types.BuildTS(42424243, 0),
	}

	migrate.RewriteCkp(cata, dataFs, newObjFS, fromEntry, ckpbats, txnNode, entryNode, objDB, objTbl, objCol)

	for _, v := range objDB {
		println(v.String())
	}
	for _, v := range objTbl {
		println(v.String())
	}
	for _, v := range objCol {
		println(v.String())
	}
	for _, v := range c.objectList {
		println(v.String())
	}

	return nil
}

type gcArg struct {
}

func (c *gcArg) PrepareCommand() *cobra.Command {
	gcCmd := &cobra.Command{
		Use:   "gc",
		Short: "gc checkpoint files",
		Run:   RunFactory(c),
	}

	return gcCmd
}

func (c *gcArg) FromCommand(cmd *cobra.Command) (err error) {
	return nil
}

func (c *gcArg) String() string {
	return ""
}

func (c *gcArg) Run() error {

	return nil
}
