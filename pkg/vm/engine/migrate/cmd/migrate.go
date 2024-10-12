package main

import (
	"context"
	"path"
	"path/filepath"

	"github.com/matrixorigin/matrixone/pkg/container/types"
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

	backup := backupArg{}
	migrateCmd.AddCommand(backup.PrepareCommand())

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

type backupArg struct {
	dir string
}

func (c *backupArg) PrepareCommand() *cobra.Command {
	backupCmd := &cobra.Command{
		Use:   "backup",
		Short: "backup data",
		Run:   RunFactory(c),
	}

	backupCmd.Flags().StringP("dir", "d", "", "dir")

	return backupCmd
}

func (c *backupArg) FromCommand(cmd *cobra.Command) (err error) {
	c.dir = cmd.Flag("dir").Value.String()
	return nil
}

func (c *backupArg) String() string {
	return ""
}

func (c *backupArg) Run() error {
	blockio.Start("")
	defer blockio.Stop("")

	ctx := context.Background()
	fs := objectio.TmpNewFileservice(ctx, c.dir)
	migrate.BackupCkpDir(fs, "ckp")

	return nil
}

type replayArg struct {
	dir, meta string

	objectList []objectio.ObjectStats
}

func (c *replayArg) PrepareCommand() *cobra.Command {
	replayCmd := &cobra.Command{
		Use:   "replay",
		Short: "replay ckp",
		Run:   RunFactory(c),
	}

	replayCmd.Flags().StringP("dir", "d", "", "dir")
	replayCmd.Flags().StringP("meta", "m", "", "meta")

	return replayCmd
}

func (c *replayArg) FromCommand(cmd *cobra.Command) (err error) {
	c.dir = cmd.Flag("dir").Value.String()
	c.meta = cmd.Flag("meta").Value.String()
	return nil
}

func (c *replayArg) String() string {
	return ""
}

func (c *replayArg) Run() error {
	blockio.Start("")
	defer blockio.Stop("")

	ctx := context.Background()
	oldDataFS := objectio.TmpNewFileservice(ctx, path.Join(c.dir, "shared"))
	newDataFS := objectio.TmpNewFileservice(ctx, path.Join(c.dir, "rewritten"))
	oldObjFS := objectio.TmpNewFileservice(ctx, path.Join(c.dir, "rewritten/old"))
	newObjFS := objectio.TmpNewFileservice(ctx, path.Join(c.dir, "rewritten/new"))

	// 0. ListCkpFiles
	c.objectList = migrate.GetCkpFiles(ctx, oldDataFS, oldObjFS)

	// 1. ReadCkp11File
	fromEntry, ckpbats := migrate.ReadCkp11File(oldDataFS, filepath.Join("ckp", c.meta))

	// 2. Replay To 1.3 catalog
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

	// 3. Dump catalog to 3 tables batch
	bDb, bTbl, bCol := migrate.DumpCatalogToBatches(cata)

	// 4. Sink and get object stats
	objDB := migrate.SinkBatch(catalog.SystemDBSchema, bDb, newDataFS)
	objTbl := migrate.SinkBatch(catalog.SystemTableSchema, bTbl, newDataFS)
	objCol := migrate.SinkBatch(catalog.SystemColumnSchema, bCol, newDataFS)

	//5. Write 1.3 Global Ckp
	txnNode := &txnbase.TxnMVCCNode{
		Start:   types.BuildTS(42424242, 0),
		Prepare: types.BuildTS(42424243, 0),
		End:     types.BuildTS(42424243, 0),
	}
	entryNode := &catalog.EntryMVCCNode{
		CreatedAt: types.BuildTS(42424243, 0),
	}

	migrate.RewriteCkp(cata, oldDataFS, newDataFS, newObjFS, fromEntry, ckpbats, txnNode, entryNode, objDB, objTbl, objCol)

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
