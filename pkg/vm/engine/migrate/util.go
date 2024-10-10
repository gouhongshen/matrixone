package migrate

import (
	"context"
	"time"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

func makeRespBatchFromSchema(schema *catalog.Schema, mp *mpool.MPool) *containers.Batch {
	bat := containers.NewBatch()

	bat.AddVector(
		pkgcatalog.Row_ID,
		containers.MakeVector(types.T_Rowid.ToType(), mp),
	)
	bat.AddVector(
		pkgcatalog.TableTailAttrCommitTs,
		containers.MakeVector(types.T_TS.ToType(), mp),
	)
	return makeBasicRespBatchFromSchema(schema, mp, bat)
}

func makeBasicRespBatchFromSchema(schema *catalog.Schema, mp *mpool.MPool, base *containers.Batch) *containers.Batch {
	var bat *containers.Batch
	if base == nil {
		bat = containers.NewBatch()
	} else {
		bat = base
	}

	// Types() is not used, then empty schema can also be handled here
	typs := schema.AllTypes()
	attrs := schema.AllNames()
	for i, attr := range attrs {
		if attr == catalog.PhyAddrColumnName {
			continue
		}
		bat.AddVector(
			attr,
			containers.MakeVector(typs[i], mp),
		)
	}
	return bat
}

// ReadFile read all data from file
func ReadFile(fs fileservice.FileService, file string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	vec := &fileservice.IOVector{
		FilePath: file,
		Entries: []fileservice.IOEntry{
			{
				Offset: 0,
				Size:   -1,
			},
		},
	}
	if err := fs.Read(ctx, vec); err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return vec.Entries[0].Data, nil
}

// WriteFile write data to file
func WriteFile(fs fileservice.FileService, file string, data []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	vec := fileservice.IOVector{
		FilePath: file,
		Entries: []fileservice.IOEntry{
			{
				Offset: 0,
				Size:   int64(len(data)),
				Data:   data,
			},
		},
	}
	return fs.Write(ctx, vec)
}

func BackupCkpDir(fs fileservice.FileService, dir string) {

	ctx := context.Background()
	bakdir := dir + "-bak"

	{
		entries, _ := fs.List(context.Background(), bakdir)
		for _, entry := range entries {
			fs.Delete(ctx, bakdir+"/"+entry.Name)
		}
	}

	entries, err := fs.List(ctx, dir)
	if err != nil {
		panic(err)
	}

	for _, entry := range entries {
		if entry.IsDir {
			panic("bad ckp dir")
		}
	}
	for _, entry := range entries {
		data, err := ReadFile(fs, dir+"/"+entry.Name)
		if err != nil {
			panic(err)
		}
		if err := WriteFile(fs, bakdir+"/"+entry.Name, data); err != nil {
			panic(err)
		}
	}
}
