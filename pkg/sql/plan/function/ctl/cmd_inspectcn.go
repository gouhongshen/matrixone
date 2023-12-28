// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ctl

import (
	"bytes"
	"context"
	"fmt"
	"github.com/google/shlex"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/ctl/inspectcn"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/rpc"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/spf13/cobra"
	"strings"
	"time"
)

type InspectCNCmd interface {
	rpc.InspectCmd
}

type InspectCNContext struct {
	args []string
	proc *process.Process
	// stores any err ever happened
	msg *bytes.Buffer
	// stores the handler's result
	out *bytes.Buffer
}

func (i *InspectCNContext) String() string   { return "" }
func (i *InspectCNContext) Set(string) error { return nil }
func (i *InspectCNContext) Type() string     { return "ictx" }

// cmd format like:
// mo_ctl("cn", "inspect cn", "cn_uuid:the real cmd"), e.g.
// mo_ctl("cn", "inspect cn", "cn_uuid:test -t hello")
func handleInspectCN(
	proc *process.Process,
	service serviceType,
	parameter string,
	sender requestSender) (Result, error) {

	cmd, err := parseInspectArgs(proc, service, parameter)
	if err != nil {
		return Result{
			Method: InspectCNMethod,
		}, err
	}

	ictx, err := InspectHere(proc, cmd)

	return fmtFinalOutput(proc.QueryService.ServiceID(), ictx, err)

}

func fmtFinalOutput(uuid string, ictx *InspectCNContext, err error) (Result, error) {
	var resp bytes.Buffer
	resp.WriteString("\n")
	if err != nil {
		resp.WriteString(fmt.Sprintf("err happend: %s", err.Error()))
	} else {
		resp.WriteString(fmt.Sprintf("message: %v\n%v", ictx.msg.String(), ictx.out.String()))
	}
	resp.WriteString("\n")

	return Result{
		Method: InspectCNMethod,
		Data:   resp.Bytes(),
	}, nil
}

func parseInspectArgs(proc *process.Process, service serviceType, param string) (cmd string, err error) {
	if service != cn {
		return "", moerr.NewInvalidArgNoCtx("`inspect cn` only can effect on cn service", service)
	}

	args := strings.Split(param, ":")
	if len(args) != 2 {
		return "", moerr.NewInvalidArgNoCtx(param, "")
	}

	if proc.QueryService.ServiceID() != args[0] {
		return "", moerr.NewInvalidArgNoCtx("transfer query not supported", args[0])
	}

	cmd = args[1]
	return
}

// InspectHere process this cmd in current cn service
func InspectHere(proc *process.Process, cmd string) (*InspectCNContext, error) {
	args, err := shlex.Split(cmd)
	if err != nil {
		return nil, err
	}

	ictx := &InspectCNContext{
		args,
		proc,
		&bytes.Buffer{},
		&bytes.Buffer{},
	}

	runnableCmd := initCommands(ictx)
	err = runnableCmd.Execute()

	return ictx, err
}

func initCommands(inspectCtx *InspectCNContext) *cobra.Command {
	rootCmd := &cobra.Command{
		Use: "inspect cn",
	}

	rootCmd.PersistentFlags().VarPF(inspectCtx, "ictx", "", "").Hidden = true

	rootCmd.SetArgs(inspectCtx.args)
	rootCmd.SetErr(inspectCtx.msg)
	rootCmd.SetOut(inspectCtx.msg)

	rootCmd.CompletionOptions.DisableDefaultCmd = true

	testCmd := &cobra.Command{
		Use:   "test",
		Short: "test inspect cn handler",
		Run:   rpc.RunFactory(&testHandler{}),
	}

	// select mo_ctl("cn", "inspect cn", "uuid:test -t xx");
	testCmd.Flags().StringP("test", "t", "null", "test inspect handler")
	rootCmd.AddCommand(testCmd)

	// mo_ctl("cn", "inspect cn", "uuids:pstate -t * -s *")
	// mo_ctl("cn", "inspect cn", "uuids:pstate -t db.tbl -s snapshot_ts")
	partitionStateCmd := &cobra.Command{
		Use:   "pstate",
		Short: "cmd for operating partition state",
		Run:   rpc.RunFactory(&partitionStateHandler{}),
	}

	partitionStateCmd.Flags().StringP("table", "t", "*", "specify a table")
	partitionStateCmd.Flags().StringP("snapshot ts", "s", "*", "specify a upper bound ts")
	rootCmd.AddCommand(partitionStateCmd)

	return rootCmd
}

//////////////////////////////////// the real cmd handlers //////////////////////////////

type testHandler struct {
	ictx *InspectCNContext
	cmd  string
}

func (t *testHandler) FromCommand(cmd *cobra.Command) error {
	t.ictx = cmd.Flag("ictx").Value.(*InspectCNContext)
	expr, err := cmd.Flags().GetString("test")
	if err != nil {
		return err
	}

	t.cmd = expr
	return nil
}

func (t *testHandler) Run() error {
	t.ictx.out.WriteString("echo " + t.cmd)
	return nil
}

func (t *testHandler) String() string {
	return "test"
}

type partitionStateHandler struct {
	ictx       *InspectCNContext
	db         engine.Database
	tbl        engine.Relation
	snapshotTS types.TS
}

func (p *partitionStateHandler) String() string {
	return "partition state cmd handler"
}

func (p *partitionStateHandler) Run() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1000)
	defer cancel()

	resp, err := inspectcn.DebugPStateTable(ctx, p.db, p.tbl, p.snapshotTS)
	if err != nil {
		return err
	}

	p.ictx.out.WriteString(resp)
	return nil
}

func (p *partitionStateHandler) FromCommand(cmd *cobra.Command) error {
	p.ictx = cmd.Flag("ictx").Value.(*InspectCNContext)

	expr, err := cmd.Flags().GetString("table")
	if err != nil {
		return err
	}

	if expr != "*" {
		args := strings.Split(expr, ".")
		var ctx context.Context

		ctx = context.WithValue(context.Background(), defines.TenantIDKey{}, p.ictx.proc.SessionInfo.AccountId)

		e := p.ictx.proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
		p.db, err = e.Database(ctx, args[0], p.ictx.proc.TxnOperator)
		if err != nil {
			return err
		}

		p.tbl, err = p.db.Relation(ctx, args[1], nil)
		if err != nil {
			return err
		}
	}

	expr, err = cmd.Flags().GetString("snapshot ts")
	if err != nil {
		return err
	}

	if expr != "*" {
		expr = strings.Replace(expr, "-", " ", 1)
		var pp, ll int64
		if _, err = fmt.Sscanf(expr, "%d %d", &pp, &ll); err != nil {
			return err
		}
		p.snapshotTS = types.BuildTS(pp, uint32(ll))
	}

	return nil
}
