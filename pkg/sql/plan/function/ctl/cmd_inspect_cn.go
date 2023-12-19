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
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
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
	args    []string
	accInfo *query.AccessInfo
	resp    *query.InspectCNResponse
	msg     *bytes.Buffer
	out     *bytes.Buffer
}

func (i *InspectCNContext) String() string   { return "" }
func (i *InspectCNContext) Set(string) error { return nil }
func (i *InspectCNContext) Type() string     { return "ictx" }

// cmd format like:
// mo_ctl("cn", "inspect cn", "*:the real cmd")
// mo_ctl("cn", "inspect cn", "cn_uuid1,cn_uuid2,...,:the real cmd")
func handleInspectCN(
	proc *process.Process,
	service serviceType,
	parameter string,
	sender requestSender) (Result, error) {

	uuids, cmd, err := parseInspectArgs(parameter)
	if err != nil {
		return Result{
			Method: InspectCNMethod,
		}, err
	}

	var errs []error
	var resps []*query.InspectCNResponse

	var resp *query.InspectCNResponse

	accInfo := &query.AccessInfo{
		UserID:    proc.SessionInfo.UserId,
		RoleID:    proc.SessionInfo.RoleId,
		AccountID: proc.SessionInfo.AccountId,
	}

	for idx := range uuids {
		if uuids[idx] == proc.QueryService.ServiceID() {
			resp, err = InspectHere(accInfo, cmd)
		} else {
			resp, err = dispatch2Peer(proc, uuids[idx], cmd)
		}
		errs = append(errs, err)
		resps = append(resps, resp)
	}

	return fmtFinalOutput(uuids, resps, errs)

}

func fmtFinalOutput(uuids []string, resps []*query.InspectCNResponse, errs []error) (Result, error) {
	var out bytes.Buffer
	for idx, cn := range uuids {
		out.WriteString(fmt.Sprintf("\n\n%s: ", cn))
		out.WriteString("\n    ")
		if errs[idx] != nil {
			out.WriteString(fmt.Sprintf("err happend: %s", errs[idx].Error()))
		} else {
			out.WriteString(fmt.Sprintf("message: %v\n    %v", resps[idx].Message, string(resps[idx].Payload)))
		}
		out.WriteString("\n")
	}
	return Result{
		Method: InspectCNMethod,
		Data:   out.Bytes(),
	}, nil
}

func parseInspectArgs(param string) (uuids []string, cmd string, err error) {
	args := strings.Split(param, ":")
	if len(args) != 2 {
		return nil, "", moerr.NewInvalidArgNoCtx(param, "")
	}

	if args[0] == "*" {
		clusterservice.GetMOCluster().GetCNService(clusterservice.Selector{}, func(cn metadata.CNService) bool {
			uuids = append(uuids, cn.ServiceID)
			return true
		})
	} else {
		uuids = strings.Split(args[0], ",")
	}

	cmd = args[1]
	return
}

func dispatch2Peer(proc *process.Process, target string, cmd string) (*query.InspectCNResponse, error) {
	var resp *query.Response
	var err error
	var found bool

	clusterservice.GetMOCluster().GetCNService(clusterservice.NewServiceIDSelector(target),
		func(cn metadata.CNService) bool {
			request := proc.QueryService.NewRequest(query.CmdMethod_InspectCN)
			request.InspectCNRequest = &query.InspectCNRequest{
				Cmd: cmd,
				AccessInfo: &query.AccessInfo{
					RoleID:    proc.SessionInfo.RoleId,
					UserID:    proc.SessionInfo.RoleId,
					AccountID: proc.SessionInfo.AccountId,
				},
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			resp, err = proc.QueryService.SendMessage(ctx, cn.QueryAddress, request)

			found = true
			return true
		})

	if err != nil {
		return nil, err
	}

	if !found {
		return nil, moerr.NewInternalErrorNoCtx(fmt.Sprintf("no such cn: %s", target))
	}

	return resp.InspectCNResponse, nil
}

func InspectHere(accInfo *query.AccessInfo, cmd string) (*query.InspectCNResponse, error) {
	args, err := shlex.Split(cmd)
	if err != nil {
		return nil, err
	}

	ctx := &InspectCNContext{
		args,
		accInfo,
		&query.InspectCNResponse{},
		&bytes.Buffer{},
		&bytes.Buffer{},
	}

	runnableCmd := initCommands(ctx)
	err = runnableCmd.Execute()

	ctx.resp.Message = ctx.msg.String()
	ctx.resp.Payload = ctx.out.Bytes()

	return ctx.resp, err
}

func initCommands(inspectCtx *InspectCNContext) *cobra.Command {
	rootCmd := &cobra.Command{
		Use: "inspect",
	}

	rootCmd.PersistentFlags().VarPF(inspectCtx, "ictx", "", "").Hidden = true

	rootCmd.SetArgs(inspectCtx.args)
	rootCmd.SetErr(inspectCtx.msg)
	rootCmd.SetOut(inspectCtx.msg)

	rootCmd.CompletionOptions.DisableDefaultCmd = true

	testCmd := &cobra.Command{
		Use:   "test",
		Short: "test inspect cn handler",
		Run:   rpc.RunFactory(&TestHandler{}),
	}

	// select mo_ctl("cn", "inspect cn", "uuid:test -t xx");
	testCmd.Flags().StringP("test", "t", "null", "test inspect handler")

	rootCmd.AddCommand(testCmd)
	return rootCmd
}

//////////////////////////////////// the real cmd handlers //////////////////////////////

type TestHandler struct {
	ctx *InspectCNContext
	cmd string
}

func (t *TestHandler) FromCommand(cmd *cobra.Command) error {
	t.ctx = cmd.Flag("ictx").Value.(*InspectCNContext)
	expr, err := cmd.Flags().GetString("test")
	if err != nil {
		return err
	}

	t.cmd = expr
	return nil
}

func (t *TestHandler) Run() error {
	t.ctx.out.WriteString("echo " + t.cmd)
	return nil
}

func (t *TestHandler) String() string {
	return "test"
}
