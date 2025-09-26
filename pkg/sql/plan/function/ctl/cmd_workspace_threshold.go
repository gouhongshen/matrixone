// Copyright 2024 Matrix Origin
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
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type Res struct {
	PodID                    string  `json:"pod_id,omitempty"`
	SingleEntrySizeThreshold float64 `json:"single_entry_size_threshold,omitempty"`
	AccumulatedSizeThreshold float64 `json:"accumulated_size_threshold,omitempty"`
	ErrorStr                 string  `json:"error,omitempty"`
}

// select mo_ctl("cn", "WorkspaceThreshold", "x:y")
// select mo_ctl("cn", "WorkspaceThreshold", "0.1:0.1")
// x := single entry size threshold (in mb)
// y := accumulated size threshold  (in mb)
// unlimited if x/y equals 0
// "x:" means only change the single threshold
// ":y" means only change the accumulated threshold
func handleWorkspaceThreshold(
	proc *process.Process,
	service serviceType,
	parameter string,
	sender requestSender,
) (Result, error) {
	if service != cn {
		return Result{}, moerr.NewWrongServiceNoCtx("expected CN", string(service))
	}

	var (
		err         error
		single      = float64(-1)
		accumulated = float64(-1)
	)

	thresholds := strings.Split(parameter, ":")
	if len(thresholds) != 2 {
		return Result{}, moerr.NewInvalidInput(proc.Ctx, "invalid parameter")
	}

	if len(thresholds[0]) != 0 {
		if single, err = strconv.ParseFloat(thresholds[0], 10); err != nil {
			return Result{}, moerr.NewInvalidInput(proc.Ctx, fmt.Sprintf("single threshold: %v", err))
		}
	}

	if len(thresholds[1]) != 0 {
		if accumulated, err = strconv.ParseFloat(thresholds[1], 10); err != nil {
			return Result{}, moerr.NewInvalidInput(proc.Ctx, fmt.Sprintf("accumulated threshold: %v", err))
		}
	}

	request := proc.GetQueryClient().NewRequest(query.CmdMethod_WorkspaceThreshold)
	request.WorkspaceThresholdRequest = &query.WorkspaceThresholdRequest{
		SingleEntrySizeThreshold: single,
		AccumulatedSizeThreshold: accumulated,
	}

	results := make([]Res, 0)

	clusterservice.GetMOCluster(
		proc.GetService()).GetCNService(clusterservice.Selector{}, func(cn metadata.CNService) bool {
		ctx, cancel := context.WithTimeoutCause(context.Background(), time.Second, moerr.CauseTransferRequest2OtherCNs)
		defer cancel()

		resp, err := proc.GetQueryClient().SendMessage(ctx, cn.QueryAddress, request)
		err = moerr.AttachCause(ctx, err)

		res := Res{
			PodID: cn.ServiceID,
		}

		if err != nil {
			res.ErrorStr = err.Error()
		} else {
			res.SingleEntrySizeThreshold = resp.WorkspaceThresholdResponse.SingleEntrySizeThreshold
			res.AccumulatedSizeThreshold = resp.WorkspaceThresholdResponse.AccumulatedSizeThreshold
		}
		results = append(results, res)
		return true
	})

	return Result{
		Method: WorkspaceThreshold,
		Data:   results,
	}, nil
}
