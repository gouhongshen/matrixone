// Copyright 2021 Matrix Origin
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

package compile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/models"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/udf"
	"github.com/matrixorigin/matrixone/pkg/util/debug/goroutine"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.uber.org/zap"
)

// CnServerMessageHandler receive and deal the message from cn-client.
//
// The message should always *pipeline.Message here.
// there are 2 types of pipeline message now.
//
//  1. notify message :
//     a message to tell the dispatch pipeline where its remote receiver are.
//     and we use this connection's write-back method to send the data.
//     or
//     a message to stop the running pipeline.
//
//  2. scope message :
//     a message contains the encoded pipeline.
//     we decoded it and run it locally.
func CnServerMessageHandler(
	ctx context.Context,
	serverAddress string,
	message morpc.Message,
	cs morpc.ClientSession,
	storageEngine engine.Engine, fileService fileservice.FileService, lockService lockservice.LockService,
	queryClient qclient.QueryClient,
	HaKeeper logservice.CNHAKeeperClient, udfService udf.Service, txnClient client.TxnClient,
	autoIncreaseCM *defines.AutoIncrCacheManager,
	messageAcquirer func() morpc.Message) (err error) {

	startTime := time.Now()
	defer func() {
		v2.PipelineServerDurationHistogram.Observe(time.Since(startTime).Seconds())

		if e := recover(); e != nil {
			err = moerr.ConvertPanicError(ctx, e)
			getLogger(lockService.GetConfig().ServiceID).Error("panic in CnServerMessageHandler",
				zap.String("error", err.Error()))
			err = errors.Join(err, cs.Close())
		}
	}()

	// check message is valid.
	msg, ok := message.(*pipeline.Message)
	if !ok {
		logutil.Errorf("cn server should receive *pipeline.Message, but get %v", message)
		panic("cn server receive a message with unexpected type")
	}
	if msg.DebugMsg != "" {
		logutil.Infof("%s, goRoutineId=%d", msg.GetDebugMsg(), goroutine.GetRoutineId())
	}

	// prepare the receiver structure, just for easy using the `send` method.
	receiver := newMessageReceiverOnServer(ctx, serverAddress, msg,
		cs, messageAcquirer, storageEngine, fileService, lockService, queryClient, HaKeeper, udfService, txnClient, autoIncreaseCM)

	// how to handle the *pipeline.Message.
	err = handlePipelineMessage(&receiver)
	if receiver.messageTyp != pipeline.Method_StopSending {
		// stop message only close a running pipeline, there is no need to reply the finished-message.
		if err != nil {
			logutil.Infof("[CN1-CANCEL] handlePipelineMessage returned error, messageId=%d, sending ErrorMessage, err=%v", 
				receiver.messageId, err)
			err = receiver.sendError(err)
		} else {
			logutil.Infof("[CN1-CANCEL] handlePipelineMessage returned nil, messageId=%d, sending EndMessage", 
				receiver.messageId)
			err = receiver.sendEndMessage()
			logutil.Infof("[CN1-CANCEL] EndMessage sent, messageId=%d, sendErr=%v", 
				receiver.messageId, err)
		}
	} else {
		logutil.Infof("[CN1-CANCEL] StopSending message, messageId=%d, skipping sendEndMessage", 
			receiver.messageId)
	}

	// if this message is responsible for the execution of certain pipelines, they should be ended after message processing is completed.
	if receiver.messageTyp == pipeline.Method_PipelineMessage || receiver.messageTyp == pipeline.Method_PrepareDoneNotifyMessage {
		// keep listening until connection was closed
		// to prevent some strange handle order between 'stop sending message' and others.
		// todo: it is tcp connection now. should be very careful, we should listen to stream context next day.
		if err == nil {
			logutil.Infof("[CN1-CANCEL] waiting for connectionCtx.Done(), messageId=%d, messageTyp=%v", 
				receiver.messageId, receiver.messageTyp)
			<-receiver.connectionCtx.Done()
			logutil.Infof("[CN1-CANCEL] connectionCtx.Done() triggered, messageId=%d, err=%v", 
				receiver.messageId, receiver.connectionCtx.Err())
		}
		colexec.Get().RemoveRelatedPipeline(receiver.clientSession, receiver.messageId)
	}
	return err
}

func handlePipelineMessage(receiver *messageReceiverOnServer) error {

	switch receiver.messageTyp {
	case pipeline.Method_PrepareDoneNotifyMessage:
		dispatchProc, dispatchNotifyCh, err := receiver.GetProcByUuid(receiver.messageUuid, HandleNotifyTimeout)
		if err != nil || dispatchProc == nil {
			return err
		}

		infoToDispatchOperator := &process.WrapCs{
			ReceiverDone: false,
			MsgId:        receiver.messageId,
			Uid:          receiver.messageUuid,
			Cs:           receiver.clientSession,
			Err:          make(chan error, 1),
		}
		colexec.Get().RecordDispatchPipeline(receiver.clientSession, receiver.messageId, infoToDispatchOperator)

		// todo : the timeout should be removed.
		//		but I keep it here because I don't know whether it will cause hung sometimes.
		timeLimit, cancel := context.WithTimeoutCause(context.TODO(), HandleNotifyTimeout, moerr.CauseHandlePipelineMessage)

		succeed := false
		select {
		case <-timeLimit.Done():
			err = moerr.NewInternalError(receiver.messageCtx, "send notify msg to dispatch operator timeout")
			err = moerr.AttachCause(timeLimit, err)
		case dispatchNotifyCh <- infoToDispatchOperator:
			succeed = true
		case <-receiver.connectionCtx.Done():
			logutil.Infof("[CN1-CANCEL] connectionCtx.Done() in PrepareDoneNotifyMessage select, messageId=%d, err=%v", 
				receiver.messageId, receiver.connectionCtx.Err())
		case <-dispatchProc.Ctx.Done():
			logutil.Infof("[CN1-CANCEL] dispatchProc.Ctx.Done() in PrepareDoneNotifyMessage select, messageId=%d, err=%v", 
				receiver.messageId, dispatchProc.Ctx.Err())
		}
		cancel()

		if err != nil || !succeed {
			dispatchProc.Cancel(err)
			return err
		}

		select {
		case <-receiver.connectionCtx.Done():
			logutil.Infof("[CN1-CANCEL] connectionCtx.Done() in PrepareDoneNotifyMessage error select, messageId=%d, err=%v, calling dispatchProc.Cancel", 
				receiver.messageId, receiver.connectionCtx.Err())
			dispatchProc.Cancel(err)

		// there is no need to check the dispatchProc.Ctx.Done() here.
		// because we need to receive the error from dispatchProc.DispatchNotifyCh.
		case err = <-infoToDispatchOperator.Err:
		}
		return err

	case pipeline.Method_PipelineMessage:
		runCompile, errBuildCompile := receiver.newCompile()
		if errBuildCompile != nil {
			return errBuildCompile
		}

		// decode and running the pipeline.
		s, err := decodeScope(receiver.scopeData, runCompile.proc, true, runCompile.e)
		if err != nil {
			return err
		}
		if !receiver.needNotReply {
			s = appendWriteBackOperator(runCompile, s)
		}

		runCompile.scopes = []*Scope{s}
		runCompile.InitPipelineContextToExecuteQuery()
		defer func() {
			runCompile.clear()
			runCompile.Release()
		}()

		colexec.Get().RecordBuiltPipeline(receiver.clientSession, receiver.messageId, runCompile.proc)

		// running pipeline.
		MarkQueryRunning(runCompile, runCompile.proc.GetTxnOperator())
		defer func() {
			MarkQueryDone(runCompile, runCompile.proc.GetTxnOperator())
		}()

		// Check ctx status before MergeRun - ALWAYS LOG
		logutil.Infof("[CN1-CANCEL] before MergeRun, messageId=%d, messageCtx.Err()=%v, connectionCtx.Err()=%v, proc.Ctx.Err()=%v", 
			receiver.messageId, receiver.messageCtx.Err(), receiver.connectionCtx.Err(), runCompile.proc.Ctx.Err())

		err = s.MergeRun(runCompile)

		// Check ctx status after MergeRun - ALWAYS LOG
		logutil.Infof("[CN1-CANCEL] after MergeRun, messageId=%d, err=%v, messageCtx.Err()=%v, connectionCtx.Err()=%v, proc.Ctx.Err()=%v", 
			receiver.messageId, err, receiver.messageCtx.Err(), receiver.connectionCtx.Err(), runCompile.proc.Ctx.Err())

		if err == nil {
			runCompile.GenPhyPlan(runCompile)
			receiver.phyPlan = runCompile.anal.GetPhyPlan()
		}

		return err

	case pipeline.Method_StopSending:
		logutil.Infof("[CN1-CANCEL] received StopSending message, messageId=%d, session=%s", 
			receiver.messageId, receiver.clientSession.RemoteAddress())
		colexec.Get().CancelPipelineSending(receiver.clientSession, receiver.messageId)
		logutil.Infof("[CN1-CANCEL] CancelPipelineSending called, messageId=%d", receiver.messageId)

	default:
		panic(fmt.Sprintf("unknown pipeline message type %d.", receiver.messageTyp))
	}
	return nil
}

const (
	maxMessageSizeToMoRpc = 64 * mpool.MB

	// HandleNotifyTimeout
	// todo: this is a bad design here.
	//  we should do the waiting work in the prepare stage of the dispatch operator but not in the exec stage.
	//      do the waiting work in the exec stage can save some execution time, but it will cause an unstable waiting time.
	//		(because we cannot control the execution time of the running sql,
	//		and the coming time of the first batch of the result is not a constant time.)
	// 		see the codes in pkg/sql/colexec/dispatch/dispatch.go:waitRemoteRegsReady()
	//
	// need to fix this in the future. for now, increase it to make tpch1T can run on 3 CN
	HandleNotifyTimeout = 300 * time.Second
)

// message receiver's cn information.
type cnInformation struct {
	cnAddr      string
	storeEngine engine.Engine
	fileService fileservice.FileService
	lockService lockservice.LockService
	queryClient qclient.QueryClient
	hakeeper    logservice.CNHAKeeperClient
	udfService  udf.Service
	aicm        *defines.AutoIncrCacheManager
}

// information to rebuild a process.
type processHelper struct {
	id          string
	lim         process.Limitation
	unixTime    int64
	accountId   uint32
	txnOperator client.TxnOperator
	txnClient   client.TxnClient
	sessionInfo process.SessionInfo
	//analysisNodeList []int32
	StmtId        uuid.UUID
	prepareParams *vector.Vector
}

// messageReceiverOnServer supported a series methods to write back results.
type messageReceiverOnServer struct {
	messageCtx    context.Context
	connectionCtx context.Context

	messageId   uint64
	messageTyp  pipeline.Method
	messageUuid uuid.UUID

	cnInformation cnInformation
	// information to build a process.
	procBuildHelper processHelper

	clientSession   morpc.ClientSession
	messageAcquirer func() morpc.Message
	maxMessageSize  int
	scopeData       []byte

	needNotReply bool

	// result.
	phyPlan *models.PhyPlan
}

func newMessageReceiverOnServer(
	ctx context.Context,
	cnAddr string,
	m *pipeline.Message,
	cs morpc.ClientSession,
	messageAcquirer func() morpc.Message,
	storeEngine engine.Engine,
	fileService fileservice.FileService,
	lockService lockservice.LockService,
	queryClient qclient.QueryClient,
	hakeeper logservice.CNHAKeeperClient,
	udfService udf.Service,
	txnClient client.TxnClient,
	aicm *defines.AutoIncrCacheManager) messageReceiverOnServer {

	receiver := messageReceiverOnServer{
		messageCtx:      ctx,
		connectionCtx:   cs.SessionCtx(),
		messageId:       m.GetId(),
		messageTyp:      m.GetCmd(),
		clientSession:   cs,
		messageAcquirer: messageAcquirer,
		maxMessageSize:  maxMessageSizeToMoRpc,
		needNotReply:    m.NeedNotReply,
	}
	receiver.cnInformation = cnInformation{
		cnAddr:      cnAddr,
		storeEngine: storeEngine,
		fileService: fileService,
		lockService: lockService,
		queryClient: queryClient,
		hakeeper:    hakeeper,
		udfService:  udfService,
		aicm:        aicm,
	}

	switch m.GetCmd() {
	case pipeline.Method_PrepareDoneNotifyMessage:
		opUuid, err := uuid.FromBytes(m.GetUuid())
		if err != nil {
			logutil.Errorf("decode uuid from pipeline.Message failed, bytes are %v", m.GetUuid())
			panic("cn receive a message with wrong uuid bytes")
		}
		receiver.messageUuid = opUuid

	case pipeline.Method_PipelineMessage:
		var err error
		receiver.procBuildHelper, err = generateProcessHelper(m.GetProcInfoData(), txnClient)
		if err != nil {
			logutil.Errorf("decode process info from pipeline.Message failed, bytes are %v", m.GetProcInfoData())
			panic("cn receive a message with wrong process bytes")
		}
		receiver.scopeData = m.Data
	}

	return receiver
}

func (receiver *messageReceiverOnServer) acquireMessage() (*pipeline.Message, error) {
	message, ok := receiver.messageAcquirer().(*pipeline.Message)
	if !ok {
		return nil, moerr.NewInternalError(receiver.messageCtx, "get a message with wrong type.")
	}
	message.SetID(receiver.messageId)
	return message, nil
}

// newCompile make and return a new compile to run a pipeline.
func (receiver *messageReceiverOnServer) newCompile() (*Compile, error) {
	// compile is almost surely wanting a small or middle pool.  Later.
	mp, err := mpool.NewMPool("compile", 0, mpool.NoFixed)
	if err != nil {
		return nil, err
	}
	pHelper, cnInfo := receiver.procBuildHelper, receiver.cnInformation

	// required deadline.
	runningCtx := defines.AttachAccountId(receiver.messageCtx, pHelper.accountId)
	
	// Log ctx status when creating proc
	if receiver.messageCtx.Err() != nil {
		logutil.Infof("[CN1-CANCEL] messageCtx already canceled when creating proc, messageId=%d, err=%v", 
			receiver.messageId, receiver.messageCtx.Err())
	}
	if receiver.connectionCtx.Err() != nil {
		logutil.Infof("[CN1-CANCEL] connectionCtx already canceled when creating proc, messageId=%d, err=%v", 
			receiver.messageId, receiver.connectionCtx.Err())
	}
	
	proc := process.NewTopProcess(
		runningCtx,
		mp,
		pHelper.txnClient,
		pHelper.txnOperator,
		cnInfo.fileService,
		cnInfo.lockService,
		cnInfo.queryClient,
		cnInfo.hakeeper,
		cnInfo.udfService,
		cnInfo.aicm)
	
	logutil.Infof("[CN1-CANCEL] proc created, messageId=%d, proc.Ctx.Err()=%v", 
		receiver.messageId, proc.Ctx.Err())
	proc.Base.UnixTime = pHelper.unixTime
	proc.Base.Id = pHelper.id
	proc.Base.Lim = pHelper.lim
	proc.Base.SessionInfo = pHelper.sessionInfo
	proc.Base.SessionInfo.StorageEngine = cnInfo.storeEngine
	proc.SetPrepareParams(pHelper.prepareParams)
	{
		txn := proc.GetTxnOperator().Txn()
		txnId := txn.GetID()
		proc.Base.StmtProfile = process.NewStmtProfile(uuid.UUID(txnId), pHelper.StmtId)
	}

	c := allocateNewCompile(proc)
	c.execType = plan2.ExecTypeAP_MULTICN
	c.e = cnInfo.storeEngine
	c.MessageBoard = c.MessageBoard.SetMultiCN(c.GetMessageCenter(), c.proc.GetStmtProfile().GetStmtId())
	c.proc.SetMessageBoard(c.MessageBoard)
	c.anal = newAnalyzeModule()
	c.addr = receiver.cnInformation.cnAddr

	// a method to send back.
	c.execType = plan2.ExecTypeAP_MULTICN
	c.fill = func(b *batch.Batch, counter *perfcounter.CounterSet) error {
		return receiver.sendBatch(b)
	}

	return c, nil
}

func (receiver *messageReceiverOnServer) sendError(
	errInfo error) error {
	message, err := receiver.acquireMessage()
	if err != nil {
		return err
	}
	message.SetID(receiver.messageId)
	message.SetSid(pipeline.Status_MessageEnd)
	if errInfo != nil {
		message.SetMoError(receiver.messageCtx, errInfo)
	}
	return receiver.clientSession.Write(receiver.messageCtx, message)
}

func (receiver *messageReceiverOnServer) sendBatch(
	b *batch.Batch) error {
	// there's no need to send the nil batch.
	if b == nil {
		logutil.Infof("[CN1-SENDBATCH] sendBatch called with nil batch, messageId=%d", receiver.messageId)
		return nil
	}

	rowCount := b.RowCount()
	logutil.Infof("[CN1-SENDBATCH] sendBatch called, messageId=%d, rowCount=%d, Attrs=%v", 
		receiver.messageId, rowCount, b.Attrs)

	data, err := b.MarshalBinary()
	if err != nil {
		logutil.Errorf("[CN1-SENDBATCH] sendBatch MarshalBinary failed, messageId=%d, err=%v", 
			receiver.messageId, err)
		return err
	}

	dataLen := len(data)
	if dataLen <= receiver.maxMessageSize {
		m, errA := receiver.acquireMessage()
		if errA != nil {
			logutil.Errorf("[CN1-SENDBATCH] sendBatch acquireMessage failed, messageId=%d, err=%v", 
				receiver.messageId, errA)
			return errA
		}
		m.SetMessageType(pipeline.Method_BatchMessage)
		m.SetData(data)
		m.SetSid(pipeline.Status_Last)
		errW := receiver.clientSession.Write(receiver.messageCtx, m)
		if errW != nil {
			logutil.Errorf("[CN1-SENDBATCH] sendBatch Write failed, messageId=%d, rowCount=%d, err=%v", 
				receiver.messageId, rowCount, errW)
		} else {
			logutil.Infof("[CN1-SENDBATCH] sendBatch Write success, messageId=%d, rowCount=%d, dataLen=%d", 
				receiver.messageId, rowCount, dataLen)
		}
		return errW
	}
	// if data is too large, cut and send
	chunkCount := 0
	for start, end := 0, 0; start < dataLen; start = end {
		chunkCount++
		m, errA := receiver.acquireMessage()
		if errA != nil {
			logutil.Errorf("[CN1-SENDBATCH] sendBatch acquireMessage failed (large data), messageId=%d, chunk=%d, err=%v", 
				receiver.messageId, chunkCount, errA)
			return errA
		}
		end = start + receiver.maxMessageSize
		if end >= dataLen {
			end = dataLen
			m.SetSid(pipeline.Status_Last)
		} else {
			m.SetSid(pipeline.Status_WaitingNext)
		}
		m.SetMessageType(pipeline.Method_BatchMessage)
		m.SetData(data[start:end])

		if errW := receiver.clientSession.Write(receiver.messageCtx, m); errW != nil {
			logutil.Errorf("[CN1-SENDBATCH] sendBatch Write failed (large data), messageId=%d, chunk=%d, rowCount=%d, err=%v", 
				receiver.messageId, chunkCount, rowCount, errW)
			return errW
		}
		logutil.Infof("[CN1-SENDBATCH] sendBatch Write success (large data), messageId=%d, chunk=%d/%d, rowCount=%d, chunkSize=%d", 
			receiver.messageId, chunkCount, (dataLen+receiver.maxMessageSize-1)/receiver.maxMessageSize, rowCount, end-start)
	}
	logutil.Infof("[CN1-SENDBATCH] sendBatch completed (large data), messageId=%d, rowCount=%d, totalChunks=%d", 
		receiver.messageId, rowCount, chunkCount)
	return nil
}

func (receiver *messageReceiverOnServer) sendEndMessage() error {
	message, err := receiver.acquireMessage()
	if err != nil {
		return err
	}
	message.SetSid(pipeline.Status_MessageEnd)
	message.SetID(receiver.messageId)
	message.SetMessageType(receiver.messageTyp)

	jsonData, err := json.MarshalIndent(receiver.phyPlan, "", "  ")
	if err != nil {
		return err
	}
	message.SetAnalysis(jsonData)

	return receiver.clientSession.Write(receiver.messageCtx, message)
}

func generateProcessHelper(data []byte, cli client.TxnClient) (processHelper, error) {
	procInfo := &pipeline.ProcessInfo{}
	err := procInfo.Unmarshal(data)
	if err != nil {
		return processHelper{}, err
	}

	result := processHelper{
		id:        procInfo.Id,
		lim:       process.ConvertToProcessLimitation(procInfo.Lim),
		unixTime:  procInfo.UnixTime,
		accountId: procInfo.AccountId,
		txnClient: cli,
	}
	if procInfo.PrepareParams.Length > 0 {
		result.prepareParams = vector.NewVecWithData(
			types.T_text.ToType(),
			int(procInfo.PrepareParams.Length),
			procInfo.PrepareParams.Data,
			procInfo.PrepareParams.Area,
		)
		for i := range procInfo.PrepareParams.Nulls {
			if procInfo.PrepareParams.Nulls[i] {
				result.prepareParams.GetNulls().Add(uint64(i))
			}
		}
	}
	result.txnOperator, err = cli.NewWithSnapshot(procInfo.Snapshot)
	if err != nil {
		return processHelper{}, err
	}
	result.sessionInfo, err = process.ConvertToProcessSessionInfo(procInfo.SessionInfo)
	if err != nil {
		return processHelper{}, err
	}
	if sessLogger := procInfo.SessionLogger; len(sessLogger.SessId) > 0 {
		copy(result.sessionInfo.SessionId[:], sessLogger.SessId)
		copy(result.StmtId[:], sessLogger.StmtId)
		result.sessionInfo.LogLevel = process.EnumLogLevel2ZapLogLevel(sessLogger.LogLevel)
		// txnId, ignore. more in txnOperator.
	}

	return result, nil
}

func (receiver *messageReceiverOnServer) GetProcByUuid(uid uuid.UUID, timeout time.Duration) (*process.Process, process.RemotePipelineInformationChannel, error) {
	tout, tcancel := context.WithTimeoutCause(context.Background(), timeout, moerr.CauseGetProcByUuid)

	for {
		select {
		case <-tout.Done():
			colexec.Get().GetProcByUuid(uid, true)
			err := moerr.AttachCause(tout, moerr.NewInternalError(receiver.messageCtx, "get dispatch process by uuid timeout"))
			tcancel()
			return nil, nil, err

		case <-receiver.connectionCtx.Done():
			logutil.Infof("[CN1-CANCEL] connectionCtx.Done() in GetProcByUuid, messageId=%d, err=%v", 
				receiver.messageId, receiver.connectionCtx.Err())
			colexec.Get().GetProcByUuid(uid, true)
			tcancel()
			return nil, nil, nil

		default:
			dispatchProc, notifyChannel, ok := colexec.Get().GetProcByUuid(uid, false)
			if ok {
				tcancel()
				return dispatchProc, notifyChannel, nil
			}

			// it's very bad to call the runtime.Gosched() here.
			// get a process receive channel first, and listen to it may be a better way.
			runtime.Gosched()
		}
	}
}
