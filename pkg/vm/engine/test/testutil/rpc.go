package testutil

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	newdisttae "github.com/matrixorigin/matrixone/pkg/vm/engine/newdisttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail/service"
	"sync"
	"time"
)

var _ TxnOperation = new(MockRPCAgent)

type MockRPCAgent struct {
	client          *MockLogtailRPCClient
	server          *MockLogtailPRCServer
	txnResponseChan chan txn.TxnResponse
	txnRequestChan  chan txn.TxnRequest

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	sessions map[uint64]morpc.ClientSession
}

func (a *MockRPCAgent) Now() timestamp.Timestamp {
	return timestamp.Timestamp{PhysicalTime: time.Now().UnixNano()}
}

func (a *MockRPCAgent) CreateDatabase(ctx context.Context, datType, sql string, accountId, userId,
	roleId uint32, databaseId uint64, databaseName string, m *mpool.MPool) (response *txn.TxnResponse) {

	commitReq, err := newdisttae.MockGenCreateDatabaseCommitRequest(
		datType, sql, accountId, userId, roleId, databaseId, databaseName, m, a.Now())
	if err != nil {
		return &txn.TxnResponse{TxnError: txn.WrapError(err, moerr.ErrTxnError)}
	}

	return a.writeAndCommitRequest(ctx, commitReq)
}

func (a *MockRPCAgent) CreateTable(ctx context.Context, sql string, schema *catalog.Schema,
	tableId uint64, databaseId uint64, databaseName string, m *mpool.MPool) (response *txn.TxnResponse) {

	commitReq, err := newdisttae.MockGenCreateTableCommitRequest(
		sql, schema, tableId, databaseId, databaseName, m, a.Now())

	if err != nil {
		return &txn.TxnResponse{TxnError: txn.WrapError(err, 0)}
	}

	return a.writeAndCommitRequest(ctx, commitReq)
}

func (a *MockRPCAgent) Insert(
	ctx context.Context, accountId uint32, txnTable engine.Relation, databaseName string,
	inBat *containers.Batch, m *mpool.MPool) (response *txn.TxnResponse) {

	bat := containers.ToCNBatch(inBat)

	commitReq, err := newdisttae.MockInsertRowsCommitRequest(
		accountId, txnTable.GetDBID(ctx), databaseName, txnTable.GetTableID(ctx), txnTable.GetTableName(), bat, m, a.Now())

	if err != nil {
		return &txn.TxnResponse{TxnError: txn.WrapError(err, 0)}
	}

	return a.writeAndCommitRequest(ctx, commitReq)
}

func NewMockLogtailAgent() *MockRPCAgent {
	la := new(MockRPCAgent)
	la.client = new(MockLogtailRPCClient)
	la.server = new(MockLogtailPRCServer)

	la.txnResponseChan = make(chan txn.TxnResponse)
	la.txnRequestChan = make(chan txn.TxnRequest)

	la.client.responseReceiver = make(chan morpc.Message)
	la.server.logtailRequestReceiver = make(chan morpc.Message)

	la.client.requestSender = la.server.logtailRequestReceiver

	la.ctx, la.cancel = context.WithCancel(context.Background())
	go la.listenLogtailRequest()

	la.sessions = make(map[uint64]morpc.ClientSession)

	return la
}

func (a *MockRPCAgent) Close() {
	a.cancel()
	close(a.txnRequestChan)
	close(a.txnResponseChan)
	close(a.client.responseReceiver)
	close(a.server.logtailRequestReceiver)
	a.wg.Wait()
}

func (a *MockRPCAgent) MockLogtailRPCClientFactory(
	serverAddr string, ownClient morpc.RPCClient) (morpc.RPCClient, morpc.Stream, error) {
	if a.client == nil {
		a.client = new(MockLogtailRPCClient)
	}

	var err error

	stream, _ := a.client.NewStream("", false)
	return a.client, stream, err
}

func (a *MockRPCAgent) MockLogtailPRCServerFactory(
	name string, address string, logtailServer *service.LogtailServer, options ...morpc.ServerOption) (morpc.RPCServer, error) {
	if a.server == nil {
		a.server = new(MockLogtailPRCServer)
	}

	return a.server, nil
}

type MockLogtailPRCServer struct {
	logtailRequestReceiver chan morpc.Message
	msgHandler             func(ctx context.Context, value morpc.RPCMessage, seq uint64, cs morpc.ClientSession) error
}

func (s *MockLogtailPRCServer) Start() error { return nil }
func (s *MockLogtailPRCServer) Close() error { return nil }
func (s *MockLogtailPRCServer) RegisterRequestHandler(
	onMessage func(ctx context.Context, request morpc.RPCMessage, sequence uint64, cs morpc.ClientSession) error) {

	s.msgHandler = onMessage
}

type MockLogtailRPCClient struct {
	responseReceiver chan morpc.Message
	requestSender    chan morpc.Message
}

func (c *MockLogtailRPCClient) Send(ctx context.Context, backend string, request morpc.Message) (*morpc.Future, error) {
	return nil, nil
}

func (c *MockLogtailRPCClient) NewStream(backend string, lock bool) (morpc.Stream, error) {
	stream := new(MockRPCClientStream)
	stream.receiver = c.responseReceiver
	stream.sender = c.requestSender

	uid := uuid.New()
	stream.id = types.DecodeUint64(uid[:8]) * types.DecodeUint64(uid[8:])

	return stream, nil
}

func (c *MockLogtailRPCClient) Ping(ctx context.Context, backend string) error {
	return nil
}

func (c *MockLogtailRPCClient) Close() error {
	return nil
}

func (c *MockLogtailRPCClient) CloseBackend() error {
	return nil
}

type MockRPCClientStream struct {
	id       uint64
	receiver chan morpc.Message
	sender   chan morpc.Message
}

func (s *MockRPCClientStream) ID() uint64 {
	return s.id
}

func (s *MockRPCClientStream) Send(ctx context.Context, request morpc.Message) error {
	request.SetID(s.ID())
	s.sender <- request
	return nil
}

func (s *MockRPCClientStream) Receive() (chan morpc.Message, error) {
	return s.receiver, nil
}

func (s *MockRPCClientStream) Close(closeConn bool) error {
	close(s.receiver)
	return nil
}

func (a *MockRPCAgent) writeAndCommitRequest(
	ctx context.Context, commitReq []*txn.TxnRequest) (response *txn.TxnResponse) {

	reqs := txn.TxnRequest{
		Method: txn.TxnMethod_Commit,
		Flag:   txn.SkipResponseFlag,
		CommitRequest: &txn.TxnCommitRequest{
			Payload:       commitReq,
			Disable1PCOpt: false,
		}}

	a.txnRequestChan <- reqs
	resp := <-a.txnResponseChan

	return &resp
}

func (a *MockRPCAgent) listenLogtailRequest() {
	a.wg.Add(1)
	defer a.wg.Done()

	var session morpc.ClientSession
	for {
		select {
		case <-a.ctx.Done():
			return

		case request, ok := <-a.server.logtailRequestReceiver:
			if !ok {
				fmt.Println("logtail request receiver closed")
				return
			}

			rpcMsg := morpc.RPCMessage{Message: request}

			if session, ok = a.sessions[request.GetID()]; !ok {
				session = newTestClientSession(a.client.responseReceiver)
				a.sessions[request.GetID()] = session
			}

			err := a.server.msgHandler(a.ctx, rpcMsg, 0, session)
			if err != nil {
				fmt.Println("a.server.msgHandler failed", err)
				return
			}
		}
	}
}
