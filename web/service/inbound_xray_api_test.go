package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/mhsanaei/3x-ui/v2/database"
	"github.com/mhsanaei/3x-ui/v2/database/model"
	"github.com/mhsanaei/3x-ui/v2/logger"
	"github.com/mhsanaei/3x-ui/v2/xray"
	"github.com/op/go-logging"
	"github.com/xtls/xray-core/app/proxyman/command"
	"google.golang.org/grpc"
)

type xrayAPICall struct {
	op, tag, email string
}

// fakeXrayHandler is a real gRPC HandlerService that records user operations instead of applying them.
type fakeXrayHandler struct {
	command.UnimplementedHandlerServiceServer
	mu    sync.Mutex
	calls []xrayAPICall
}

func (f *fakeXrayHandler) AlterInbound(_ context.Context, req *command.AlterInboundRequest) (*command.AlterInboundResponse, error) {
	op, err := req.Operation.GetInstance()
	if err != nil {
		return nil, err
	}
	call := xrayAPICall{tag: req.Tag}
	switch o := op.(type) {
	case *command.AddUserOperation:
		call.op, call.email = "add", o.User.Email
	case *command.RemoveUserOperation:
		call.op, call.email = "remove", o.Email
	default:
		return nil, fmt.Errorf("unexpected operation %T", op)
	}
	f.mu.Lock()
	f.calls = append(f.calls, call)
	f.mu.Unlock()
	return &command.AlterInboundResponse{}, nil
}

func (f *fakeXrayHandler) takeCalls() []xrayAPICall {
	f.mu.Lock()
	defer f.mu.Unlock()
	calls := f.calls
	f.calls = nil
	return calls
}

var serviceTestLoggerOnce sync.Once

func setupXrayAPITest(t *testing.T) *fakeXrayHandler {
	t.Helper()
	serviceTestLoggerOnce.Do(func() { logger.InitLogger(logging.ERROR) })

	dir := t.TempDir()
	t.Setenv("XUI_DB_FOLDER", dir)
	t.Setenv("XUI_LOG_FOLDER", dir)
	if err := database.InitDB(filepath.Join(dir, "x-ui.db")); err != nil {
		t.Fatalf("InitDB: %v", err)
	}
	t.Cleanup(func() { database.CloseDB() })

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	fake := &fakeXrayHandler{}
	command.RegisterHandlerServiceServer(srv, fake)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	prev := p
	p = xray.NewProcess(&xray.Config{InboundConfigs: []xray.InboundConfig{{Tag: "api", Port: lis.Addr().(*net.TCPAddr).Port}}})
	t.Cleanup(func() { p = prev })
	return fake
}

func clientSettings(t *testing.T, clients ...model.Client) string {
	t.Helper()
	b, err := json.Marshal(map[string]any{"clients": clients, "decryption": "none"})
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// seedVlessInbound stores a disabled inbound so seeding itself never reaches the Xray API.
func seedVlessInbound(t *testing.T, s *InboundService, port int, clients ...model.Client) *model.Inbound {
	t.Helper()
	inbound := &model.Inbound{
		UserId:         1,
		Port:           port,
		Protocol:       model.VLESS,
		Tag:            fmt.Sprintf("inbound-%d", port),
		Settings:       clientSettings(t, clients...),
		StreamSettings: `{"network":"tcp","security":"none"}`,
		Sniffing:       `{}`,
	}
	if _, _, err := s.AddInbound(inbound); err != nil {
		t.Fatalf("seed inbound %d: %v", port, err)
	}
	return inbound
}

// The panel shares one InboundService across requests; with a shared gRPC client one request's Close broke another's call.
func TestDelInboundClientConcurrentCallsDoNotFallBackToRestart(t *testing.T) {
	fake := setupXrayAPITest(t)
	s := &InboundService{}
	const inbounds, deletesPerInbound = 8, 10
	ids := make([]int, inbounds)
	toDelete := make([][]model.Client, inbounds)
	for i := range inbounds {
		clients := []model.Client{{ID: uuid.NewString(), Email: fmt.Sprintf("keep-%d", i), Enable: true}}
		for j := range deletesPerInbound {
			c := model.Client{ID: uuid.NewString(), Email: fmt.Sprintf("del-%d-%d", i, j), Enable: true}
			clients = append(clients, c)
			toDelete[i] = append(toDelete[i], c)
		}
		ids[i] = seedVlessInbound(t, s, 41000+i, clients...).Id
	}
	fake.takeCalls()

	errs := make(chan error, inbounds*deletesPerInbound)
	var wg sync.WaitGroup
	for i := range inbounds {
		wg.Go(func() {
			defer func() {
				if r := recover(); r != nil {
					errs <- fmt.Errorf("inbound %d: panic: %v", i, r)
				}
			}()
			for _, c := range toDelete[i] {
				needRestart, err := s.DelInboundClient(ids[i], c.ID)
				if err != nil {
					errs <- fmt.Errorf("%s: %v", c.Email, err)
				} else if needRestart {
					errs <- fmt.Errorf("%s: fell back to an Xray restart", c.Email)
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if got := len(fake.takeCalls()); got != inbounds*deletesPerInbound {
		t.Errorf("Xray received %d RemoveUser calls, want %d", got, inbounds*deletesPerInbound)
	}
}
