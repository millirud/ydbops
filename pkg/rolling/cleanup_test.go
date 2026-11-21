package rolling

import (
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/spf13/pflag"
	"github.com/ydb-platform/ydb-go-genproto/draft/Ydb_Maintenance_V1"
	"github.com/ydb-platform/ydb-go-genproto/draft/protos/Ydb_Maintenance"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ydb-platform/ydbops/pkg/client/auth/provider"
	"github.com/ydb-platform/ydbops/pkg/client/cms"
	"github.com/ydb-platform/ydbops/pkg/client/connectionsfactory"
	"github.com/ydb-platform/ydbops/pkg/client/discovery"
	"github.com/ydb-platform/ydbops/pkg/rolling/restarters"
)

type cleanupCMSState struct {
	mu            sync.Mutex
	tasks         map[string]cms.MaintenanceTaskType
	drops         []string
	lockID        string
	createLockErr func() error
	drainErr      error
	dropErr       error
}

type cleanupCMS struct {
	cms.Client
	*cleanupCMSState
	ctx context.Context
}

func (c *cleanupCMS) SetContext(ctx context.Context) { c.ctx = ctx }
func (c *cleanupCMS) WithContext(ctx context.Context) cms.Client {
	clone := *c
	clone.ctx = ctx
	return &clone
}
func (c *cleanupCMS) Nodes() ([]*Ydb_Maintenance.Node, error) {
	return []*Ydb_Maintenance.Node{{NodeId: 1, State: Ydb_Maintenance.ItemState_ITEM_STATE_UP}}, nil
}
func (c *cleanupCMS) Tenants() ([]string, error) { return nil, nil }
func (c *cleanupCMS) GetMaintenanceTask(string) (cms.MaintenanceTask, error) {
	return &Ydb_Maintenance.MaintenanceTaskResult{}, c.ctx.Err()
}
func (c *cleanupCMS) MaintenanceTasks(string) ([]cms.MaintenanceTask, error) {
	// The initial cleanup runs before this restart owns any tasks.
	return nil, nil
}
func (c *cleanupCMS) CreateMaintenanceTask(p cms.MaintenanceTaskParams) (cms.MaintenanceTask, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tasks[p.TaskUID] = p.TaskType
	task := &Ydb_Maintenance.MaintenanceTaskResult{}
	if p.TaskType == cms.DrainTask {
		return task, c.drainErr
	}
	c.lockID = p.TaskUID
	if c.createLockErr != nil {
		return nil, c.createLockErr()
	}
	task.ActionGroupStates = []*Ydb_Maintenance.ActionGroupStates{{ActionStates: []*Ydb_Maintenance.ActionState{{
		Status: Ydb_Maintenance.ActionState_ACTION_STATUS_PERFORMED,
		Action: &Ydb_Maintenance.Action{Action: &Ydb_Maintenance.Action_LockAction{LockAction: &Ydb_Maintenance.LockAction{
			Scope: &Ydb_Maintenance.ActionScope{Scope: &Ydb_Maintenance.ActionScope_NodeId{NodeId: 1}},
		}}},
	}}}}
	return task, nil
}
func (c *cleanupCMS) DropMaintenanceTask(id string) (string, error) {
	if err := c.ctx.Err(); err != nil {
		return "", err
	}
	if deadline, ok := c.ctx.Deadline(); !ok || time.Until(deadline) > maintenanceCleanupTimeout {
		return "", errors.New("cleanup context has no bounded deadline")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.drops = append(c.drops, id)
	if c.dropErr != nil && id == c.lockID {
		return "", c.dropErr
	}
	delete(c.tasks, id)
	return "", nil
}

type cleanupDiscovery struct{ discovery.Client }

func (cleanupDiscovery) WhoAmI() (string, error) { return "test", nil }

type cleanupRestarter struct{ restarters.Restarter }

func (cleanupRestarter) Filter(_ restarters.FilterNodeParams, c restarters.ClusterNodesInfo) []*Ydb_Maintenance.Node {
	return c.AllNodes
}
func (cleanupRestarter) RestartNode(*Ydb_Maintenance.Node) error {
	panic("must not restart after drain failure")
}

func cleanupTestOptions() *RestartOptions {
	opts := &RestartOptions{}
	opts.DefineFlags(pflag.NewFlagSet("cleanup", pflag.ContinueOnError))
	opts.WithDrain = true
	opts.DrainWaitTimeout = time.Millisecond
	return opts
}

func TestDrainFailureReleasesRestartLock(t *testing.T) {
	cmsErr := errors.New("CMS drain failed")
	cleanupErr := errors.New("CMS drop failed")
	for _, tc := range []struct {
		name                       string
		cleanupOnExit              bool
		drainErr, dropErr, wantErr error
	}{
		{name: "stop timeout", cleanupOnExit: true, wantErr: context.DeadlineExceeded},
		{name: "stop timeout without signal cleanup", wantErr: context.DeadlineExceeded},
		{name: "CMS drain error", drainErr: cmsErr, wantErr: cmsErr},
		{name: "cleanup error preserves original", dropErr: cleanupErr, wantErr: context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := cleanupTestOptions()
			opts.CleanupOnExit = tc.cleanupOnExit
			c := &cleanupCMS{cleanupCMSState: &cleanupCMSState{tasks: map[string]cms.MaintenanceTaskType{}, drainErr: tc.drainErr, dropErr: tc.dropErr}}
			err := NewExecuter(opts, zap.NewNop().Sugar(), c, cleanupDiscovery{}, cleanupRestarter{}).Execute()
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("error = %v, want %v", err, tc.wantErr)
			}
			if tc.dropErr != nil && !errors.Is(err, tc.dropErr) {
				t.Fatalf("cleanup failure lost: %v", err)
			}
			if len(c.drops) != 2 || c.drops[1] != c.lockID {
				t.Fatalf("expected drain cleanup before lock cleanup, got %v", c.drops)
			}
			if tc.dropErr == nil && len(c.tasks) != 0 {
				t.Fatalf("tasks left after stop: %v", c.tasks)
			}
		})
	}
}

func TestCancelledLockCreationCleanup(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		t.Run(strconv.FormatBool(enabled), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			opts := cleanupTestOptions()
			opts.CleanupOnExit = enabled
			// CMS accepted the task but the RPC response was lost on cancellation.
			// gRPC's Canceled status does not satisfy errors.Is(context.Canceled).
			rpcErr := status.Error(codes.Canceled, "request canceled")
			c := &cleanupCMS{ctx: ctx, cleanupCMSState: &cleanupCMSState{tasks: map[string]cms.MaintenanceTaskType{}, createLockErr: func() error { cancel(); return rpcErr }}}
			r := &Rolling{cms: c, discovery: cleanupDiscovery{}, restarter: cleanupRestarter{}, logger: zap.NewNop().Sugar(), opts: opts}
			if err := r.DoRestart(ctx); !errors.Is(err, rpcErr) {
				t.Fatalf("original RPC error lost: %v", err)
			}
			if enabled && len(c.tasks) != 0 {
				t.Fatalf("restart lock left after cancellation: %v", c.tasks)
			}
			if !enabled && len(c.drops) != 0 {
				t.Fatalf("cleanup-on-exit=false ignored: %v", c.drops)
			}
		})
	}
}

type cleanupMaintenanceServer struct {
	Ydb_Maintenance_V1.UnimplementedMaintenanceServiceServer
	dropped chan string
}

func (s *cleanupMaintenanceServer) DropMaintenanceTask(_ context.Context, request *Ydb_Maintenance.DropMaintenanceTaskRequest) (*Ydb_Maintenance.ManageMaintenanceTaskResponse, error) {
	s.dropped <- request.TaskUid
	return &Ydb_Maintenance.ManageMaintenanceTaskResponse{Operation: &Ydb_Operations.Operation{Ready: true, Status: Ydb.StatusIds_SUCCESS}}, nil
}

func TestCleanupRPCWithCancelledParent(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	maintenance := &cleanupMaintenanceServer{dropped: make(chan string, 1)}
	Ydb_Maintenance_V1.RegisterMaintenanceServiceServer(server, maintenance)
	go func() { _ = server.Serve(lis) }()
	defer server.Stop()
	host, portString, err := net.SplitHostPort(lis.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portString)
	if err != nil {
		t.Fatal(err)
	}
	cl := cms.NewCMSClient(connectionsfactory.NewFromConfig(connectionsfactory.Config{Endpoint: host, GRPCPort: port, TransportTimeout: time.Second}), zap.NewNop().Sugar(), provider.NewProviderFromToken("test"))
	defer cl.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cl.SetContext(ctx)
	cancel()
	r := &Rolling{cms: cl}
	if err := r.cleanupMaintenanceTask(ctx, "owned-restart"); err != nil {
		t.Fatal(err)
	}
	select {
	case id := <-maintenance.dropped:
		if id != "owned-restart" {
			t.Fatalf("wrong task dropped: %s", id)
		}
	default:
		t.Fatal("drop did not reach CMS")
	}
	// Cleanup must not reset the shared client's canceled context.
	if _, err := cl.DropMaintenanceTask("other-task"); status.Code(err) != codes.Canceled {
		t.Fatalf("shared context changed: %v", err)
	}
}

// Send real signals in a subprocess so a broken signal handler cannot kill the
// test runner or interfere with other tests.
func TestSignalReleasesRestartLock(t *testing.T) {
	signalName := os.Getenv("YDBOPS_CLEANUP_TEST_SIGNAL")
	if signalName == "" {
		for _, name := range []string{"SIGINT", "SIGTERM"} {
			t.Run(name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSignalReleasesRestartLock$", "-test.v")
				cmd.Env = append(os.Environ(), "YDBOPS_CLEANUP_TEST_SIGNAL="+name)
				if output, err := cmd.CombinedOutput(); err != nil {
					t.Fatalf("%s: %v\n%s", name, err, output)
				}
			})
		}
		return
	}
	opts := cleanupTestOptions()
	c := &cleanupCMS{cleanupCMSState: &cleanupCMSState{tasks: map[string]cms.MaintenanceTaskType{}}}
	rpcErr := status.Error(codes.Canceled, "signal interrupted request")
	c.createLockErr = func() error {
		sig := syscall.SIGINT
		if signalName == "SIGTERM" {
			sig = syscall.SIGTERM
		}
		if err := syscall.Kill(os.Getpid(), sig); err != nil {
			return err
		}
		<-c.ctx.Done()
		return rpcErr
	}
	err := NewExecuter(opts, zap.NewNop().Sugar(), c, cleanupDiscovery{}, cleanupRestarter{}).Execute()
	if !errors.Is(err, rpcErr) {
		t.Fatalf("original error lost: %v", err)
	}
	if len(c.drops) != 1 || c.drops[0] != c.lockID || len(c.tasks) != 0 {
		t.Fatalf("lock not released after %s: drops=%v tasks=%v", signalName, c.drops, c.tasks)
	}
}
