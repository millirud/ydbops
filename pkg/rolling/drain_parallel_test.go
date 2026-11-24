package rolling

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/draft/protos/Ydb_Maintenance"
	"go.uber.org/zap"

	"github.com/ydb-platform/ydbops/pkg/client/cms"
	"github.com/ydb-platform/ydbops/pkg/rolling/restarters"
)

type stopCleanupState struct {
	mu           sync.Mutex
	nodes        map[string]uint32
	cleaning     chan struct{}
	thirdPolling chan struct{}
	thirdStarted chan struct{}
}

type stopCleanupCMS struct {
	cms.Client
	state *stopCleanupState
	ctx   context.Context
}

func (c *stopCleanupCMS) WithContext(ctx context.Context) cms.Client {
	clone := *c
	clone.ctx = ctx
	return &clone
}
func (c *stopCleanupCMS) CreateMaintenanceTask(p cms.MaintenanceTaskParams) (cms.MaintenanceTask, error) {
	c.state.mu.Lock()
	c.state.nodes[p.TaskUID] = p.Nodes[0].NodeId
	c.state.mu.Unlock()
	return &Ydb_Maintenance.MaintenanceTaskResult{}, nil
}
func (c *stopCleanupCMS) taskNode(id string) uint32 {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	return c.state.nodes[id]
}
func (c *stopCleanupCMS) GetMaintenanceTask(id string) (cms.MaintenanceTask, error) {
	switch c.taskNode(id) {
	case 1:
		<-c.ctx.Done()
		return nil, c.ctx.Err()
	case 3:
		close(c.state.thirdPolling)
		select {
		case <-c.state.cleaning:
		case <-c.ctx.Done():
			return nil, c.ctx.Err()
		}
	}
	return &Ydb_Maintenance.MaintenanceTaskResult{ActionGroupStates: []*Ydb_Maintenance.ActionGroupStates{{ActionStates: []*Ydb_Maintenance.ActionState{{Status: Ydb_Maintenance.ActionState_ACTION_STATUS_PERFORMED}}}}}, nil
}
func (c *stopCleanupCMS) DropMaintenanceTask(id string) (string, error) {
	if c.taskNode(id) == 1 {
		close(c.state.cleaning)
		// Simulate slow cleanup after the first node's drain timeout. Node 3 must
		// already have been cancelled, although its drain can now report PERFORMED.
		select {
		case <-c.state.thirdStarted:
		case <-time.After(100 * time.Millisecond):
		}
	}
	return "", nil
}

type stopCleanupRestarter struct {
	restarters.Restarter
	state *stopCleanupState
}

func (r *stopCleanupRestarter) RestartNode(n *Ydb_Maintenance.Node) error {
	if n.NodeId == 3 {
		close(r.state.thirdStarted)
	}
	return nil
}

func TestStopDoesNotRestartDuringCleanup(t *testing.T) {
	for _, byTenant := range []bool{false, true} {
		t.Run(strconv.FormatBool(byTenant), func(t *testing.T) {
			s := &stopCleanupState{nodes: map[string]uint32{}, cleaning: make(chan struct{}), thirdPolling: make(chan struct{}), thirdStarted: make(chan struct{})}
			r := &Rolling{
				cms: &stopCleanupCMS{state: s}, restarter: &stopCleanupRestarter{state: s}, logger: zap.NewNop().Sugar(),
				opts:  &RestartOptions{WithDrain: true, DrainWaitTimeout: 250 * time.Millisecond, DrainWaitTimeoutAction: DrainWaitTimeoutActionStop, NodesInflight: 2, DelayBetweenRestarts: 30 * time.Millisecond},
				state: &state{restartTaskUID: "restart", nodes: map[uint32]*Ydb_Maintenance.Node{}, retriesMadeForNode: map[uint32]int{}},
			}
			if byTenant {
				r.opts.TenantsInflight = 1
			}
			var actions []*Ydb_Maintenance.ActionGroupStates
			for _, id := range []uint32{1, 2, 3} {
				r.state.nodes[id] = testTenantNode(id, "tenant-a")
				actions = append(actions, &Ydb_Maintenance.ActionGroupStates{ActionStates: []*Ydb_Maintenance.ActionState{{
					ActionUid: &Ydb_Maintenance.ActionUid{TaskUid: "restart", ActionId: strconv.Itoa(int(id))}, Status: Ydb_Maintenance.ActionState_ACTION_STATUS_PERFORMED,
					Action: &Ydb_Maintenance.Action{Action: &Ydb_Maintenance.Action_LockAction{LockAction: &Ydb_Maintenance.LockAction{Scope: &Ydb_Maintenance.ActionScope{Scope: &Ydb_Maintenance.ActionScope_NodeId{NodeId: id}}}}},
				}}})
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_, err := r.processActionGroupStates(ctx, actions)
			var drainErr *drainError
			if !errors.As(err, &drainErr) || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("original drain timeout lost: %v", err)
			}
			select {
			case <-s.thirdPolling:
			default:
				t.Fatal("test did not reach the concurrent drain before cancellation")
			}
			select {
			case <-s.thirdStarted:
				t.Fatal("node 3 restarted after stop timeout while drain cleanup was pending")
			default:
			}
		})
	}
}
