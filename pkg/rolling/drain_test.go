package rolling

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/spf13/pflag"
	"github.com/ydb-platform/ydb-go-genproto/draft/protos/Ydb_Maintenance"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/ydb-platform/ydbops/pkg/client/cms"
	"github.com/ydb-platform/ydbops/pkg/rolling/restarters"
)

type drainCMS struct {
	cms.Client
	ctx       context.Context
	params    *cms.MaintenanceTaskParams
	status    Ydb_Maintenance.ActionState_ActionStatus
	createErr error
	created   func(context.Context, cms.MaintenanceTaskParams)
	getErr    error
	getTask   func(context.Context, string) (cms.MaintenanceTask, error)
	dropTask  func(context.Context, string) error
	dropped   *bool
}

func (c *drainCMS) WithContext(ctx context.Context) cms.Client {
	clone := *c
	clone.ctx = ctx
	return &clone
}
func (c *drainCMS) CreateMaintenanceTask(p cms.MaintenanceTaskParams) (cms.MaintenanceTask, error) {
	*c.params = p
	if c.created != nil {
		c.created(c.ctx, p)
	}
	return &Ydb_Maintenance.MaintenanceTaskResult{ActionGroupStates: []*Ydb_Maintenance.ActionGroupStates{{ActionStates: []*Ydb_Maintenance.ActionState{{Status: c.status}}}}}, c.createErr
}
func (c *drainCMS) GetMaintenanceTask(id string) (cms.MaintenanceTask, error) {
	if c.getTask != nil {
		return c.getTask(c.ctx, id)
	}
	return &Ydb_Maintenance.MaintenanceTaskResult{ActionGroupStates: []*Ydb_Maintenance.ActionGroupStates{{ActionStates: []*Ydb_Maintenance.ActionState{{Status: c.status}}}}}, c.getErr
}
func (c *drainCMS) DropMaintenanceTask(id string) (string, error) {
	*c.dropped = true
	if c.dropTask != nil {
		return "", c.dropTask(c.ctx, id)
	}
	return "", c.ctx.Err()
}

type drainRestarter struct {
	restarters.Restarter
	calls  int
	before func()
}

func (r *drainRestarter) RestartNode(*Ydb_Maintenance.Node) error {
	if r.before != nil {
		r.before()
	}
	r.calls++
	return nil
}

func TestDrainRestart(t *testing.T) {
	for _, tc := range []struct {
		name, action string
		status       Ydb_Maintenance.ActionState_ActionStatus
		createErr    error
		getErr       error
		cancel       bool
		wantErr      bool
		wantCalls    int
		wantWarnings int
	}{
		{name: "performed", action: "stop", status: Ydb_Maintenance.ActionState_ACTION_STATUS_PERFORMED, wantCalls: 1},
		{name: "pending stop", action: "stop", status: Ydb_Maintenance.ActionState_ACTION_STATUS_PENDING, wantErr: true},
		{name: "in progress stop", action: "stop", status: Ydb_Maintenance.ActionState_ACTION_STATUS_IN_PROGRESS, wantErr: true},
		{name: "timeout force", action: "force", wantCalls: 1, wantWarnings: 1},
		{name: "CMS error is not forced", action: "force", createErr: errors.New("CMS unavailable"), wantErr: true},
		{name: "Get errors reach force timeout", action: "force", getErr: errors.New("CMS unavailable"), wantCalls: 1, wantWarnings: 2},
		{name: "Get errors reach stop timeout", action: "stop", getErr: errors.New("CMS unavailable"), wantErr: true, wantWarnings: 1},
		{name: "cancellation is not forced", action: "force", cancel: true, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params := &cms.MaintenanceTaskParams{}
			dropped := false
			cl := &drainCMS{params: params, dropped: &dropped, status: tc.status, createErr: tc.createErr, getErr: tc.getErr}
			rst := &drainRestarter{before: func() {
				if dropped {
					t.Error("drain task dropped before restart")
				}
			}}
			core, logs := observer.New(zap.WarnLevel)
			r := &Rolling{cms: cl, restarter: rst, logger: zap.New(core).Sugar(), state: &state{restartTaskUID: "restart"}, opts: &RestartOptions{WithDrain: true, DrainWaitTimeout: 5 * time.Millisecond, DrainWaitTimeoutAction: tc.action}}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancel {
				cancel()
			}
			err := r.restartNode(ctx, &Ydb_Maintenance.Node{NodeId: 42}, nil)
			if (err != nil) != tc.wantErr {
				t.Fatalf("error = %v", err)
			}
			if rst.calls != tc.wantCalls {
				t.Fatalf("restarts = %d", rst.calls)
			}
			if !dropped {
				t.Error("drain task not cleaned up")
			}
			if params.TaskType != cms.DrainTask || len(params.Nodes) != 1 || params.Nodes[0].NodeId != 42 || params.TaskUID == "restart" {
				t.Fatalf("wrong drain task: %+v", params)
			}
			if logs.Len() != tc.wantWarnings {
				t.Fatalf("warnings = %d, want %d", logs.Len(), tc.wantWarnings)
			}
		})
	}
}

func TestDrainStopAbortsBatch(t *testing.T) {
	dropped := false
	cl := &drainCMS{params: &cms.MaintenanceTaskParams{}, dropped: &dropped}
	rst := &drainRestarter{}
	r := &Rolling{cms: cl, restarter: rst, logger: zap.NewNop().Sugar(), opts: &RestartOptions{WithDrain: true, DrainWaitTimeout: time.Millisecond, DrainWaitTimeoutAction: "stop", NodesInflight: 1}, state: &state{restartTaskUID: "restart", nodes: map[uint32]*Ydb_Maintenance.Node{1: {NodeId: 1}, 2: {NodeId: 2}}}}
	var actions []*Ydb_Maintenance.ActionGroupStates
	for _, id := range []uint32{1, 2} {
		actions = append(actions, &Ydb_Maintenance.ActionGroupStates{ActionStates: []*Ydb_Maintenance.ActionState{{Status: Ydb_Maintenance.ActionState_ACTION_STATUS_PERFORMED, Action: &Ydb_Maintenance.Action{Action: &Ydb_Maintenance.Action_LockAction{LockAction: &Ydb_Maintenance.LockAction{Scope: &Ydb_Maintenance.ActionScope{Scope: &Ydb_Maintenance.ActionScope_NodeId{NodeId: id}}}}}}}})
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	completed, err := r.processActionGroupStates(ctx, actions)
	if completed || !errors.Is(err, context.DeadlineExceeded) || rst.calls != 0 {
		t.Fatalf("completed=%v err=%v restarts=%d", completed, err, rst.calls)
	}
}

func TestRestartWithoutDrain(t *testing.T) {
	rst := &drainRestarter{}
	r := &Rolling{opts: &RestartOptions{}, restarter: rst}
	if err := r.restartNode(context.Background(), &Ydb_Maintenance.Node{}, nil); err != nil || rst.calls != 1 {
		t.Fatalf("err=%v restarts=%d", err, rst.calls)
	}
}

func TestDrainFlags(t *testing.T) {
	opts := &RestartOptions{}
	fs := pflag.NewFlagSet("restart", pflag.ContinueOnError)
	opts.DefineFlags(fs)
	if opts.WithDrain || opts.DrainWaitTimeout != 15*time.Minute || opts.DrainWaitTimeoutAction != "stop" {
		t.Fatalf("wrong defaults: %+v", opts)
	}
	if err := fs.Parse([]string{"--with-drain", "--drain-wait-timeout", "10m", "--drain-wait-timeout-action", "force"}); err != nil {
		t.Fatal(err)
	}
	if !opts.WithDrain || opts.DrainWaitTimeout != 10*time.Minute || opts.DrainWaitTimeoutAction != "force" {
		t.Fatal("flags not propagated")
	}
	opts.DrainWaitTimeoutAction = "skip"
	if opts.Validate() == nil {
		t.Fatal("invalid action accepted")
	}
	opts.DrainWaitTimeoutAction = "stop"
	opts.DrainWaitTimeout = 0
	if opts.Validate() == nil {
		t.Fatal("zero timeout accepted")
	}
}

func TestDrainWaitsForGetPerformed(t *testing.T) {
	responses := []struct {
		status Ydb_Maintenance.ActionState_ActionStatus
		err    error
	}{
		{err: errors.New("GetMaintenanceTask internal retries exhausted")},
		{status: Ydb_Maintenance.ActionState_ACTION_STATUS_PENDING},
		{status: Ydb_Maintenance.ActionState_ACTION_STATUS_IN_PROGRESS},
		{status: Ydb_Maintenance.ActionState_ACTION_STATUS_PERFORMED},
	}
	params := &cms.MaintenanceTaskParams{}
	dropped := false
	reads := 0
	creates := 0
	var originalDeadline time.Time
	var originalTaskID string
	rst := &drainRestarter{before: func() {
		if reads != len(responses) {
			t.Errorf("restart before GetMaintenanceTask returned PERFORMED: %d reads", reads)
		}
		if dropped {
			t.Error("drain task removed before restart")
		}
	}}
	cl := &drainCMS{
		params: params, dropped: &dropped,
		created: func(ctx context.Context, p cms.MaintenanceTaskParams) {
			creates++
			if creates != 1 {
				t.Fatal("drain task recreated after a Get failure")
			}
			var ok bool
			originalDeadline, ok = ctx.Deadline()
			if !ok {
				t.Fatal("drain creation has no deadline")
			}
			originalTaskID = p.TaskUID
		},
		// Even a PERFORMED create response must be verified with GetMaintenanceTask.
		status: Ydb_Maintenance.ActionState_ACTION_STATUS_PERFORMED,
		getTask: func(ctx context.Context, id string) (cms.MaintenanceTask, error) {
			if id != originalTaskID {
				t.Fatalf("queried task %q instead of %q", id, params.TaskUID)
			}
			if rst.calls != 0 || dropped {
				t.Fatal("restart or cleanup happened before drain completed")
			}
			if deadline, ok := ctx.Deadline(); !ok || !deadline.Equal(originalDeadline) {
				t.Fatal("GetMaintenanceTask reset the original drain deadline")
			}
			if reads >= len(responses) {
				t.Fatal("polling continued after PERFORMED")
			}
			response := responses[reads]
			reads++
			return &Ydb_Maintenance.MaintenanceTaskResult{ActionGroupStates: []*Ydb_Maintenance.ActionGroupStates{{ActionStates: []*Ydb_Maintenance.ActionState{{Status: response.status}}}}}, response.err
		},
	}
	r := &Rolling{
		cms: cl, restarter: rst, logger: zap.NewNop().Sugar(), state: &state{restartTaskUID: "restart"},
		opts: &RestartOptions{WithDrain: true, DrainWaitTimeout: 10 * time.Second, DrainWaitTimeoutAction: DrainWaitTimeoutActionStop, CMSQueryInterval: 1},
	}
	if err := r.restartNode(context.Background(), &Ydb_Maintenance.Node{NodeId: 42}, nil); err != nil {
		t.Fatal(err)
	}
	if creates != 1 || reads != len(responses) || rst.calls != 1 || !dropped {
		t.Fatalf("reads=%d restarts=%d dropped=%v", reads, rst.calls, dropped)
	}
}

func TestDrainGetDeadline(t *testing.T) {
	for _, tc := range []struct {
		name, action                 string
		permanentError, parentCancel bool
	}{
		{name: "hanging Get stop", action: DrainWaitTimeoutActionStop},
		{name: "hanging Get force", action: DrainWaitTimeoutActionForce},
		{name: "Get errors stop", action: DrainWaitTimeoutActionStop, permanentError: true},
		{name: "Get errors force", action: DrainWaitTimeoutActionForce, permanentError: true},
		{name: "cancel while retrying Get", action: DrainWaitTimeoutActionForce, permanentError: true, parentCancel: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			dropped := false
			queried := false
			cl := &drainCMS{
				params: &cms.MaintenanceTaskParams{}, dropped: &dropped,
				getTask: func(ctx context.Context, _ string) (cms.MaintenanceTask, error) {
					queried = true
					if tc.parentCancel {
						timer := time.AfterFunc(10*time.Millisecond, func() { cancel(context.Canceled) })
						t.Cleanup(func() { timer.Stop() })
					}
					if tc.permanentError {
						return nil, errors.New("GetMaintenanceTask retries exhausted")
					}
					<-ctx.Done()
					return nil, ctx.Err()
				},
			}
			rst := &drainRestarter{}
			timeout := 20 * time.Millisecond
			if tc.parentCancel {
				timeout = 5 * time.Second
			}
			r := &Rolling{
				cms: cl, restarter: rst, logger: zap.NewNop().Sugar(), state: &state{restartTaskUID: "restart"},
				opts: &RestartOptions{WithDrain: true, DrainWaitTimeout: timeout, DrainWaitTimeoutAction: tc.action},
			}
			started := time.Now()
			err := r.restartNode(ctx, &Ydb_Maintenance.Node{NodeId: 42}, cancel)
			if tc.parentCancel {
				if !errors.Is(err, context.Canceled) || rst.calls != 0 || time.Since(started) >= time.Second {
					t.Fatalf("cancellation was not immediate: error=%v restarts=%d elapsed=%s", err, rst.calls, time.Since(started))
				}
			} else if tc.action == DrainWaitTimeoutActionStop {
				if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(context.Cause(ctx), context.DeadlineExceeded) || rst.calls != 0 {
					t.Fatalf("error=%v restarts=%d", err, rst.calls)
				}
			} else if err != nil || ctx.Err() != nil || rst.calls != 1 {
				t.Fatalf("error=%v restarts=%d", err, rst.calls)
			}
			if !queried || !dropped {
				t.Fatalf("queried=%v dropped=%v", queried, dropped)
			}
		})
	}
}

func TestDrainStopCancelsBeforeCleanup(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	dropped := false
	cl := &drainCMS{
		params: &cms.MaintenanceTaskParams{}, dropped: &dropped,
		status: Ydb_Maintenance.ActionState_ACTION_STATUS_IN_PROGRESS,
		dropTask: func(cleanupCtx context.Context, _ string) error {
			// A slow CMS drop must not postpone cancellation of the other workers.
			if !errors.Is(context.Cause(ctx), context.DeadlineExceeded) {
				t.Errorf("workers were not stopped before drain cleanup: %v", context.Cause(ctx))
			}
			if err := cleanupCtx.Err(); err != nil {
				t.Errorf("cleanup inherited cancellation: %v", err)
			}
			return nil
		},
	}
	rst := &drainRestarter{}
	r := &Rolling{
		cms: cl, restarter: rst, logger: zap.NewNop().Sugar(), state: &state{restartTaskUID: "restart"},
		opts: &RestartOptions{WithDrain: true, DrainWaitTimeout: 5 * time.Millisecond, DrainWaitTimeoutAction: DrainWaitTimeoutActionStop},
	}
	err := r.restartNode(ctx, &Ydb_Maintenance.Node{NodeId: 42}, cancel)
	if !errors.Is(err, context.DeadlineExceeded) || rst.calls != 0 || !dropped {
		t.Fatalf("error=%v restarts=%d dropped=%v", err, rst.calls, dropped)
	}
}
