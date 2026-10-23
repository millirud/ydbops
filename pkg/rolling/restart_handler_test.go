package rolling

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/ydb-platform/ydb-go-genproto/draft/protos/Ydb_Maintenance"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ydb-platform/ydbops/pkg/client/cms"
	"github.com/ydb-platform/ydbops/pkg/rolling/restarters"
)

type recordedTask struct {
	uid    string
	states []*Ydb_Maintenance.ActionGroupStates
}

func (t *recordedTask) GetRetryAfter() *timestamppb.Timestamp { return nil }
func (t *recordedTask) GetActionGroupStates() []*Ydb_Maintenance.ActionGroupStates {
	return t.states
}
func (t *recordedTask) GetTaskUid() string { return t.uid }

type scriptedGet struct {
	status  Ydb_Maintenance.ActionState_ActionStatus
	details string
	err     error
}

type scriptedCMS struct {
	createErr    error
	gets         []scriptedGet
	dropFailures int

	createCalls int
	getCalls    int
	dropCalls   int
	created     []cms.DrainTaskParams
	dropped     []string
	started     chan struct{}
}

func (s *scriptedCMS) CreateDrainTask(params cms.DrainTaskParams) (cms.MaintenanceTask, error) {
	s.createCalls++
	s.created = append(s.created, params)
	if s.started != nil {
		s.started <- struct{}{}
	}
	if s.createErr != nil {
		return nil, s.createErr
	}
	return &recordedTask{uid: params.TaskUID}, nil
}

func (s *scriptedCMS) GetMaintenanceTask(taskID string) (cms.MaintenanceTask, error) {
	s.getCalls++
	if len(s.gets) == 0 {
		return nil, fmt.Errorf("no scripted get response")
	}
	idx := s.getCalls - 1
	if idx >= len(s.gets) {
		idx = len(s.gets) - 1
	}
	resp := s.gets[idx]
	if resp.err != nil {
		return nil, resp.err
	}
	return &recordedTask{
		uid: taskID,
		states: []*Ydb_Maintenance.ActionGroupStates{{
			ActionStates: []*Ydb_Maintenance.ActionState{{
				Status:  resp.status,
				Details: resp.details,
				Action: &Ydb_Maintenance.Action{
					Action: &Ydb_Maintenance.Action_DrainAction{
						DrainAction: &Ydb_Maintenance.DrainAction{
							Scope: &Ydb_Maintenance.ActionScope{
								Scope: &Ydb_Maintenance.ActionScope_NodeId{NodeId: 7},
							},
						},
					},
				},
			}},
		}},
	}, nil
}

func (s *scriptedCMS) DropMaintenanceTask(taskID string) (string, error) {
	s.dropCalls++
	s.dropped = append(s.dropped, taskID)
	if s.dropFailures > 0 {
		s.dropFailures--
		return "", fmt.Errorf("drop failed")
	}
	return "SUCCESS", nil
}

type fakeRestarter struct {
	calls     int
	failTimes int
}

func (f *fakeRestarter) RestartNode(_ *Ydb_Maintenance.Node) error {
	f.calls++
	if f.calls <= f.failTimes {
		return fmt.Errorf("restart failed")
	}
	return nil
}

func (f *fakeRestarter) Filter(restarters.FilterNodeParams, restarters.ClusterNodesInfo) []*Ydb_Maintenance.Node {
	return nil
}

func lockPerformed(nodeID uint32) *Ydb_Maintenance.ActionGroupStates {
	return &Ydb_Maintenance.ActionGroupStates{
		ActionStates: []*Ydb_Maintenance.ActionState{{
			Status: Ydb_Maintenance.ActionState_ACTION_STATUS_PERFORMED,
			Action: &Ydb_Maintenance.Action{
				Action: &Ydb_Maintenance.Action_LockAction{
					LockAction: &Ydb_Maintenance.LockAction{
						Scope: &Ydb_Maintenance.ActionScope{
							Scope: &Ydb_Maintenance.ActionScope_NodeId{NodeId: nodeID},
						},
					},
				},
			},
			ActionUid: &Ydb_Maintenance.ActionUid{TaskUid: "lock-task", GroupId: "g", ActionId: "a"},
		}},
	}
}

func newTestHandler(
	ctx context.Context,
	client maintenanceClient,
	restarter restarters.Restarter,
	drains *drainTracker,
	statusCh chan<- restartStatus,
) *restartHandler {
	if drains == nil {
		drains = newDrainTracker()
	}
	handler := newRestartHandler(
		ctx,
		zap.NewNop().Sugar(),
		restarter,
		client,
		1,
		0,
		time.Millisecond,
		Ydb_Maintenance.AvailabilityMode_AVAILABILITY_MODE_STRONG,
		-3,
		true,
		map[uint32]*Ydb_Maintenance.Node{
			7: {NodeId: 7},
		},
		drains,
		statusCh,
	)
	handler.drainTimeout = time.Second
	return handler
}

func runNode(handler *restartHandler, statusCh chan restartStatus) restartStatus {
	handler.run()
	handler.push(lockPerformed(7))
	handler.stop(true)
	return <-statusCh
}

type stubCMS struct {
	scriptedCMS
}

func (s *stubCMS) Tenants() ([]string, error)              { panic("unused") }
func (s *stubCMS) Nodes() ([]*Ydb_Maintenance.Node, error) { panic("unused") }
func (s *stubCMS) CompleteAction([]*Ydb_Maintenance.ActionUid) (*Ydb_Maintenance.ManageActionResult, error) {
	panic("unused")
}
func (s *stubCMS) CompleteActions(string, []string) (*Ydb_Maintenance.ManageActionResult, error) {
	panic("unused")
}
func (s *stubCMS) CreateMaintenanceTask(cms.MaintenanceTaskParams) (cms.MaintenanceTask, error) {
	panic("unused")
}
func (s *stubCMS) DropTask(string) error                                      { panic("unused") }
func (s *stubCMS) ListMaintenanceTaskUIDs(*string) ([]string, error)          { panic("unused") }
func (s *stubCMS) ListTasksForUser(string) ([]cms.MaintenanceTask, error)     { panic("unused") }
func (s *stubCMS) MaintenanceTasks(string) ([]cms.MaintenanceTask, error)     { panic("unused") }
func (s *stubCMS) RefreshMaintenanceTask(string) (cms.MaintenanceTask, error) { panic("unused") }
func (s *stubCMS) RefreshTask(string) (cms.MaintenanceTask, error)            { panic("unused") }
func (s *stubCMS) SetContext(context.Context)                                 {}
func (s *stubCMS) Close() error                                               { return nil }

var _ = Describe("restart drain", func() {
	It("restarts without a drain task when with-drain is disabled", func() {
		client := &scriptedCMS{}
		restarter := &fakeRestarter{}
		statusCh := make(chan restartStatus, 1)
		handler := newTestHandler(context.Background(), client, restarter, nil, statusCh)
		handler.withDrain = false

		status := runNode(handler, statusCh)

		Expect(status.err).NotTo(HaveOccurred())
		Expect(restarter.calls).To(Equal(1))
		Expect(client.createCalls).To(Equal(0))
		Expect(client.getCalls).To(Equal(0))
		Expect(client.dropCalls).To(Equal(0))
	})

	It("restarts only after drain reaches PERFORMED and then drops the drain task", func() {
		client := &scriptedCMS{gets: []scriptedGet{
			{status: Ydb_Maintenance.ActionState_ACTION_STATUS_IN_PROGRESS, details: "Progress: 40%"},
			{status: Ydb_Maintenance.ActionState_ACTION_STATUS_PERFORMED},
		}}
		restarter := &fakeRestarter{}
		statusCh := make(chan restartStatus, 1)
		handler := newTestHandler(context.Background(), client, restarter, nil, statusCh)
		handler.cmsQueryInterval = time.Hour

		status := runNode(handler, statusCh)

		Expect(status.err).NotTo(HaveOccurred())
		Expect(status.nodeID).To(Equal(uint32(7)))
		Expect(restarter.calls).To(Equal(1))
		Expect(client.createCalls).To(Equal(1))
		Expect(client.getCalls).To(Equal(2))
		Expect(client.dropCalls).To(Equal(1))
		Expect(client.created[0].NodeID).To(Equal(uint32(7)))
		Expect(client.created[0].Priority).To(Equal(int32(-3)))
		Expect(client.created[0].AvailabilityMode).To(Equal(Ydb_Maintenance.AvailabilityMode_AVAILABILITY_MODE_STRONG))
		Expect(client.created[0].TaskUID).To(HavePrefix("rolling-drain-7-"))
		Expect(client.dropped).To(Equal([]string{client.created[0].TaskUID}))
	})

	It("accepts PERFORMED on the first poll without waiting", func() {
		client := &scriptedCMS{gets: []scriptedGet{
			{status: Ydb_Maintenance.ActionState_ACTION_STATUS_PERFORMED},
		}}
		restarter := &fakeRestarter{}
		statusCh := make(chan restartStatus, 1)
		handler := newTestHandler(context.Background(), client, restarter, nil, statusCh)
		handler.cmsQueryInterval = time.Hour

		status := runNode(handler, statusCh)

		Expect(status.err).NotTo(HaveOccurred())
		Expect(client.getCalls).To(Equal(1))
		Expect(restarter.calls).To(Equal(1))
	})

	It("keeps waiting while drain is PENDING", func() {
		client := &scriptedCMS{gets: []scriptedGet{
			{status: Ydb_Maintenance.ActionState_ACTION_STATUS_PENDING},
			{status: Ydb_Maintenance.ActionState_ACTION_STATUS_IN_PROGRESS, details: "Progress: 10%"},
			{status: Ydb_Maintenance.ActionState_ACTION_STATUS_PERFORMED},
		}}
		restarter := &fakeRestarter{}
		statusCh := make(chan restartStatus, 1)

		status := runNode(newTestHandler(context.Background(), client, restarter, nil, statusCh), statusCh)

		Expect(status.err).NotTo(HaveOccurred())
		Expect(client.getCalls).To(Equal(3))
		Expect(restarter.calls).To(Equal(1))
	})

	It("does not restart when drain task creation fails", func() {
		client := &scriptedCMS{createErr: fmt.Errorf("cms unavailable")}
		restarter := &fakeRestarter{}
		statusCh := make(chan restartStatus, 1)

		status := runNode(newTestHandler(context.Background(), client, restarter, nil, statusCh), statusCh)

		Expect(status.err).To(MatchError(ContainSubstring("failed to create drain task")))
		Expect(restarter.calls).To(Equal(0))
		Expect(client.dropCalls).To(Equal(0))
	})

	It("does not restart when drain status is UNSPECIFIED and drops that task", func() {
		client := &scriptedCMS{gets: []scriptedGet{
			{status: Ydb_Maintenance.ActionState_ACTION_STATUS_UNSPECIFIED, details: "Drain failed"},
		}}
		restarter := &fakeRestarter{}
		statusCh := make(chan restartStatus, 1)

		status := runNode(newTestHandler(context.Background(), client, restarter, nil, statusCh), statusCh)

		Expect(status.err).To(MatchError(ContainSubstring("ACTION_STATUS_UNSPECIFIED")))
		Expect(restarter.calls).To(Equal(0))
		Expect(client.dropCalls).To(Equal(1))
		Expect(client.dropped).To(Equal([]string{client.created[0].TaskUID}))
	})

	It("does not restart when CMS reports drain failed while it is still in progress", func() {
		client := &scriptedCMS{gets: []scriptedGet{
			{status: Ydb_Maintenance.ActionState_ACTION_STATUS_IN_PROGRESS, details: "Drain failed: hive timeout"},
		}}
		restarter := &fakeRestarter{}
		statusCh := make(chan restartStatus, 1)

		status := runNode(newTestHandler(context.Background(), client, restarter, nil, statusCh), statusCh)

		Expect(status.err).To(MatchError(ContainSubstring("Drain failed: hive timeout")))
		Expect(restarter.calls).To(Equal(0))
		Expect(client.dropCalls).To(Equal(1))
	})

	It("does not restart when GetMaintenanceTask fails and drops the drain task", func() {
		client := &scriptedCMS{gets: []scriptedGet{{err: fmt.Errorf("rpc failed")}}}
		restarter := &fakeRestarter{}
		statusCh := make(chan restartStatus, 1)

		status := runNode(newTestHandler(context.Background(), client, restarter, nil, statusCh), statusCh)

		Expect(status.err).To(MatchError(ContainSubstring("rpc failed")))
		Expect(restarter.calls).To(Equal(0))
		Expect(client.dropCalls).To(Equal(1))
	})

	It("does not restart when drain exceeds its timeout and the timeout action is fail", func() {
		client := &scriptedCMS{gets: []scriptedGet{
			{status: Ydb_Maintenance.ActionState_ACTION_STATUS_IN_PROGRESS, details: "Progress: 1%"},
		}}
		restarter := &fakeRestarter{}
		statusCh := make(chan restartStatus, 1)
		handler := newTestHandler(context.Background(), client, restarter, nil, statusCh)
		handler.drainTimeout = 30 * time.Millisecond
		handler.drainTimeoutAction = DrainWaitTimeoutActionFail
		handler.cmsQueryInterval = 5 * time.Millisecond

		status := runNode(handler, statusCh)

		Expect(status.err).To(MatchError(ContainSubstring("did not reach PERFORMED")))
		Expect(restarter.calls).To(Equal(0))
		Expect(client.dropCalls).To(Equal(1))
	})

	It("restarts the node when drain exceeds its timeout and the timeout action is skip", func() {
		client := &scriptedCMS{gets: []scriptedGet{
			{status: Ydb_Maintenance.ActionState_ACTION_STATUS_IN_PROGRESS, details: "Progress: 1%"},
		}}
		restarter := &fakeRestarter{}
		statusCh := make(chan restartStatus, 1)
		handler := newTestHandler(context.Background(), client, restarter, nil, statusCh)
		handler.drainTimeout = 30 * time.Millisecond
		handler.drainTimeoutAction = DrainWaitTimeoutActionSkip
		handler.cmsQueryInterval = 5 * time.Millisecond

		status := runNode(handler, statusCh)

		Expect(status.err).NotTo(HaveOccurred())
		Expect(restarter.calls).To(Equal(1))
		Expect(client.createCalls).To(Equal(1))
		Expect(client.dropCalls).To(Equal(1))
		Expect(client.dropped).To(Equal([]string{client.created[0].TaskUID}))
	})

	It("does not drop an in-progress drain when the restart is cancelled", func() {
		client := &scriptedCMS{
			started: make(chan struct{}),
			gets: []scriptedGet{
				{status: Ydb_Maintenance.ActionState_ACTION_STATUS_PENDING},
			},
		}
		restarter := &fakeRestarter{}
		statusCh := make(chan restartStatus, 1)
		ctx, cancel := context.WithCancel(context.Background())
		handler := newTestHandler(ctx, client, restarter, nil, statusCh)
		handler.cmsQueryInterval = time.Hour
		handler.run()
		handler.push(lockPerformed(7))
		<-client.started
		cancel()
		handler.stop(true)

		Expect(restarter.calls).To(Equal(0))
		Expect(client.createCalls).To(Equal(1))
		Expect(client.dropCalls).To(Equal(0))
	})

	It("keeps the drain task when restart fails and does not create another one on retry", func() {
		client := &scriptedCMS{gets: []scriptedGet{
			{status: Ydb_Maintenance.ActionState_ACTION_STATUS_IN_PROGRESS},
			{status: Ydb_Maintenance.ActionState_ACTION_STATUS_PERFORMED},
		}}
		restarter := &fakeRestarter{failTimes: 1}
		statusCh := make(chan restartStatus, 2)
		handler := newTestHandler(context.Background(), client, restarter, nil, statusCh)
		handler.run()
		handler.push(lockPerformed(7))
		handler.push(lockPerformed(7))
		handler.stop(true)

		first := <-statusCh
		second := <-statusCh
		Expect(first.err).To(MatchError(ContainSubstring("restart failed")))
		Expect(second.err).NotTo(HaveOccurred())
		Expect(client.createCalls).To(Equal(1))
		Expect(client.getCalls).To(Equal(2))
		Expect(restarter.calls).To(Equal(2))
		Expect(client.dropCalls).To(Equal(1))
		Expect(client.dropped).To(Equal([]string{client.created[0].TaskUID}))
	})

	It("retries dropping the drain task without restarting the node again", func() {
		client := &scriptedCMS{
			dropFailures: 1,
			gets: []scriptedGet{
				{status: Ydb_Maintenance.ActionState_ACTION_STATUS_PERFORMED},
			},
		}
		restarter := &fakeRestarter{}
		statusCh := make(chan restartStatus, 2)
		handler := newTestHandler(context.Background(), client, restarter, nil, statusCh)
		handler.run()
		handler.push(lockPerformed(7))
		handler.push(lockPerformed(7))
		handler.stop(true)

		first := <-statusCh
		second := <-statusCh
		Expect(first.err).To(MatchError(ContainSubstring("failed to drop drain task")))
		Expect(second.err).NotTo(HaveOccurred())
		Expect(restarter.calls).To(Equal(1))
		Expect(client.createCalls).To(Equal(1))
		Expect(client.dropCalls).To(Equal(2))
	})

	It("drops the drain task when restart retries are exhausted", func() {
		client := &stubCMS{}
		drains := newDrainTracker()
		drains.remember(7, "rolling-drain-7-task")
		rolling := &Rolling{
			cms:    client,
			logger: zap.NewNop().Sugar(),
			opts:   &RestartOptions{RestartRetryNumber: 2},
			state: &state{
				retriesMadeForNode:             map[uint32]int{},
				unreportedButFinishedActionIds: []string{},
			},
			drains: drains,
		}
		action := &Ydb_Maintenance.ActionState{
			ActionUid: &Ydb_Maintenance.ActionUid{TaskUid: "lock", GroupId: "g", ActionId: "a"},
		}

		first := make(chan restartStatus, 1)
		first <- restartStatus{nodeID: 7, as: action, err: fmt.Errorf("restart failed")}
		rolling.handleRestartStatus(context.Background(), first, 1)
		Expect(client.dropCalls).To(Equal(0))
		_, _, stillDrained := drains.get(7)
		Expect(stillDrained).To(BeTrue())

		second := make(chan restartStatus, 1)
		second <- restartStatus{nodeID: 7, as: action, err: fmt.Errorf("restart failed")}
		rolling.handleRestartStatus(context.Background(), second, 1)
		Expect(client.dropCalls).To(Equal(1))
		Expect(client.dropped).To(Equal([]string{"rolling-drain-7-task"}))
		_, _, stillDrained = drains.get(7)
		Expect(stillDrained).To(BeFalse())
		Expect(rolling.completedActions).To(HaveLen(1))
	})
})
