package rolling

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/ydb-platform/ydb-go-genproto/draft/protos/Ydb_Maintenance"
	"go.uber.org/zap"

	"github.com/ydb-platform/ydbops/pkg/client/cms"
	"github.com/ydb-platform/ydbops/pkg/rolling/restarters"
)

type restartStatus struct {
	nodeID uint32
	as     *Ydb_Maintenance.ActionState
	err    error
}

type maintenanceClient interface {
	CreateDrainTask(cms.DrainTaskParams) (cms.MaintenanceTask, error)
	GetMaintenanceTask(string) (cms.MaintenanceTask, error)
	DropMaintenanceTask(string) (string, error)
}

type drainState struct {
	taskUID   string
	restarted bool
}

type drainTracker struct {
	mu     sync.Mutex
	byNode map[uint32]*drainState
}

func newDrainTracker() *drainTracker {
	return &drainTracker{byNode: make(map[uint32]*drainState)}
}

func (t *drainTracker) get(nodeID uint32) (taskUID string, restarted bool, ok bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	state, ok := t.byNode[nodeID]
	if !ok {
		return "", false, false
	}
	return state.taskUID, state.restarted, true
}

func (t *drainTracker) remember(nodeID uint32, taskUID string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, ok := t.byNode[nodeID]; ok {
		return
	}
	t.byNode[nodeID] = &drainState{taskUID: taskUID}
}

func (t *drainTracker) markRestarted(nodeID uint32) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if state, ok := t.byNode[nodeID]; ok {
		state.restarted = true
	}
}

func (t *drainTracker) forget(nodeID uint32) (string, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	state, ok := t.byNode[nodeID]
	if !ok {
		return "", false
	}
	delete(t.byNode, nodeID)
	return state.taskUID, true
}

type restartHandler struct {
	ctx       context.Context
	logger    *zap.SugaredLogger
	queue     chan *Ydb_Maintenance.ActionGroupStates
	restarter restarters.Restarter
	cms       maintenanceClient
	statusCh  chan<- restartStatus
	drains    *drainTracker

	// TODO(shmel1k@): probably, not needed here.
	nodes map[uint32]*Ydb_Maintenance.Node

	wg sync.WaitGroup

	nodesInflight        int
	delayBetweenRestarts time.Duration
	cmsQueryInterval     time.Duration
	drainTimeout         time.Duration
	drainTimeoutAction   string
	availabilityMode     Ydb_Maintenance.AvailabilityMode
	priority             int32
	withDrain            bool
}

func (rh *restartHandler) push(state *Ydb_Maintenance.ActionGroupStates) {
	select {
	case <-rh.ctx.Done():
		return
	case rh.queue <- state:
	}
}

func (rh *restartHandler) run() {
	for i := 0; i < rh.nodesInflight; i++ {
		rh.wg.Add(1)
		go func() {
			defer rh.wg.Done()

			for {
				select {
				case <-rh.ctx.Done():
					return
				case gs, ok := <-rh.queue:
					if !ok {
						return
					}

					as := gs.ActionStates[0]
					lock := as.Action.GetLockAction()
					if lock == nil {
						panic(fmt.Sprintf("unexpected non-lock action type in restartHandler: %v", as.Action))
					}
					nodeID := lock.Scope.GetNodeId()
					node := rh.nodes[nodeID]

					err := rh.drainAndRestart(node)

					select {
					case <-rh.ctx.Done():
						return
					case rh.statusCh <- restartStatus{
						nodeID: nodeID,
						as:     as,
						err:    err,
					}:
					}

					select {
					case <-rh.ctx.Done():
						return
					case <-time.After(rh.delayBetweenRestarts):
						continue
					}
				}
			}
		}()
	}
}

func (rh *restartHandler) drainAndRestart(node *Ydb_Maintenance.Node) error {
	if !rh.withDrain {
		rh.logger.Debugf("Restart node with id: %d", node.GetNodeId())
		return rh.restarter.RestartNode(node)
	}

	if err := rh.ctx.Err(); err != nil {
		return err
	}

	nodeID := node.GetNodeId()
	taskUID, restarted, alreadyDrained := rh.drains.get(nodeID)
	if !alreadyDrained {
		taskUID = drainTaskUID(nodeID)
		rh.logger.Infof("Draining node %d with task %s", nodeID, taskUID)

		var err error
		taskUID, err = rh.createAndWaitDrain(nodeID, taskUID)
		if err != nil {
			return err
		}
		rh.drains.remember(nodeID, taskUID)
	} else if restarted {
		rh.logger.Infof("Node %d is already restarted, retrying drop of drain task %s", nodeID, taskUID)
	} else {
		rh.logger.Infof("Node %d is already drained by task %s", nodeID, taskUID)
	}

	if !restarted {
		rh.logger.Debugf("Restart node with id: %d", nodeID)
		if err := rh.restarter.RestartNode(node); err != nil {
			return err
		}
		rh.drains.markRestarted(nodeID)
	}

	if _, err := rh.cms.DropMaintenanceTask(taskUID); err != nil {
		return fmt.Errorf("failed to drop drain task %s for node %d: %w", taskUID, nodeID, err)
	}
	rh.drains.forget(nodeID)
	return nil
}

func drainTaskUID(nodeID uint32) string {
	return fmt.Sprintf("%s%d-%s", DrainTaskPrefix, nodeID, uuid.New().String())
}

func (rh *restartHandler) createAndWaitDrain(nodeID uint32, taskUID string) (string, error) {
	created, err := rh.cms.CreateDrainTask(cms.DrainTaskParams{
		TaskUID:          taskUID,
		AvailabilityMode: rh.availabilityMode,
		Priority:         rh.priority,
		NodeID:           nodeID,
	})
	if err != nil {
		return "", fmt.Errorf("failed to create drain task for node %d: %w", nodeID, err)
	}
	if created != nil && created.GetTaskUid() != "" {
		taskUID = created.GetTaskUid()
	}

	err = rh.waitDrain(nodeID, taskUID)
	if err != nil && rh.ctx.Err() == nil {
		if _, dropErr := rh.cms.DropMaintenanceTask(taskUID); dropErr != nil {
			rh.logger.Warnf("Failed to drop unsuccessful drain task %s for node %d: %v", taskUID, nodeID, dropErr)
		}
	}
	if err != nil {
		return "", err
	}
	return taskUID, nil
}

func (rh *restartHandler) waitDrain(nodeID uint32, taskUID string) error {
	deadline := time.Now().Add(rh.drainTimeout)
	for {
		if err := rh.ctx.Err(); err != nil {
			return err
		}

		task, err := rh.cms.GetMaintenanceTask(taskUID)
		if err != nil {
			return fmt.Errorf("failed to get drain task %s for node %d: %w", taskUID, nodeID, err)
		}

		status, details, err := drainActionStatus(task)
		if err != nil {
			return fmt.Errorf("drain task %s for node %d: %w", taskUID, nodeID, err)
		}

		switch status {
		case Ydb_Maintenance.ActionState_ACTION_STATUS_PERFORMED:
			rh.logger.Infof("Drain task %s for node %d performed", taskUID, nodeID)
			return nil
		case Ydb_Maintenance.ActionState_ACTION_STATUS_IN_PROGRESS, Ydb_Maintenance.ActionState_ACTION_STATUS_PENDING:
			if strings.Contains(strings.ToLower(details), "drain failed") {
				return fmt.Errorf("drain of node %d failed: %s", nodeID, details)
			}
			if details != "" {
				rh.logger.Infof("Drain node %d: %s (%s)", nodeID, status, details)
			} else {
				rh.logger.Infof("Drain node %d: %s", nodeID, status)
			}
		default:
			return fmt.Errorf("drain of node %d returned status %s: %s", nodeID, status, details)
		}

		if !time.Now().Before(deadline) {
			if rh.drainTimeoutAction == DrainWaitTimeoutActionSkip {
				rh.logger.Warnf("Drain wait timeout %s exceeded for node %d, continuing restart", rh.drainTimeout, nodeID)
				return nil
			}
			return fmt.Errorf("drain of node %d did not reach PERFORMED within %s", nodeID, rh.drainTimeout)
		}

		wait := rh.cmsQueryInterval
		if remaining := time.Until(deadline); remaining < wait {
			wait = remaining
		}
		if err := waitOrCancel(rh.ctx, wait); err != nil {
			return err
		}
	}
}

func drainActionStatus(task cms.MaintenanceTask) (Ydb_Maintenance.ActionState_ActionStatus, string, error) {
	if task == nil {
		return Ydb_Maintenance.ActionState_ACTION_STATUS_UNSPECIFIED, "", fmt.Errorf("task is nil")
	}

	groups := task.GetActionGroupStates()
	if len(groups) == 0 || len(groups[0].GetActionStates()) == 0 {
		return Ydb_Maintenance.ActionState_ACTION_STATUS_UNSPECIFIED, "", fmt.Errorf("task has no action state")
	}

	state := groups[0].GetActionStates()[0]
	if state.GetAction().GetDrainAction() == nil {
		return Ydb_Maintenance.ActionState_ACTION_STATUS_UNSPECIFIED, state.GetDetails(), fmt.Errorf("task action is not a drain")
	}
	return state.GetStatus(), state.GetDetails(), nil
}

func (rh *restartHandler) stop(waitForDelay bool) {
	close(rh.queue)
	if waitForDelay {
		rh.wg.Wait()
	}
}

func newRestartHandler(
	ctx context.Context,
	logger *zap.SugaredLogger,
	restarter restarters.Restarter,
	cmsClient maintenanceClient,
	nodesInflight int,
	delayBetweenRestarts time.Duration,
	cmsQueryInterval time.Duration,
	availabilityMode Ydb_Maintenance.AvailabilityMode,
	priority int32,
	withDrain bool,
	nodes map[uint32]*Ydb_Maintenance.Node,
	drains *drainTracker,
	statusCh chan<- restartStatus,
) *restartHandler {
	return &restartHandler{
		ctx:                  ctx,
		logger:               logger,
		restarter:            restarter,
		cms:                  cmsClient,
		queue:                make(chan *Ydb_Maintenance.ActionGroupStates),
		statusCh:             statusCh,
		drains:               drains,
		nodesInflight:        nodesInflight,
		nodes:                nodes,
		delayBetweenRestarts: delayBetweenRestarts,
		cmsQueryInterval:     cmsQueryInterval,
		drainTimeout:         DefaultDrainWaitTimeout,
		drainTimeoutAction:   DrainWaitTimeoutActionSkip,
		availabilityMode:     availabilityMode,
		priority:             priority,
		withDrain:            withDrain,
	}
}
