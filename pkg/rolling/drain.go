package rolling

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/ydb-platform/ydb-go-genproto/draft/protos/Ydb_Maintenance"

	"github.com/ydb-platform/ydbops/pkg/client/cms"
)

// Drain failures must stop the rolling restart rather than enter the restart retry loop.
type drainError struct{ err error }

func (e *drainError) Error() string { return e.err.Error() }
func (e *drainError) Unwrap() error { return e.err }

func (r *Rolling) restartNode(ctx context.Context, node *Ydb_Maintenance.Node, cancel context.CancelCauseFunc) error {
	if r.opts.WithDrain {
		taskID := r.state.restartTaskUID + "-drain-" + uuid.NewString()
		// Keep the drain task until RestartNode returns. Also clean up after failed or
		// cancelled creation: CMS may have accepted the request before the RPC failed.
		defer func() {
			r.logger.Debugf("Dropping drain task %s for node %d", taskID, node.NodeId)
			if err := r.cleanupMaintenanceTask(ctx, taskID); err != nil {
				r.logger.Warnf("Failed to drop drain task %s for node %d: %v", taskID, node.NodeId, err)
				return
			}
			r.logger.Debugf("Dropped drain task %s for node %d", taskID, node.NodeId)
		}()
		if err := r.drainNode(ctx, node, taskID); err != nil {
			failure := &drainError{err}
			// Stop peer workers before cleanup, which may take up to 30 seconds.
			// Preserve the failure as the cancellation cause for the status loop.
			if cancel != nil {
				cancel(failure)
			}
			return failure
		}
	}
	if err := ctx.Err(); err != nil {
		return &drainError{err}
	}
	return r.restarter.RestartNode(node)
}

func (r *Rolling) drainNode(ctx context.Context, node *Ydb_Maintenance.Node, taskID string) error {
	waitCtx, cancel := context.WithTimeout(ctx, r.opts.DrainWaitTimeout)
	defer cancel()
	cl := r.cms.WithContext(waitCtx)
	r.logger.Debugf("Creating drain task %s for node %d", taskID, node.NodeId)
	_, err := cl.CreateMaintenanceTask(cms.MaintenanceTaskParams{
		TaskUID:          taskID,
		TaskType:         cms.DrainTask,
		ScopeType:        cms.NodeScope,
		Nodes:            []*Ydb_Maintenance.Node{node},
		AvailabilityMode: r.opts.GetAvailabilityMode(),
		Priority:         int32(r.opts.Priority),
	})
	if err == nil {
		r.logger.Debugf("Created drain task %s for node %d; waiting for ACTION_STATUS_PERFORMED via GetMaintenanceTask", taskID, node.NodeId)
	}
	interval := time.Duration(r.opts.CMSQueryInterval) * time.Second
	if interval <= 0 {
		interval = time.Second
	}
	for err == nil {
		if waitCtx.Err() != nil {
			err = waitCtx.Err()
			break
		}
		// Observe completion through GetMaintenanceTask, including the first
		// check after creation. RefreshMaintenanceTask is not a status read.
		task, getErr := cl.GetMaintenanceTask(taskID)
		if getErr != nil {
			err = getErr
			break
		}
		if err = waitCtx.Err(); err != nil {
			break
		}
		if drainPerformed(task) {
			r.logger.Debugf("Confirmed drain task %s for node %d: ACTION_STATUS_PERFORMED via GetMaintenanceTask", taskID, node.NodeId)
			return nil
		}
		if err = waitOrCancel(waitCtx, interval); err != nil {
			break
		}
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if waitCtx.Err() == context.DeadlineExceeded {
		if r.opts.DrainWaitTimeoutAction == DrainWaitTimeoutActionForce {
			r.logger.Warnf("Drain task %s for node %d exceeded %s; continuing restart (--drain-wait-timeout-action=force)", taskID, node.NodeId, r.opts.DrainWaitTimeout)
			return nil
		}
		return fmt.Errorf("drain task %s for node %d exceeded %s: %w", taskID, node.NodeId, r.opts.DrainWaitTimeout, context.DeadlineExceeded)
	}
	return fmt.Errorf("drain task %s failed for node %d: %w", taskID, node.NodeId, err)
}

func drainPerformed(task cms.MaintenanceTask) bool {
	if task == nil || len(task.GetActionGroupStates()) == 0 {
		return false
	}
	for _, group := range task.GetActionGroupStates() {
		if len(group.GetActionStates()) == 0 {
			return false
		}
		for _, action := range group.GetActionStates() {
			if action.GetStatus() != Ydb_Maintenance.ActionState_ACTION_STATUS_PERFORMED {
				return false
			}
		}
	}
	return true
}
