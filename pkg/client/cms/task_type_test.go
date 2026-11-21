package cms

import (
	"context"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/draft/protos/Ydb_Maintenance"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestMaintenanceTaskTypes(t *testing.T) {
	for _, taskType := range []MaintenanceTaskType{LockTask, DrainTask} {
		params := MaintenanceTaskParams{TaskType: taskType, Nodes: []*Ydb_Maintenance.Node{{NodeId: 42}}, Hosts: []string{"host"}, Duration: durationpb.New(time.Minute)}
		nodes, err := actionGroupsFromNodes(params)
		if err != nil {
			t.Fatal(err)
		}
		hosts, err := actionGroupsFromHosts(params)
		if err != nil {
			t.Fatal(err)
		}
		for i, group := range []*Ydb_Maintenance.ActionGroup{nodes[0], hosts[0]} {
			if len(group.Actions) != 1 {
				t.Fatal("expected one action")
			}
			var scope *Ydb_Maintenance.ActionScope
			if taskType == DrainTask {
				scope = group.Actions[0].GetDrainAction().GetScope()
			} else {
				scope = group.Actions[0].GetLockAction().GetScope()
				if group.Actions[0].GetLockAction().GetDuration().AsDuration() != time.Minute {
					t.Fatal("lock duration lost")
				}
			}
			if i == 0 && scope.GetNodeId() != 42 || i == 1 && scope.GetHost() != "host" {
				t.Fatalf("wrong scope: %v", scope)
			}
		}
	}
	if err := validateMaintenanceTaskType(MaintenanceTaskType(99)); err == nil {
		t.Fatal("invalid type accepted")
	}
}

func TestWithContextDoesNotMutateClient(t *testing.T) {
	original := &defaultCMSClient{ctx: context.Background()}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	clone := original.WithContext(ctx).(*defaultCMSClient)
	if original.ctx.Err() != nil || clone.ctx.Err() != context.Canceled {
		t.Fatal("context is not isolated")
	}
}
