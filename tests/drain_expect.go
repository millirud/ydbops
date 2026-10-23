package tests

import (
	"regexp"
	"sort"
	"strconv"

	"github.com/ydb-platform/ydb-go-genproto/draft/protos/Ydb_Maintenance"
	"google.golang.org/protobuf/proto"
)

var rollingDrainUID = regexp.MustCompile(`^rolling-drain-(\d+)-`)

func drainNodeID(req proto.Message) (uint32, bool) {
	switch typed := req.(type) {
	case *Ydb_Maintenance.CreateMaintenanceTaskRequest:
		groups := typed.GetActionGroups()
		if len(groups) == 0 || len(groups[0].GetActions()) == 0 {
			return 0, false
		}
		drain := groups[0].GetActions()[0].GetDrainAction()
		if drain == nil {
			return 0, false
		}
		return drain.GetScope().GetNodeId(), true
	case *Ydb_Maintenance.GetMaintenanceTaskRequest:
		return nodeIDFromDrainUID(typed.GetTaskUid())
	case *Ydb_Maintenance.DropMaintenanceTaskRequest:
		return nodeIDFromDrainUID(typed.GetTaskUid())
	default:
		return 0, false
	}
}

func nodeIDFromDrainUID(taskUID string) (uint32, bool) {
	match := rollingDrainUID.FindStringSubmatch(taskUID)
	if match == nil {
		return 0, false
	}
	nodeID, err := strconv.ParseUint(match[1], 10, 32)
	if err != nil {
		return 0, false
	}
	return uint32(nodeID), true
}

func normalizeDrainRequestOrder(reqs []proto.Message) []proto.Message {
	out := make([]proto.Message, 0, len(reqs))
	for i := 0; i < len(reqs); {
		if _, ok := drainNodeID(reqs[i]); !ok {
			out = append(out, reqs[i])
			i++
			continue
		}
		j := i + 1
		for j < len(reqs) {
			if _, ok := drainNodeID(reqs[j]); !ok {
				break
			}
			j++
		}
		run := append([]proto.Message(nil), reqs[i:j]...)
		sort.SliceStable(run, func(a, b int) bool {
			aNode, _ := drainNodeID(run[a])
			bNode, _ := drainNodeID(run[b])
			return aNode < bNode
		})
		out = append(out, run...)
		i = j
	}
	return out
}
