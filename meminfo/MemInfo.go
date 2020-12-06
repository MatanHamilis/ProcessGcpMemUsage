package meminfo

import (
	"google_cluster_project/TaskId"
	"google_cluster_project/void"
	"sort"
)

type MemInfo struct {
	CollectionId int64
	InstanceId   int32
	AvgUsing     float32
	MaxUsage     float32
	MaxAvail     float32
}

type MemInfoBy func(m1, m2 *MemInfo) bool

func MemInfoById(m1, m2 *MemInfo) bool {
	if m1.CollectionId > m2.CollectionId {
		return false
	}
	if m1.CollectionId < m2.CollectionId {
		return true
	}
	if m1.CollectionId > m2.CollectionId {
		return false
	}
	return true
}

func MemInfoByMaxAvail(m1, m2 *MemInfo) bool {
	return m1.MaxAvail < m2.MaxAvail
}

type memInfoSorter struct {
	memInfos []MemInfo
	by       MemInfoBy
}

func (ms *memInfoSorter) Len() int {
	return len(ms.memInfos)
}

func (ms *memInfoSorter) Swap(i, j int) {
	a := ms.memInfos[i]
	ms.memInfos[i] = ms.memInfos[j]
	ms.memInfos[j] = a
}

func (ms *memInfoSorter) Less(i, j int) bool {
	return ms.by(&ms.memInfos[i], &ms.memInfos[j])
}

func (by MemInfoBy) Sort(memInfos []MemInfo) {
	ms := &memInfoSorter{
		memInfos: memInfos,
		by:       by,
	}
	sort.Sort(ms)
}

func SliceToSet(ms []MemInfo) map[MemInfo]void.Void {
	set := make(map[MemInfo]void.Void)
	for _, m := range ms {
		set[m] = void.Void{}
	}

	return set
}

func IsTaskInSet(task TaskId.TaskId, ms map[MemInfo]void.Void) bool {
	for m := range ms {
		mTask := ToTaskId(m)
		if task == mTask {
			return true
		}
	}
	return false
}

func ToTaskId(m MemInfo) TaskId.TaskId {
	return TaskId.TaskId{
		CollectionId: m.CollectionId,
		InstanceId:   m.InstanceId,
	}
}
