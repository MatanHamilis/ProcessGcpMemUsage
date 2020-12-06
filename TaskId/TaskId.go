package TaskId

import (
	"sort"
)

type By func(t1, t2 *TaskId) bool

func IdCompare(t1, t2 *TaskId) bool {
	if t1.CollectionId < t2.CollectionId {
		return true
	}
	if t1.CollectionId > t2.CollectionId {
		return false
	}
	if t1.InstanceId < t2.InstanceId {
		return true
	}
	return false
}

type TaskId struct {
	CollectionId int64
	InstanceId   int32
}

type TaskIdSorter struct {
	tasks []TaskId
	by    By
}

func (ts *TaskIdSorter) Less(i, j int) bool {
	return ts.by(&ts.tasks[i], &ts.tasks[j])
}

func (ts *TaskIdSorter) Swap(i, j int) {
	a := ts.tasks[i]
	ts.tasks[i] = ts.tasks[j]
	ts.tasks[j] = a
}

func (ts *TaskIdSorter) Len() int {
	return len(ts.tasks)
}

func (by By) Sort(tasks []TaskId) {
	ts := &TaskIdSorter{
		tasks: tasks,
		by:    by,
	}
	sort.Sort(ts)
}
