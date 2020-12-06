package main

import (
	"google_cluster_project/TaskId"
	"google_cluster_project/meminfo"
	"google_cluster_project/void"
	"log"
	"math/rand"
)

type taskMrcInfo struct {
	mrcID    int
	maxMem   float32
	localMem float32
}

type matchingFunction struct {
	Match       map[int64]singleSlotMatchingFunction
	MrcMatching map[TaskId.TaskId]*taskMrcInfo
	Mrcs        map[int]*mrc
}

type singleSlotMatchingFunction struct {
	ConsumerMatch   map[TaskId.TaskId]TaskId.TaskId
	ProducerMatch   map[TaskId.TaskId]TaskId.TaskId
	ConsumerNoMatch map[TaskId.TaskId]void.Void
	ProducerNoMatch map[TaskId.TaskId]void.Void
	AllMemoryInfo   map[TaskId.TaskId]meminfo.MemInfo
}

func (mf *matchingFunction) assignMrcID() int {
	return int(rand.Int31n(int32(len(mf.Mrcs))))
}

func randomizeBit() bool {
	if rand.Int()%2 == 0 {
		return false
	}
	return true
}

// mrc and other pertinent information.
func (mf *matchingFunction) setMrc(file string) {
	mf.Mrcs = loadMrc(file)
	tasksMaxMemUsage := make(map[TaskId.TaskId]float32)
	for _, slotMatchingFunction := range mf.Match {
		for task, taskMeminfo := range slotMatchingFunction.AllMemoryInfo {
			if taskMeminfo.MaxUsage > tasksMaxMemUsage[task] {
				tasksMaxMemUsage[task] = taskMeminfo.MaxUsage
			}
		}
	}
	for task, maxMem := range tasksMaxMemUsage {
		mrcID := mf.assignMrcID()
		mf.MrcMatching[task] = &taskMrcInfo{
			mrcID:    mrcID,
			maxMem:   maxMem,
			localMem: maxMem,
		}
	}
}

func (mf *matchingFunction) setAcceptableMissRation(acceptableMissRatio float32) {
	for k := range mf.MrcMatching {
		mrcID := mf.MrcMatching[k].mrcID
		maxMem := mf.MrcMatching[k].maxMem
		mf.MrcMatching[k].localMem = (1.0 - mf.Mrcs[mrcID].getMissRatioFromNormalizedMissRatio(acceptableMissRatio)) * maxMem
	}
}

func (mf *matchingFunction) initSlot(s int64, slotInfo map[meminfo.MemInfo]void.Void) {
	if !mf.hasSlot(s) {
		mf.Match[s] = singleSlotMatchingFunction{
			ConsumerMatch:   make(map[TaskId.TaskId]TaskId.TaskId),
			ProducerMatch:   make(map[TaskId.TaskId]TaskId.TaskId),
			ConsumerNoMatch: make(map[TaskId.TaskId]void.Void),
			ProducerNoMatch: make(map[TaskId.TaskId]void.Void),
			AllMemoryInfo:   make(map[TaskId.TaskId]meminfo.MemInfo),
		}
	}
	for c := range slotInfo {
		mf.Match[s].AllMemoryInfo[meminfo.ToTaskId(c)] = c
	}
}

func (mf *matchingFunction) hasSlot(slot int64) bool {
	_, present := mf.Match[slot]
	return present
}

func (mf *matchingFunction) isConsumerMatched(consumer TaskId.TaskId, slot int64) bool {
	if !mf.hasSlot(slot) {
		return false
	}
	_, present := mf.Match[slot].ConsumerMatch[consumer]
	return present
}

func (mf *matchingFunction) isProducerMatched(producer TaskId.TaskId, slot int64) bool {
	if !mf.hasSlot(slot) {
		return false
	}
	_, present := mf.Match[slot].ProducerMatch[producer]
	return present
}

func (mf *matchingFunction) existsInSlot(task TaskId.TaskId, slot int64) bool {
	if !mf.hasSlot(slot) {
		return false
	}
	_, present := mf.Match[slot].AllMemoryInfo[task]
	return present
}

func (mf *matchingFunction) addMatch(consumer, producer TaskId.TaskId, slot int64) {
	mf.Match[slot].ConsumerMatch[consumer] = producer
	mf.Match[slot].ProducerMatch[producer] = consumer
}

func (mf *matchingFunction) addUnpairedConsumer(consumer TaskId.TaskId, slot int64) {
	mf.Match[slot].ConsumerNoMatch[consumer] = void.Void{}
}

func (mf *matchingFunction) addUnpairedProducer(producer TaskId.TaskId, slot int64) {
	mf.Match[slot].ProducerNoMatch[producer] = void.Void{}
}

func (mf *matchingFunction) getMatch(task TaskId.TaskId, slot int64) (TaskId.TaskId, bool) {
	if mf.isConsumerMatched(task, slot) {
		return mf.Match[slot].ConsumerMatch[task], true
	}
	if mf.isProducerMatched(task, slot) {
		return mf.Match[slot].ProducerMatch[task], true
	}
	return TaskId.TaskId{}, false
}

func shuffleMemInfo(ms []meminfo.MemInfo) {
	rand.Shuffle(len(ms), func(i, j int) {
		var tmpMem meminfo.MemInfo
		tmpMem = ms[i]
		ms[i] = ms[j]
		ms[j] = tmpMem
	})
}

func (mf *matchingFunction) assignRoles(slotInfo []meminfo.MemInfo) ([]meminfo.MemInfo, []meminfo.MemInfo) {
	consumers := make([]meminfo.MemInfo, len(slotInfo)/2)
	producers := make([]meminfo.MemInfo, len(slotInfo)/2)
	for _, s := range slotInfo {
		if randomizeBit() {
			consumers = append(consumers, s)
		} else {
			producers = append(producers, s)
		}
	}
	return consumers, producers
}

func (mf *matchingFunction) doMatch(consumers, producers []meminfo.MemInfo, slot int64) {
	shuffleMemInfo(consumers)
	shuffleMemInfo(producers)
	for len(producers) > 0 && len(consumers) > 0 {
		mf.addMatch(meminfo.ToTaskId(producers[0]), meminfo.ToTaskId(consumers[0]), slot)
		producers = producers[1:]
		consumers = consumers[1:]
	}
	for _, c := range consumers {
		mf.addUnpairedConsumer(meminfo.ToTaskId(c), slot)
	}
	for _, p := range producers {
		mf.addUnpairedProducer(meminfo.ToTaskId(p), slot)
	}
}

func createEmpty() *matchingFunction {
	mf := &matchingFunction{
		Match: make(map[int64]singleSlotMatchingFunction),
	}
	return mf
}

func (mf *matchingFunction) isConsumer(task TaskId.TaskId, slot int64) bool {
	if !mf.hasSlot(slot) {
		return false
	}

	if mf.isConsumerMatched(task, slot) {
		return true
	}

	_, present := mf.Match[slot].ConsumerNoMatch[task]
	return present
}

func (mf *matchingFunction) isProducer(task TaskId.TaskId, slot int64) bool {
	if !mf.hasSlot(slot) {
		return false
	}

	if mf.isProducerMatched(task, slot) {
		return true
	}

	_, present := mf.Match[slot].ProducerNoMatch[task]
	return present
}

func (mf *matchingFunction) nextSlotMatch(slotInfoSlice []meminfo.MemInfo) {
	slotInfo := meminfo.SliceToSet(slotInfoSlice)
	newTasks := make([]meminfo.MemInfo, 0)
	consumersToMatch := make([]meminfo.MemInfo, 0)
	producersToMatch := make([]meminfo.MemInfo, 0)
	lastSlot := int64(-1)
	for k := range mf.Match {
		if k > lastSlot {
			lastSlot = k
		}
	}
	currentSlot := lastSlot + 1
	if currentSlot%1000 == 0 {
		log.Println("Initializing slot:", currentSlot)
	}
	mf.initSlot(currentSlot, slotInfo)
	for c := range slotInfo {
		cTask := meminfo.ToTaskId(c)
		match, hasMatch := mf.getMatch(cTask, lastSlot)
		if hasMatch && meminfo.IsTaskInSet(match, slotInfo) {
			// If me and match are from previous round and in this round - add a match.
			// My match will be added when we get to him in the loop.
			if mf.isConsumer(cTask, lastSlot) {
				mf.addMatch(cTask, match, currentSlot)
			} else {
				mf.addMatch(match, cTask, currentSlot)
			}
		} else {
			// Otherwise - either I'm a new task or not.
			if mf.isConsumer(cTask, lastSlot) {
				// Not a new task - I already have a role.
				consumersToMatch = append(consumersToMatch, c)
			} else if mf.isProducer(cTask, lastSlot) {
				producersToMatch = append(producersToMatch, c)
			} else {
				// I'm a new class - I'll have to be assigned a role.
				newTasks = append(newTasks, c)
			}
		}
	}

	// Now all mathcing that existing from previous slot exist in this slot as well. We handle the unmatched.
	// First - give a role to all those new tasks that don't have a role yet.
	consumers, producers := mf.assignRoles(newTasks)
	consumers = append(consumers, consumersToMatch...)
	producers = append(producers, producersToMatch...)

	// Next - lets assign them to each other and update the unpaired
	mf.doMatch(consumers, producers, currentSlot)
}

func (mf *matchingFunction) getProducerTotalMemorSize(producer TaskId.TaskId) float32 {
	return mf.MrcMatching[producer].maxMem
}

func (mf *matchingFunction) getProducerSupply(producer TaskId.TaskId, slot int64) float32 {
	return mf.getProducerTotalMemorSize(producer) - mf.getMemoryUsage(producer, slot)
}

func (mf *matchingFunction) getMaximalMemoryUsage(task TaskId.TaskId, slot int64) float32 {
	return mf.Match[slot].AllMemoryInfo[task].MaxUsage
}

func (mf *matchingFunction) getMemoryUsage(task TaskId.TaskId, slot int64) float32 {
	return mf.Match[slot].AllMemoryInfo[task].AvgUsing
}

func (mf *matchingFunction) getLocalAvailableMemory(consumer TaskId.TaskId) float32 {
	return mf.MrcMatching[consumer].localMem
}

func (mf *matchingFunction) getRemoteDemand(consumer TaskId.TaskId, slot int64) float32 {
	return mf.getMaximalMemoryUsage(consumer, slot) - mf.getLocalAvailableMemory(consumer)
}

func (mf *matchingFunction) getConsumerRemoteUsage(consumer TaskId.TaskId, slot int64) float32 {
	producer, hasMatch := mf.getMatch(consumer, slot)
	demand := mf.getRemoteDemand(consumer, slot)
	if !hasMatch {
		return 0
	}
	supply := mf.getProducerSupply(producer, slot)
	if demand > supply {
		return supply
	}
	return demand
}

func (mf *matchingFunction) getConsumerSatisfactionRateAtSlot(consumer TaskId.TaskId, slot int64) float32 {
	demand := mf.getRemoteDemand(consumer, slot)

	// If there is no remote demand - we consider the consumer is fully satisfied.
	if demand == float32(0) {
		return 1
	}
	actualUsage := mf.getConsumerRemoteUsage(consumer, slot)
	return actualUsage / demand
}

func (mf *matchingFunction) getMrc(task TaskId.TaskId) *mrc {
	return mf.Mrcs[mf.MrcMatching[task].mrcID]
}

func (mf *matchingFunction) getConsumerMissRatioAtSlot(consumer TaskId.TaskId, slot int64) float32 {
	missAmount := mf.getRemoteDemand(consumer, slot) - mf.getConsumerRemoteUsage(consumer, slot)
	normalizedAvailableMemory := 1.0 - missAmount/mf.getMaximalMemoryUsage(consumer, slot)
	return mf.getMrc(consumer).getMissRatioFromNormalizedAvailableMemory(normalizedAvailableMemory)
}

func (mf *matchingFunction) getProducerUtilizationRateAtSlot(producer TaskId.TaskId, slot int64) float32 {
	match, has_match := mf.getMatch(producer, slot)
	if !has_match {
		return 0
	}
	usage := mf.getConsumerRemoteUsage(match, slot)
	return usage / mf.getProducerSupply(producer, slot)
}

type slotUsageStatus struct {
	demand           float32
	supply           float32
	satisfactionRate float32
	utilizationRate  float32
	averageMissRatio float32
}

func (mf *matchingFunction) analyzeSlot(slot int64) slotUsageStatus {
	res := slotUsageStatus{}
	sumSatisfactionRate := float32(0)
	countSatisfactionRate := 0
	missRatioSum := float32(0)
	missRatioCount := 0
	for c := range mf.Match[slot].ConsumerMatch {
		res.demand += mf.getRemoteDemand(c, slot)
		sumSatisfactionRate += mf.getConsumerSatisfactionRateAtSlot(c, slot)
		countSatisfactionRate++
		missRatioSum += mf.getConsumerMissRatioAtSlot(c, slot)
		missRatioCount++
	}
	for c := range mf.Match[slot].ConsumerNoMatch {
		res.demand += mf.getRemoteDemand(c, slot)
		sumSatisfactionRate += mf.getConsumerSatisfactionRateAtSlot(c, slot)
		countSatisfactionRate++
		missRatioSum += mf.getConsumerMissRatioAtSlot(c, slot)
		missRatioCount++
	}
	res.satisfactionRate = sumSatisfactionRate / float32(countSatisfactionRate)
	res.averageMissRatio = missRatioSum / float32(missRatioCount)

	utilizationCount := 0
	utilizationSum := float32(0)
	for p := range mf.Match[slot].ProducerMatch {
		res.supply += mf.getProducerSupply(p, slot)
		utilizationSum += mf.getProducerUtilizationRateAtSlot(p, slot)
		utilizationCount++
	}
	for p := range mf.Match[slot].ProducerNoMatch {
		res.supply += mf.getProducerSupply(p, slot)
		utilizationCount++
	}
	res.utilizationRate = utilizationSum / float32(utilizationCount)

	return res
}
