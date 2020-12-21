package main

import (
	"google_cluster_project/TaskId"
	"google_cluster_project/meminfo"
	"google_cluster_project/void"
	"math/rand"

	log "github.com/sirupsen/logrus"
)

type taskMrcInfo struct {
	mrcID  int
	maxMem float32
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

func (mf *matchingFunction) iterateConsumers(slot int64) chan TaskId.TaskId {
	res := make(chan TaskId.TaskId)
	go func() {
		for i := range mf.Match[slot].ConsumerMatch {
			res <- i
		}
		for i := range mf.Match[slot].ConsumerNoMatch {
			res <- i
		}
		close(res)
	}()
	return res
}

func (mf *matchingFunction) iterateProducers(slot int64) chan TaskId.TaskId {
	res := make(chan TaskId.TaskId)
	go func() {
		for i := range mf.Match[slot].ProducerMatch {
			res <- i
		}
		for i := range mf.Match[slot].ProducerNoMatch {
			res <- i
		}
		close(res)
	}()
	return res
}

// mrc and other pertinent information.
func (mf *matchingFunction) setMrc(file string, histogramsDir string) {
	mf.Mrcs = loadMrc(file)
	tasksMaxMemUsage := make(map[TaskId.TaskId]float32)
	for histogram := range generateHistogramsFromHistogramDir(histogramsDir) {
		for _, taskMeminfo := range histogram.MemInfo {
			task := meminfo.ToTaskId(taskMeminfo)
			// for task, taskMeminfo := range slotMatchingFunction.AllMemoryInfo {
			_, present := tasksMaxMemUsage[task]
			if !present {
				tasksMaxMemUsage[task] = float32(0)
			}
			if taskMeminfo.MaxUsage > tasksMaxMemUsage[task] {
				tasksMaxMemUsage[task] = taskMeminfo.MaxUsage
			}
		}
	}
	for task, maxMem := range tasksMaxMemUsage {
		mrcID := mf.assignMrcID()
		mf.MrcMatching[task] = &taskMrcInfo{
			mrcID:  mrcID,
			maxMem: maxMem,
		}
	}
}

func (mf *matchingFunction) getLocalMem(acceptableMissRatio float32, task TaskId.TaskId) float32 {
	mrcID := mf.MrcMatching[task].mrcID
	maxMem := mf.MrcMatching[task].maxMem
	return (1.0 - mf.Mrcs[mrcID].getMissRatioFromNormalizedMissRatio(acceptableMissRatio)) * maxMem
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
	consumers := make([]meminfo.MemInfo, 0)
	producers := make([]meminfo.MemInfo, 0)
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
		Match:       make(map[int64]singleSlotMatchingFunction),
		MrcMatching: make(map[TaskId.TaskId]*taskMrcInfo),
		Mrcs:        make(map[int]*mrc),
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

func (mf *matchingFunction) deleteSlot(slot int64) {
	if mf.hasSlot(slot) {
		delete(mf.Match, slot)
	}
}

func (mf *matchingFunction) nextSlotMatch(slotInfoSlice []meminfo.MemInfo, slot int64) {
	slotInfo := meminfo.SliceToSet(slotInfoSlice)
	newTasks := make([]meminfo.MemInfo, 0)
	consumersToMatch := make([]meminfo.MemInfo, 0)
	producersToMatch := make([]meminfo.MemInfo, 0)
	lastSlot := int64(0)
	for k := range mf.Match {
		if k > lastSlot {
			lastSlot = k
		}
	}
	currentSlot := slot

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

func (mf *matchingFunction) getLocalAvailableMemory(consumer TaskId.TaskId, acceptableMissRatio float32) float32 {
	return mf.getLocalMem(acceptableMissRatio, consumer)
	// mf.MrcMatching[consumer].localMem
}

func (mf *matchingFunction) getRemoteDemand(consumer TaskId.TaskId, slot int64, acceptableMissRatio float32) float32 {
	maxUsageInSlot := mf.getMaximalMemoryUsage(consumer, slot)
	localMemSize := mf.getLocalAvailableMemory(consumer, acceptableMissRatio)
	if maxUsageInSlot-localMemSize < 0 {
		return 0
	}
	return maxUsageInSlot - localMemSize
}

func (mf *matchingFunction) getMatchedConsumerCount(slot int64) int {
	return len(mf.Match[slot].ConsumerMatch)
}

func (mf *matchingFunction) getUnmatchedConsumerCount(slot int64) int {
	return len(mf.Match[slot].ConsumerNoMatch)
}

func (mf *matchingFunction) getMatchedProducerCount(slot int64) int {
	return len(mf.Match[slot].ProducerMatch)
}

func (mf *matchingFunction) getUnmatchedProducerCount(slot int64) int {
	return len(mf.Match[slot].ProducerNoMatch)
}

func (mf *matchingFunction) getConsumerCount(slot int64) int {
	return mf.getMatchedConsumerCount(slot) + mf.getUnmatchedConsumerCount(slot)
}

func (mf *matchingFunction) getProducerCount(slot int64) int {
	return mf.getMatchedProducerCount(slot) + mf.getUnmatchedProducerCount(slot)
}

func (mf *matchingFunction) getConsumerRemoteUsage(consumer TaskId.TaskId, slot int64, acceptableMissRatio float32) float32 {
	producer, hasMatch := mf.getMatch(consumer, slot)
	demand := mf.getRemoteDemand(consumer, slot, acceptableMissRatio)
	if !hasMatch {
		return 0
	}
	supply := mf.getProducerSupply(producer, slot)
	if demand > supply {
		return supply
	}
	if demand < 0 {
		log.Println("Demand is negative!")
	}
	return demand
}

func (mf *matchingFunction) getConsumerSatisfactionRateAtSlot(consumer TaskId.TaskId, slot int64, acceptableMissRatio float32) float32 {
	demand := mf.getRemoteDemand(consumer, slot, acceptableMissRatio)

	// If there is no remote demand - we consider the consumer is fully satisfied.
	if demand <= float32(0) {
		return 1
	}
	actualUsage := mf.getConsumerRemoteUsage(consumer, slot, acceptableMissRatio)
	return actualUsage / demand
}

func (mf *matchingFunction) getMrc(task TaskId.TaskId) *mrc {
	return mf.Mrcs[mf.MrcMatching[task].mrcID]
}

// To compute the miss ratio at a given slot for a given consumer we do the following:
// 1. Calculate the remote *DEMAND* (i.e how much the consmer is willing to lend from the producer)
// 2. Calculate how much memory is the consumer *ACTUALLY* consuming.
// 3. Calculate the missing memory about by performing the subtraction of bullet #1 minus bullet #2.
// 4. Normalize the amount of missed memory by deviding the missed memory by the maximal memory usage by this consumer.
// 5. Calculate the normalized available memory as "1  - normalized_missed_memory"
// 4. With the missing memory we want to know how much misses are experienced when this amount of memory is missing.
// 	this is done using the mrc.
func (mf *matchingFunction) getConsumerMissRatioAtSlot(consumer TaskId.TaskId, slot int64, acceptableMissRatio float32) float32 {
	missAmount := mf.getRemoteDemand(consumer, slot, acceptableMissRatio) - mf.getConsumerRemoteUsage(consumer, slot, acceptableMissRatio)
	if missAmount < 0 {
		missAmount = 0.0
	}
	normalizedAvailableMemory := float32(1.0)
	if missAmount != 0.0 {
		normalizedAvailableMemory -= missAmount / mf.getMaximalMemoryUsage(consumer, slot)
	}
	if normalizedAvailableMemory > 1.0 {
		log.Trace("getMaximalMemoryUsage", mf.getMaximalMemoryUsage(consumer, slot), " missAmount ", missAmount)
	}
	return mf.getMrc(consumer).getMissRatioFromNormalizedAvailableMemory(normalizedAvailableMemory)
}

func (mf *matchingFunction) getProducerUtilizationRateAtSlot(producer TaskId.TaskId, slot int64, acceptableMissRatio float32) float32 {
	match, has_match := mf.getMatch(producer, slot)
	if !has_match {
		return 0
	}
	usage := mf.getConsumerRemoteUsage(match, slot, acceptableMissRatio)
	supply := mf.getProducerSupply(producer, slot)
	if supply <= 0 {
		return float32(1.0)
	}
	if usage < 0 {
		log.Println("Usage is negative!!!")
	}
	return usage / mf.getProducerSupply(producer, slot)
}

type slotUsageStatus struct {
	Demand             float32
	Supply             float32
	SatisfactionRate   float32
	UtilizationRate    float32
	AverageMissRatio   float32
	ProducersLeft      int
	ConsumersLeft      int
	ProducersJoined    int
	ConsumersJoined    int
	TotalConsumers     int
	TotalProducers     int
	UnmatchedConsumers int
	UnmatchedProducers int
}

func (mf *matchingFunction) analyzeSlot(slot int64, acceptableMissRatio float32) slotUsageStatus {
	res := slotUsageStatus{}
	sumSatisfactionRate := float32(0)
	countSatisfactionRate := 0
	missRatioSum := float32(0)
	missRatioCount := 0
	for c := range mf.Match[slot].ConsumerMatch {
		res.Demand += mf.getRemoteDemand(c, slot, acceptableMissRatio)
		sumSatisfactionRate += mf.getConsumerSatisfactionRateAtSlot(c, slot, acceptableMissRatio)
		countSatisfactionRate++
		missRatioSum += mf.getConsumerMissRatioAtSlot(c, slot, acceptableMissRatio)
		missRatioCount++
	}
	for c := range mf.Match[slot].ConsumerNoMatch {
		res.Demand += mf.getRemoteDemand(c, slot, acceptableMissRatio)
		sumSatisfactionRate += mf.getConsumerSatisfactionRateAtSlot(c, slot, acceptableMissRatio)
		countSatisfactionRate++
		missRatioSum += mf.getConsumerMissRatioAtSlot(c, slot, acceptableMissRatio)
		missRatioCount++
	}
	res.SatisfactionRate = sumSatisfactionRate / float32(countSatisfactionRate)
	res.AverageMissRatio = missRatioSum / float32(missRatioCount)

	utilizationCount := 0
	utilizationSum := float32(0)
	for p := range mf.Match[slot].ProducerMatch {
		res.Supply += mf.getProducerSupply(p, slot)
		utilizationSum += mf.getProducerUtilizationRateAtSlot(p, slot, acceptableMissRatio)
		utilizationCount++
	}
	for p := range mf.Match[slot].ProducerNoMatch {
		res.Supply += mf.getProducerSupply(p, slot)
		utilizationCount++
	}
	res.UtilizationRate = utilizationSum / float32(utilizationCount)
	res.TotalConsumers = mf.getConsumerCount(slot)
	res.TotalProducers = mf.getProducerCount(slot)
	res.UnmatchedConsumers = mf.getUnmatchedConsumerCount(slot)
	res.UnmatchedProducers = mf.getUnmatchedProducerCount(slot)
	for c := range mf.iterateConsumers(slot) {
		if !mf.isConsumer(c, slot-1) {
			res.ConsumersJoined++
		}
	}
	for c := range mf.iterateConsumers(slot - 1) {
		if !mf.isConsumer(c, slot) {
			res.ConsumersLeft++
		}
	}
	for p := range mf.iterateProducers(slot) {
		if !mf.isProducer(p, slot-1) {
			res.ProducersJoined++
		}
	}
	for p := range mf.iterateProducers(slot - 1) {
		if !mf.isProducer(p, slot) {
			res.ProducersLeft++
		}
	}

	return res
}
