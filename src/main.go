package main

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"google_cluster_project/TaskId"
	"google_cluster_project/google_cluster_data"
	"google_cluster_project/meminfo"
	"hash/crc32"
	"math"
	"math/rand"
	"os"
	"path"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"

	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/encoding/protojson"
)

type instanceUsage google_cluster_data.InstanceUsage

const (
	processedHistogramDir           = "processed_hist_part"
	consolidatedDir                 = "consolidated_histogram_dir"
	histogramPartPrefix             = "histogram_part_"
	honsolidatedHistogramPartPrefix = "consolidated_part_"
)

var randomSeed uint32

var multithreadingFactor int

func generateInstanceUsageFromLines(lines chan string) chan *google_cluster_data.InstanceUsage {
	instanceUsageChan := make(chan *google_cluster_data.InstanceUsage)
	go func() {
		for line := range lines {
			var v google_cluster_data.InstanceUsage
			t := strings.Replace(line, "collection_type\":\"0\"", "collection_type\":0", 1)
			t = strings.Replace(t, "collection_type\":\"1\"", "collection_type\":1", 1)
			b := []byte(t)
			err := protojson.Unmarshal(b, &v)
			if err != nil {
				log.Println(t)
			}
			if v.CollectionType == google_cluster_data.CollectionType_ALLOC_SET.Enum() {
				continue
			}
			if v.AverageUsage == nil {
				continue
			}
			if v.StartTime == nil {
				log.Println("Start Time Nil! ")
			}
			if v.GetStartTime() == 0 || v.GetEndTime() == math.MaxInt64 {
				continue
			}
			if v.GetAssignedMemory() == 0 {
				continue
			}
			instanceUsageChan <- &v
		}
		close(instanceUsageChan)
	}()

	return instanceUsageChan
}

func parseInstanceUsage(histChan chan *usageHistogramKeyValuePair, f chan string, wg *sync.WaitGroup) {
	for file := range f {
		log.Println("Processing file: ", file)
		hist := generateHistogramFromStream(generateInstanceUsageFromLines(generateGzipLines(file)))
		pair := &usageHistogramKeyValuePair{
			hist:     hist,
			filepath: file,
		}
		histChan <- pair
	}
	wg.Done()
}

type usageHistogram map[int64][]meminfo.MemInfo

func (hp usageHistogram) add(u *google_cluster_data.InstanceUsage) {
	const slotSize = 300 * 1000000
	slot := *u.StartTime / slotSize
	v, exists := hp[slot]
	if !exists {
		hp[slot] = make([]meminfo.MemInfo, 0, 5)
		v = hp[slot]
	}
	memInfo := meminfo.MemInfo{
		AvgUsing:     u.AverageUsage.GetMemory(),
		MaxAvail:     u.GetAssignedMemory(),
		MaxUsage:     u.MaximumUsage.GetMemory(),
		CollectionId: u.GetCollectionId(),
		InstanceId:   u.GetInstanceIndex(),
	}
	hp[slot] = append(v, memInfo)
}

func generateHistogramFromStream(c chan *google_cluster_data.InstanceUsage) *usageHistogram {
	m := make(usageHistogram)
	for l := range c {
		m.add(l)
	}
	return &m
}

type usageHistogramKeyValuePair struct {
	hist     *usageHistogram
	filepath string
}

func processEachInstanceEntryInDir(dir string, filter string) chan *usageHistogramKeyValuePair {
	histChan := make(chan *usageHistogramKeyValuePair)
	go func() {
		c := iterateFilesInDir(dir, filter)

		numOfReaders := multithreadingFactor * 2
		log.Println("Starting ", numOfReaders, " readers")
		var instanceUsageWg sync.WaitGroup
		instanceUsageWg.Add(numOfReaders)
		for i := 0; i < numOfReaders; i++ {
			go parseInstanceUsage(histChan, c, &instanceUsageWg)
		}
		instanceUsageWg.Wait()
		close(histChan)
	}()

	return histChan
}

type histInfo struct {
	m meminfo.MemInfo
	s int64
}

func emitMemInfo(uc chan *usageHistogram) chan histInfo {
	h := make(chan histInfo)

	var wg sync.WaitGroup
	numOfWorkers := multithreadingFactor * 2
	wg.Add(numOfWorkers)
	for i := 0; i < numOfWorkers; i++ {
		go func() {
			for u := range uc {
				for slot, slotMap := range *u {
					for _, memInfo := range slotMap {
						h <- histInfo{
							m: memInfo,
							s: slot,
						}
					}
				}
			}
			wg.Done()
		}()
	}

	go func() {
		wg.Wait()
		close(h)
	}()
	return h
}

type memInfoWithSlot struct {
	MemInfo []meminfo.MemInfo
	Slot    int64
}

func splitHistogram(splitSize int64, hist *usageHistogram, outputChannels []chan *memInfoWithSlot) {
	modulus := int64(len(outputChannels))
	for k, v := range *hist {
		chanID := (k / splitSize) % modulus
		outputChannels[chanID] <- &memInfoWithSlot{
			MemInfo: v,
			Slot:    k,
		}
	}
}

func doWriter(ch chan *memInfoWithSlot, outputDir string, wg *sync.WaitGroup, slotsPerFile int64) {
	files := make(map[int64]*json.Encoder)
	underlyingFiles := make([]*gzip.Writer, 0)
	for m := range ch {
		fileID := m.Slot / slotsPerFile
		w, present := files[fileID]
		if !present {
			filePath := path.Join(outputDir, histogramPartPrefix+strconv.Itoa(int(fileID))+".json.gz")
			newFile, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)
			if err != nil {
				log.Panicln("Failed to create file. ", filePath, err)
			}
			log.Println("WRITER, created new file", filePath)
			gzipWriter := gzip.NewWriter(newFile)
			files[fileID] = json.NewEncoder(gzipWriter)
			underlyingFiles = append(underlyingFiles, gzipWriter)
			w = files[fileID]
		}
		w.Encode(m)
	}
	for i := range underlyingFiles {
		underlyingFiles[i].Close()
	}
	wg.Done()
}

func sendMemInfoToWriters(histChan chan *usageHistogramKeyValuePair, numberOfWriters int64, writers []chan *memInfoWithSlot, wg *sync.WaitGroup, slotsPerSplit int64) {
	var writerID int64
	for h := range histChan {
		for k, v := range *h.hist {
			writerID = (k / slotsPerSplit) % numberOfWriters
			writers[writerID] <- &memInfoWithSlot{
				MemInfo: v,
				Slot:    k,
			}

		}
	}
	wg.Done()
}

func generateHistParts(histChan chan *usageHistogramKeyValuePair, outputDir string) {
	const slotsPerSplit = 100
	const wgSize = 10
	numberOfWriters := int64(multithreadingFactor * 2)
	var wg sync.WaitGroup
	var writersWg sync.WaitGroup
	wg.Add(wgSize)
	writersWg.Add(int(numberOfWriters))
	writers := make([]chan *memInfoWithSlot, numberOfWriters)

	// Writer i responsible for timeslots j s.t.:
	// ((j/slotsPerSplit) % numOfWriters) == i
	for i := range writers {
		writers[i] = make(chan *memInfoWithSlot)
	}
	for i := int64(0); i < numberOfWriters; i++ {
		go doWriter(writers[i], outputDir, &writersWg, slotsPerSplit)
	}
	for i := 0; i < wgSize; i++ {
		go sendMemInfoToWriters(histChan, numberOfWriters, writers, &wg, slotsPerSplit)
	}
	wg.Wait()
	for i := range writers {
		close(writers[i])
	}
	writersWg.Wait()
}

func consolidateEachHistPart(c chan string, consolidatedOutputDir string, wg *sync.WaitGroup) {
	for file := range c {
		log.Println("Consolidating ", file)
		consolidatedMap := make(map[int64]*memInfoWithSlot)
		i := 0
		for line := range generateGzipLines(file) {
			i++
			m := &memInfoWithSlot{
				MemInfo: make([]meminfo.MemInfo, 0),
				Slot:    0,
			}
			json.Unmarshal(([]byte)(line), &m)
			_, present := consolidatedMap[m.Slot]
			if !present {
				consolidatedMap[m.Slot] = &memInfoWithSlot{
					MemInfo: make([]meminfo.MemInfo, 0),
					Slot:    m.Slot,
				}
			}
			consolidatedMap[m.Slot].MemInfo = append(consolidatedMap[m.Slot].MemInfo, m.MemInfo...)
		}
		log.Println("Consolidated ", i, " lines in file ", file)
		outputFilePath := path.Join(consolidatedOutputDir, path.Base(file))
		if len(consolidatedMap) > 0 {
			marshalObjectToJSONFile(outputFilePath, consolidatedMap)
		}
		os.Remove(file)
	}
	wg.Done()
}

func generateMemoryHistogram(rawDataPath, consolidatedOutputDir string) {
	histChan := processEachInstanceEntryInDir(rawDataPath, "instance_usage")
	outputDir := path.Join(rawDataPath, processedHistogramDir)
	os.MkdirAll(outputDir, 0755)
	generateHistParts(histChan, outputDir)

	c := iterateFilesInDir(outputDir, histogramPartPrefix)
	os.RemoveAll(consolidatedOutputDir)
	os.MkdirAll(consolidatedOutputDir, 0755)
	NumberOfConsolidators := multithreadingFactor * 2
	var wg sync.WaitGroup
	wg.Add(NumberOfConsolidators)
	log.Println("Beginning to consolidate...")
	for i := 0; i < NumberOfConsolidators; i++ {
		go consolidateEachHistPart(c, consolidatedOutputDir, &wg)
	}
	wg.Wait()
	err := os.Remove(outputDir)
	if err != nil {
		log.Panicln("Failed in consolidation", err)
	}
	log.Println("Finished consolidating parts")
}

type memDescriptor struct {
	Usage float32
	Max   float32
}
type totalMemoryUsage map[int64]*memDescriptor

func (tmu totalMemoryUsage) addToHist(histInfoParam *histInfo) {
	_, present := tmu[histInfoParam.s]
	if !present {
		tmu[histInfoParam.s] = &memDescriptor{
			Usage: 0,
			Max:   0,
		}
	}
	tmu[histInfoParam.s].Usage += histInfoParam.m.AvgUsing
	tmu[histInfoParam.s].Max += histInfoParam.m.MaxAvail
}

func generateTotalHistogram(histDir string, output string) {
	c := iterateFilesInDir(histDir, "")
	l := unmarshalObjectFiles(c)
	histEntries := emitMemInfo(l)
	supplyFunction := make(totalMemoryUsage)
	for entry := range histEntries {
		supplyFunction.addToHist(&entry)
	}
	marshalObjectToJSONFile(output, supplyFunction)
}

// This functions splits the machines into consumers and producers randomly. In reality there is no guarantee
// for a specfic consumer to be either consumer or producer based on its memory usage pattern - therefore we assume
// random assignment.
// It runs the simulation over multiple randomly partitioned consumers/producers.
// For each partitioning it creates a function G_s(x) so that G_s(x)=y if, under the partitioning
// Multiple "dimensions" are relevant to this optimization problem:
// 	1. How many servers serve each client?
// 	2. How are consumers and producers assigned to each other?
// 	3. How many evictions take place?
// In theory - we would be interested in matching consumers and producers with negative correlation in their
// memory consumption. In other words - match consumers and producers with very small vo
// Since we look at each consumer's remote memory as a cache - we can create an MRC for it.

// We match consumers with producers with a "Matching function" 'F' so that for a consumer 'c' and producer 'p' we have F(c) = p and F(p) = c. (F(F(x)) == x for all x).
// From the memcachier trace we have some typical MRCs that we can adjust and assign to each consumer.
// The adjustment is performed so that each MRC x-axis spans between 0 and the maximal usage of memory in the trace by that specific consumer.
// We denote this MRC <--> consumer assignment as function M_c(x) where M_c(x) is the adjusted-MRC function of client 'c'.
// We define a parameter "Lacc" as the acceptable local miss ratio, so that consumer 'c' will be assigned with local memory of Inv(M_c)(Lacc) and the rest of the memory will be used remotely.
// Inv(f) is the inverse function of a function f.

// Many-To-Many producer-consumer relationship.
// Producers are partitioned into groups of size 'Gp' and each producer in each group will serve the same number of 'Gc' consumers. Producers in the same group serve the same number of consumers.
// Similarly, consumers will be split into groups of size 'Gc'. Each consumer will be matched with 'Gp' producers from the same producer group.
// To load-balance, the consumer will distribute its memory demand between the different producers.

// For now we just assume there is a 1:1 ratio between consumers and producers.
// Let 'b' be a producer/consumer, we also denote by Tp(x) the memory usage of 'b' at time x.
// For each producer 'p' we assign memory of amount max(Tp).
// The process of simulation will be that we check how much of the currently needed memory of process is available.

// We wish to compute the *actual* miss ratio of each consumer, which is somewhere between Lacc and 0, this is the first purpose of the simulation. It's a percentage between 100 and the minimal attainable hit rate.
// Besides, we are interested in the stability of the remote memory availability in the average case. Or - for each remote block of memory allocated - how long does it remain usable?
// With these two we can estimate how much could be saved in the google cluster using the remote memory.

// Implementation observation -
// We can process each file independently and the merge the results.
// For each pair of processes matched - if the consumer terminates we wait for the next task to begin and consider it as a consumer and match it to the pending producer.
// If the producer is terminated we consider it as an eviction (obviously) and wait for the next producer to show up and match it to the consumer.
// Therefore, we first have to pass over all the histogram to determine the matching at each point in time.
// Next we can, in parallel, calculate the aforementioned data.

// Additional points to consider:
// 	1. Is it beneficial to match consumers with producers of roughly the same size of memory?
// 	2.

type match struct {
	Consumer TaskId.TaskId
	Producer TaskId.TaskId
}

// Things to do:
// 	* Global Preparations:
//		- Load the MRCs.
//		- Split tasks to producers and consumers.
//		- For each producer determine how much memory should be allocated to it. (== Maximal memory usage).
//		- For each consumer assign an MRC.
//		- For each consumer determine how much memory should be allocated to it locally. (== Based on Lacc).
// 	* Procedure of the simulation
//		- Initially we determine who are active tasks and their memory demand in each timeslot.
//		- Whenever a consumer exits, its remote memory is now available.

// IsGoingToBeConsumer this can be used to create smart decisions in the future.
func IsGoingToBeConsumer() chan bool {
	output := make(chan bool)
	go func() {
		for i := false; ; i = !i {
			output <- i
		}
	}()
	return output
}

// generateConsumerProducerMatching Returns (set_of_consumers, set_of_producers, matching function.
// The matching function states for each time slot and for each task - its pairing.

func generateConsumerProducerMatching(mrcPath, consolidatedHistogramDir string) *matchingFunction {
	mf := createEmpty()
	numberOfFiles := countChanString(iterateFilesInDir(consolidatedHistogramDir, histogramPartPrefix))
	for fileIdx := 0; fileIdx < numberOfFiles; fileIdx++ {
		fmt.Printf("\rReading slots from file : %d", k)
		fileName := histogramPartPrefix + strconv.Itoa(fileIdx) + ".json.gz"
		filePath := path.Join(consolidatedHistogramDir, fileName)
		fileHist := unmarshalHistogramFile(filePath)
		histKeys := make([]int64, 0)
		for k := range fileHist {
			histKeys = append(histKeys, k)
		}
		sort.Slice(histKeys, func(i, j int) bool { return histKeys[i] < histKeys[j] })
		for _, k := range histKeys {
			mf.nextSlotMatch(fileHist[k].MemInfo)
		}

	}
	fmt.Printf("\n")
	mf.setMrc(mrcPath)
	return mf
}

type (
	fullSimulationResultsStruct []simulationResultsStruct
	simulationResultsStruct     struct {
		RandomSeed          uint32
		SlotAnalyses        map[int64]slotUsageStatus
		AcceptableMissRatio float32
	}
)

func performSimulation(mrcsPath string, consolidatedHistogramDir string, fromAcceptableMissRatio, toAcceptableMissRatio, stepAcceptableMissRatio float32) *fullSimulationResultsStruct {
	mf := generateConsumerProducerMatching(mrcsPath, consolidatedHistogramDir)
	fullSimulationResults := fullSimulationResultsStruct(make([]simulationResultsStruct, 0))
	for acceptableMissRatio := fromAcceptableMissRatio; acceptableMissRatio < toAcceptableMissRatio; acceptableMissRatio += stepAcceptableMissRatio {
		mf.setAcceptableMissRatio(acceptableMissRatio)
		simulationResults := simulationResultsStruct{
			RandomSeed:          randomSeed,
			SlotAnalyses:        make(map[int64]slotUsageStatus),
			AcceptableMissRatio: acceptableMissRatio,
		}

		slots := make([]int, 0)
		for slot := range mf.Match {
			slots = append(slots, int(slot))
		}
		sort.Ints(slots)
		for slot := range slots {
			fmt.Printf("\rAnalyzing Slot: %d", slot)
			slotAnalysis := mf.analyzeSlot(int64(slot))
			simulationResults.SlotAnalyses[int64(slot)] = slotAnalysis
		}
		fmt.Printf("\n")
		fullSimulationResults = append(fullSimulationResults, simulationResults)
	}

	return &fullSimulationResults
}

func init() {
	log.SetLevel(log.TraceLevel)
	log.SetOutput(os.Stdout)
	log.SetReportCaller(true)
}

func main() {
	randomSeed = crc32.ChecksumIEEE([]byte("Columbia University"))
	multithreadingFactor = runtime.NumCPU()
	log.Println("Starting with ", multithreadingFactor, " processors")
	log.Println("Starting with ", randomSeed, "random seed")
	rand.Seed(int64(randomSeed))
	argsParsed := parseArgs()
	// Generate preliminary data.
	rawDataPath := *argsParsed.rawHistogramPath
	consolidatedOutputDir := path.Join(rawDataPath, consolidatedDir)
	fromAcceptableMissRatio := float32(*argsParsed.fromAcceptableMissRatio)
	toAcceptableMissRatio := float32(*argsParsed.toAcceptableMissRatio)
	stepAcceptableMissRatio := float32(*argsParsed.stepAcceptableMissRatio)
	simulationResultsOutputPath := *argsParsed.resultsOutputPath
	mrcPath := *argsParsed.mrcPath
	// generateMemoryHistogram(rawDataPath, consolidatedOutputDir)
	results := performSimulation(mrcPath, consolidatedOutputDir, float32(fromAcceptableMissRatio), float32(toAcceptableMissRatio), float32(stepAcceptableMissRatio))
	marshalSimulationResult(results, simulationResultsOutputPath)
	// Generate data to plot supply curve
	// case 2:
	// generateTotalHistogram(os.Args[2], os.Args[3])
}
