package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"google.golang.org/protobuf/encoding/protojson"
	"google_cluster_project/google_cluster_data"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

type InstanceUsage google_cluster_data.InstanceUsage

const PROCESSED_HISTOGRAM_DIR = "processed_hist_part"
const CONSOLIDATED_DIR = "consolidated_histogram_dir"
const HISTOGRAM_PART_PREFIX = "histogram_part_"
const CONSOLIDATED_HISTOGRAM_PART_PREFIX = "consolidated_part_"

var MULTITHREADING_FACTOR int

func GenerateLinesFromReader(reader io.Reader, done chan bool) chan string {
	c := make(chan string)
	cbuffer := make([]byte, 0, bufio.MaxScanTokenSize*100)
	go func() {
		scanner := bufio.NewScanner(reader)
		scanner.Buffer(cbuffer, bufio.MaxScanTokenSize*100)
		for scanner.Scan() != false {
			c <- scanner.Text()
		}
		close(c)
		if done != nil {
			done <- true
		}
	}()
	return c
}

func GenerateGzipLines(path string) chan string {
	f, err := os.Open(path)
	if err != nil {
		log.Panicln("Can't open Gzip file!", path, err.Error())
	}
	file_stat, err := os.Stat(path)
	log.Println("Unzipping file of size ", file_stat.Size())
	t, err := gzip.NewReader(f)
	if err != nil {
		log.Panicln("File isn't a Gzip file!", path, err.Error())
	}
	lines_done := make(chan bool)
	c := GenerateLinesFromReader(t, lines_done)
	go func() {
		<-lines_done
		close(lines_done)
		t.Close()
		f.Close()
	}()
	return c
}

func GenerateInstanceUsageFromLines(lines chan string) chan google_cluster_data.InstanceUsage {
	var v google_cluster_data.InstanceUsage
	instanceUsageChan := make(chan google_cluster_data.InstanceUsage)
	go func() {
		for line := range lines {
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
				log.Println("Start Time Nil: ", v)
			}
			if v.GetStartTime() == 0 || v.GetEndTime() == math.MaxInt64 {
				continue
			}
			if v.GetAssignedMemory() == 0 {
				continue
			}
			instanceUsageChan <- v
		}
		close(instanceUsageChan)
	}()

	return instanceUsageChan
}

func ParseInstanceUsage(histChan chan *UsageHistogramKeyValuePair, f chan string, wg *sync.WaitGroup) {
	for file := range f {
		log.Println("Processing file: ", file)
		hist := GenerateHistogramFromStream(GenerateInstanceUsageFromLines(GenerateGzipLines(file)))
		pair := &UsageHistogramKeyValuePair{
			hist:     hist,
			filepath: file,
		}
		histChan <- pair
	}
	wg.Done()
}

func IterateFilesInDir(p string, filter string) chan string {
	c := make(chan string)
	go func() {
		d, _ := ioutil.ReadDir(p)
		for _, f := range d {
			if f.IsDir() {
				continue
			}
			if !strings.Contains(f.Name(), filter) {
				continue
			}
			c <- path.Join(p, f.Name())
		}
		close(c)
	}()
	return c
}

type UsageHistogram map[int64][]MemInfo

type MemInfo struct {
	CollectionId int64
	InstanceId   int32
	AvgUsing     float32
	MaxUsage     float32
	MaxAvail     float32
}

func (hp UsageHistogram) add(u *google_cluster_data.InstanceUsage) {
	const slotSize = 300 * 1000000
	slot := *u.StartTime / slotSize
	v, exists := hp[slot]
	if !exists {
		hp[slot] = make([]MemInfo, 0, 5)
		v = hp[slot]
	}
	memInfo := MemInfo{
		AvgUsing:     u.AverageUsage.GetMemory(),
		MaxAvail:     u.GetAssignedMemory(),
		MaxUsage:     u.MaximumUsage.GetMemory(),
		CollectionId: u.GetCollectionId(),
		InstanceId:   u.GetInstanceIndex(),
	}
	hp[slot] = append(v, memInfo)
}

func GenerateHistogramFromStream(c chan google_cluster_data.InstanceUsage) *UsageHistogram {
	m := make(UsageHistogram)
	for l := range c {
		m.add(&l)
	}
	return &m
}

type UsageHistogramKeyValuePair struct {
	hist     *UsageHistogram
	filepath string
}

func ProcessEachInstanceEntryInDir(dir string, filter string) chan *UsageHistogramKeyValuePair {
	histChan := make(chan *UsageHistogramKeyValuePair)
	go func() {
		c := IterateFilesInDir(dir, filter)

		var numOfReaders = MULTITHREADING_FACTOR * 2
		log.Println("Starting ", numOfReaders, " readers")
		var instanceUsageWg sync.WaitGroup
		instanceUsageWg.Add(numOfReaders)
		for i := 0; i < numOfReaders; i++ {
			go ParseInstanceUsage(histChan, c, &instanceUsageWg)
		}
		instanceUsageWg.Wait()
		close(histChan)
	}()

	return histChan
}

func marshalObjectToJsonFile(path string, v interface{}) {
	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	g := gzip.NewWriter(f)
	j := json.NewEncoder(g)
	j.Encode(v)
	g.Close()
	f.Close()
}

func UnmarshalObjectFiles(c chan string) chan *UsageHistogram {
	var wg sync.WaitGroup
	var numOfWorkers = MULTITHREADING_FACTOR * 2

	l := make(chan *UsageHistogram)
	wg.Add(10)
	for i := 0; i < numOfWorkers; i++ {
		go func() {
			for p := range c {
				log.Println("Current file: ", p)
				v := make(UsageHistogram)
				f, _ := os.Open(p)
				g, _ := gzip.NewReader(f)
				d := json.NewDecoder(g)
				d.Decode(v)
				l <- &v
				g.Close()
				f.Close()

			}
			wg.Done()
		}()
	}
	go func() {
		wg.Wait()
		close(l)
	}()
	return l
}

type HistInfo struct {
	m MemInfo
	s int64
}

func EmitMemInfo(uc chan *UsageHistogram) chan HistInfo {
	h := make(chan HistInfo)

	var wg sync.WaitGroup
	var numOfWorkers = MULTITHREADING_FACTOR * 2
	wg.Add(numOfWorkers)
	for i := 0; i < numOfWorkers; i++ {
		go func() {
			for u := range uc {
				for slot, slot_map := range *u {
					for _, mem_info := range slot_map {
						h <- HistInfo{
							m: mem_info,
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

type MemInfoWithSlot struct {
	MemInfo []MemInfo
	Slot    int64
}

func splitHistogram(splitSize int64, hist *UsageHistogram, output_channels []chan *MemInfoWithSlot) {
	modulus := int64(len(output_channels))
	for k, v := range *hist {
		chan_id := (k / splitSize) % modulus
		output_channels[chan_id] <- &MemInfoWithSlot{
			MemInfo: v,
			Slot:    k,
		}
	}
}

func doWriter(ch chan *MemInfoWithSlot, output_dir string, wg *sync.WaitGroup, writer_id int64) {
	files := make(map[int64]*json.Encoder)
	underlying_files := make([]*gzip.Writer, 0)
	for m := range ch {
		file_id := writer_id
		w, present := files[file_id]
		if !present {
			file_path := path.Join(output_dir, HISTOGRAM_PART_PREFIX+strconv.Itoa(int(file_id))+".json.gz")
			new_file, err := os.OpenFile(file_path, os.O_CREATE|os.O_RDWR, 0644)
			if err != nil {
				log.Panicln("Failed to create file. ", file_path, err)
			}
			log.Println("WRITER, created new file", file_path)
			gzip_writer := gzip.NewWriter(new_file)
			files[file_id] = json.NewEncoder(gzip_writer)
			underlying_files = append(underlying_files, gzip_writer)
			w = files[file_id]
		}
		w.Encode(m)
	}
	for i := range underlying_files {
		underlying_files[i].Close()
	}
	wg.Done()
}

func SendMemInfoToWriters(histChan chan *UsageHistogramKeyValuePair, numberOfWriters int64, writers []chan *MemInfoWithSlot, wg *sync.WaitGroup, slotsPerSplit int64) {
	var writer_id int64
	for h := range histChan {
		for k, v := range *h.hist {
			writer_id = (k / slotsPerSplit) % numberOfWriters
			writers[writer_id] <- &MemInfoWithSlot{
				MemInfo: v,
				Slot:    k,
			}

		}
	}
	wg.Done()
}

func GenerateHistParts(histChan chan *UsageHistogramKeyValuePair, output_dir string) {
	const slotsPerSplit = 100
	const wgSize = 10
	numberOfWriters := int64(MULTITHREADING_FACTOR * 2)
	var wg sync.WaitGroup
	var writers_wg sync.WaitGroup
	wg.Add(wgSize)
	writers_wg.Add(int(numberOfWriters))
	writers := make([]chan *MemInfoWithSlot, numberOfWriters)

	// Writer i responsible for timeslots j s.t.:
	// ((j/slotsPerSplit) % numOfWriters) == i
	for i := range writers {
		writers[i] = make(chan *MemInfoWithSlot)
	}
	for i := int64(0); i < numberOfWriters; i++ {
		go doWriter(writers[i], output_dir, &writers_wg, i)
	}
	for i := 0; i < wgSize; i++ {
		go SendMemInfoToWriters(histChan, numberOfWriters, writers, &wg, slotsPerSplit)
	}
	wg.Wait()
	for i := range writers {
		close(writers[i])
	}
	writers_wg.Wait()
}

func ConsolidateEachHistPart(c chan string, consolidated_output_dir string, wg *sync.WaitGroup) {
	for file := range c {
		log.Println("Consolidating ", file)
		consolidated_map := make(map[int64]*MemInfoWithSlot)
		var m MemInfoWithSlot
		i := 0
		for line := range GenerateGzipLines(file) {
			i++
			json.Unmarshal(([]byte)(line), &m)
			_, present := consolidated_map[m.Slot]
			if !present {
				consolidated_map[m.Slot] = &MemInfoWithSlot{
					MemInfo: make([]MemInfo, 0),
					Slot:    m.Slot,
				}
			}
			consolidated_map[m.Slot].MemInfo = append(consolidated_map[m.Slot].MemInfo, m.MemInfo...)
		}
		log.Println("Consolidated ", i, " lines in file ", file)
		output_file_path := path.Join(consolidated_output_dir, path.Base(file))
		marshalObjectToJsonFile(output_file_path, consolidated_map)
		os.Remove(file)
	}
	wg.Done()
}

func GenerateMemoryHistogram(raw_data_path string) {
	histChan := ProcessEachInstanceEntryInDir(raw_data_path, "instance_usage")
	output_dir := path.Join(raw_data_path, PROCESSED_HISTOGRAM_DIR)
	consolidated_output_dir := path.Join(raw_data_path, CONSOLIDATED_DIR)
	os.MkdirAll(output_dir, 0755)
	GenerateHistParts(histChan, output_dir)

	c := IterateFilesInDir(output_dir, HISTOGRAM_PART_PREFIX)
	os.MkdirAll(consolidated_output_dir, 0755)
	var NUMBER_OF_CONSOLIDATORS = MULTITHREADING_FACTOR * 2
	var wg sync.WaitGroup
	wg.Add(NUMBER_OF_CONSOLIDATORS)
	log.Println("Beginning to consolidate...")
	for i := 0; i < NUMBER_OF_CONSOLIDATORS; i++ {
		go ConsolidateEachHistPart(c, consolidated_output_dir, &wg)
	}
	wg.Wait()
	err := os.Remove(output_dir)
	if err != nil {
		log.Panicln("Failed in consolidation", err)
	}
	log.Println("Finished consolidating parts")
}

type MemDescriptor struct {
	Usage float32
	Max   float32
}
type TotalMemoryUsage map[int64]*MemDescriptor

func (tmu TotalMemoryUsage) addToHist(hist_info *HistInfo) {
	_, present := tmu[hist_info.s]
	if !present {
		tmu[hist_info.s] = &MemDescriptor{
			Usage: 0,
			Max:   0,
		}
	}
	tmu[hist_info.s].Usage += hist_info.m.AvgUsing
	tmu[hist_info.s].Max += hist_info.m.MaxAvail
}

func GenerateTotalHistogram(histDir string, output string) {
	c := IterateFilesInDir(histDir, "")
	l := UnmarshalObjectFiles(c)
	hist_entries := EmitMemInfo(l)
	supply_function := make(TotalMemoryUsage)
	for entry := range hist_entries {
		supply_function.addToHist(&entry)
	}
	marshalObjectToJsonFile(output, supply_function)
}

func IsDirectory(path string) bool {
	s, err := os.Stat(path)
	if err != nil {
		return false
	}
	return s.IsDir()
}

func IsFileExists(path string) bool {
	_, err := os.Stat(path)
	return err != nil
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

type TaskId struct {
	CollectionId int64
	InstanceId   int32
}

type Match struct {
	Consumer TaskId
	Producer TaskId
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

// func GeneratePairing(hist_dir string) map[uint][]Match {
// }

func main() {
	option, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Panicln("Failed to parse cmdline argument of option", os.Args[1], err)
	}
	MULTITHREADING_FACTOR = runtime.NumCPU()
	log.Println("Starting with ", MULTITHREADING_FACTOR, " processors")
	switch option {
	// Generate preliminary data.
	case 1:
		GenerateMemoryHistogram(os.Args[2])
	// Generate data to plot supply curve
	case 2:
		GenerateTotalHistogram(os.Args[2], os.Args[3])
	}

}
