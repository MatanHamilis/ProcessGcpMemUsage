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
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
)

type InstanceUsage google_cluster_data.InstanceUsage

func ParseCollectionEvents(c chan google_cluster_data.CollectionEvent, s *bufio.Scanner) {
	var v google_cluster_data.CollectionEvent
	for s.Scan() != false {
		err := protojson.Unmarshal(s.Bytes(), &v)
		if err != nil {
			log.Println("Eror: ", err)
		}
		c <- v
	}
	close(c)
}

func GenerateLinesFromReader(reader io.Reader, done chan bool) chan string {
	c := make(chan string)
	go func() {
		scanner := bufio.NewScanner(reader)
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
		CollectionId: u.GetCollectionId(),
		InstanceId:   u.GetInstanceIndex(),
	}
	hp[slot] = append(v, memInfo)
}

func GenerateHistogramFromStream(c chan google_cluster_data.InstanceUsage) *UsageHistogram {
	m := make(UsageHistogram)
	i := 0
	for l := range c {
		m.add(&l)
		i++
	}
	return &m
}

func (into UsageHistogram) MergeHistograms(from UsageHistogram) {
	for from_key, from_slice := range from {
		_, present := into[from_key]
		if !present {
			into[from_key] = from_slice
		} else {
			into[from_key] = append(into[from_key], from_slice...)
		}
	}
}

type UsageHistogramKeyValuePair struct {
	hist     *UsageHistogram
	filepath string
}

func ProcessEachInstanceEntryInDir(dir string, filter string) chan *UsageHistogramKeyValuePair {
	histChan := make(chan *UsageHistogramKeyValuePair)
	go func() {
		c := IterateFilesInDir(dir, filter)

		const numOfReaders = 40
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
	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0755)
	g := gzip.NewWriter(f)
	j := json.NewEncoder(g)
	j.Encode(v)
	g.Close()
	f.Close()
}

func UnmarshalObjectFiles(c chan string) chan *UsageHistogram {
	l := make(chan *UsageHistogram)
	var wg sync.WaitGroup
	wg.Add(10)
	for i := 0; i < 10; i++ {
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
	wg.Add(10)
	for i := 0; i < 10; i++ {
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

func doWriter(ch chan *MemInfoWithSlot, slots_in_file int64, output_dir string, wg *sync.WaitGroup) {
	files := make(map[int64]*json.Encoder)
	underlying_files := make([]*gzip.Writer, 0)
	for m := range ch {
		file_id := m.Slot / slots_in_file
		w, present := files[file_id]
		if !present {
			file_path := path.Join(output_dir, "histogram_part_"+strconv.Itoa(int(file_id))+".json.gz")
			new_file, err := os.OpenFile(file_path, os.O_CREATE|os.O_RDWR, 0644)
			if err != nil {
				log.Panicln("Failed to create file. ", file_path, err)
			}
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

func SendMemInfoToWriters(histChan chan *UsageHistogramKeyValuePair, numberOfWriters int64, slotsPerSplit int64, writers []chan *MemInfoWithSlot, wg *sync.WaitGroup) {
	for h := range histChan {
		for k, v := range *h.hist {
			writer_id := k / slotsPerSplit % numberOfWriters
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
	const numberOfWriters = 10
	const wgSize = 10
	var wg sync.WaitGroup
	var writers_wg sync.WaitGroup
	wg.Add(wgSize)
	writers_wg.Add(numberOfWriters)
	writers := make([]chan *MemInfoWithSlot, numberOfWriters)

	// Writer i responsible for timeslots j s.t.:
	// (j/slotsPerSplit) % numberOfWriters == i
	for i := range writers {
		writers[i] = make(chan *MemInfoWithSlot)
	}
	for i := 0; i < numberOfWriters; i++ {
		go doWriter(writers[i], slotsPerSplit, output_dir, &writers_wg)
	}
	for i := 0; i < wgSize; i++ {
		go SendMemInfoToWriters(histChan, numberOfWriters, slotsPerSplit, writers, &wg)
	}
	wg.Wait()
	for i := range writers {
		close(writers[i])
	}
	writers_wg.Wait()
}

func GenerateMemoryHistogram(raw_data_path string) {
	histChan := ProcessEachInstanceEntryInDir(raw_data_path, "instance_usage")
	output_dir := path.Join(raw_data_path, "processed_hist_part")
	os.MkdirAll(output_dir, 0755)
	GenerateHistParts(histChan, output_dir)
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

func SimulateConsumersProducersByPercentage(percentage int, histogramDir string) {
}
func main() {
	option, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Panicln("Failed to parse cmdline argument of option", os.Args[1], err)
	}
	switch option {
	// Generate preliminary data.
	case 1:
		GenerateMemoryHistogram(os.Args[2])
	// Generate data to plot supply curve
	case 2:
		GenerateTotalHistogram(os.Args[2], os.Args[3])
	}

}
