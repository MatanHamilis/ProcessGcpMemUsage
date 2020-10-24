package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"google.golang.org/protobuf/encoding/protojson"
	"google_cluster_project/google_cluster_data"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
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

func ParseInstanceUsage(c chan google_cluster_data.InstanceUsage, s *bufio.Scanner) {
	var v google_cluster_data.InstanceUsage
	i := 0
	for s.Scan() != false {
		t := strings.Replace(s.Text(), "collection_type\":\"0\"", "collection_type\":0", 1)
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
		c <- v
		i++
	}
	close(c)
}

func IterateFilesInDir(p string, filter string, c chan string) {
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
}

func GenerateGzipScanner(path string) *bufio.Scanner {
	f, _ := os.Open(path)
	t, _ := gzip.NewReader(f)
	return bufio.NewScanner(t)
}

type UsageHistogram struct {
	H map[int64][]MemInfo
}

type MemInfo struct {
	CollectionId int64
	InstanceId   int32
	AvgUsing     float32
	MaxAvail     float32
}

func (hp *UsageHistogram) add(u *google_cluster_data.InstanceUsage) {
	const slotSize = 300 * 1000000
	slot := *u.StartTime / slotSize
	v, exists := hp.H[slot]
	if !exists {
		hp.H[slot] = make([]MemInfo, 0, 5)
		v = hp.H[slot]
	}
	memInfo := MemInfo{
		AvgUsing:     u.AverageUsage.GetMemory(),
		MaxAvail:     u.GetAssignedMemory(),
		CollectionId: u.GetCollectionId(),
		InstanceId:   u.GetInstanceIndex(),
	}
	hp.H[slot] = append(v, memInfo)
}

func GenerateHistogramFromStream(c chan google_cluster_data.InstanceUsage, outChan chan *UsageHistogram) {
	m := &UsageHistogram{
		H: make(map[int64][]MemInfo),
	}
	i := 0
	for l := range c {
		m.add(&l)
		i++
	}
	outChan <- m
}

func (into *UsageHistogram) MergeHistograms(from *UsageHistogram) {
	for from_key, from_slice := range from.H {
		_, present := into.H[from_key]
		if !present {
			into.H[from_key] = from_slice
		} else {
			into.H[from_key] = append(into.H[from_key], from_slice...)
		}
	}
}

func ProcessEachInstanceEntryInDir(dir string, filter string) *UsageHistogram {
	c := make(chan string)
	go IterateFilesInDir(dir, filter, c)
	outputChan := make(chan *UsageHistogram)
	i := 0
	for f := range c {
		i++
		log.Println("Found File: ", f)
		evtChan := make(chan google_cluster_data.InstanceUsage)
		go ParseInstanceUsage(evtChan, GenerateGzipScanner(f))
		go GenerateHistogramFromStream(evtChan, outputChan)
	}

	var totalHistogram *UsageHistogram
	totalHistogram = <-outputChan

	for ; i > 1; i-- {
		totalHistogram.MergeHistograms(<-outputChan)
	}
	return totalHistogram
}

func marshalObjectToJsonFile(path string, v interface{}) {
	log.Println(v)
	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0755)
	g := gzip.NewWriter(f)
	j := json.NewEncoder(f)
	j.SetIndent("", "    ")
	j.Encode(v)
	g.Close()
	f.Close()
}
func main() {
	f := ProcessEachInstanceEntryInDir(os.Args[1], "instance_usage")
	marshalObjectToJsonFile("usage_histogram.json.gz", *f)
}
