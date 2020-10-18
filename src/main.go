package main

import (
	"bufio"
	"compress/gzip"
	"container/list"
	"github.com/golang/protobuf/jsonpb"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
)

func ParseCollectionEvents(c chan CollectionEvent, s *bufio.Scanner) {
	var v CollectionEvent
	for s.Scan() != false {
		jsonpb.UnmarshalString(s.Text(), &v)
		c <- v
	}
	close(c)
}

func ParseInstanceUsage(c chan InstanceUsage, s *bufio.Scanner) {
	var v InstanceUsage
	i := 0
	for s.Scan() != false {
		jsonpb.UnmarshalString(s.Text(), &v)
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

func GenerateHistogramFromStream(c chan InstanceUsage, end chan bool) map[uint64][]uint64 {
	m := make(map[uint64][]uint64)
	for l := range c {
		log.Println(*l.StartTime)
	}
	end <- true
	return m
}

func ProcessEachInstanceEntryInDir(dir string, filter string) {
	c := make(chan string)
	go IterateFilesInDir(dir, filter, c)
	lst := list.New()
	for f := range c {
		evtChan := make(chan InstanceUsage)
		endChan := make(chan bool)
		lst.PushBack(endChan)
		go ParseInstanceUsage(evtChan, GenerateGzipScanner(f))
		go GenerateHistogramFromStream(evtChan, endChan)
	}

	for ch := lst.Front(); ch != nil; ch = ch.Next() {
		realChan, _ := ch.Value.(chan bool)
		<-realChan
	}

}
func main() {
	ProcessEachInstanceEntryInDir("../data/clusterdata_2019_a/", "instance_usage")
}
