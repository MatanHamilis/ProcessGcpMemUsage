package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
	"sync"
)

func generateLinesFromReader(reader io.Reader, done chan bool) chan string {
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

func generateGzipLines(path string) chan string {
	f, err := os.Open(path)
	if err != nil {
		log.Panicln("Can't open Gzip file!", path, err.Error())
	}
	fileStat, err := os.Stat(path)
	log.Println("Unzipping file of size ", fileStat.Size())
	t, err := gzip.NewReader(f)
	if err != nil {
		log.Panicln("File isn't a Gzip file!", path, err.Error())
	}
	linesDone := make(chan bool)
	c := generateLinesFromReader(t, linesDone)
	go func() {
		<-linesDone
		close(linesDone)
		err := t.Close()
		if err != nil {
			panic("Failed to close")
		}

		err = f.Close()
		if err != nil {
			panic("Failed to close")
		}
	}()
	return c
}

func countChanString(ch chan string) int {
	res := 0
	for range ch {
		res++
	}
	return res
}

func iterateFilesInDir(p string, filter string) chan string {
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

func marshalObjectToJSONFile(path string, v interface{}) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		log.Panic("Failed to open file")
	}
	g := gzip.NewWriter(f)
	j := json.NewEncoder(g)
	j.SetIndent("", "\t")
	err = j.Encode(v)
	if err != nil {
		panic("Failed to encode object to json")
	}

	err = g.Close()
	if err != nil {
		panic("Failed to close gzip")
	}
	err = f.Close()
	if err != nil {
		panic("Failed to close file")
	}
}

func unmarshalObjectFiles(c chan string) chan *usageHistogram {
	var wg sync.WaitGroup
	numOfWorkers := multithreadingFactor * 2

	l := make(chan *usageHistogram)
	wg.Add(10)
	for i := 0; i < numOfWorkers; i++ {
		go func() {
			for p := range c {
				log.Println("Current file: ", p)
				v := make(usageHistogram)
				f, _ := os.Open(p)
				g, _ := gzip.NewReader(f)
				d := json.NewDecoder(g)
				d.Decode(&v)
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

func isDirectory(path string) bool {
	s, err := os.Stat(path)
	if err != nil {
		return false
	}
	return s.IsDir()
}

func isFileExists(path string) bool {
	_, err := os.Stat(path)
	return err != nil
}

func decodeMrcJSON(jsonFileChan io.Reader) map[int]*mrc {
	j := json.NewDecoder(jsonFileChan)
	rawMrc := make(map[string]mrc)
	mrc := make(map[int]*mrc)
	j.Decode(&rawMrc)
	i := 0
	for _, v := range rawMrc {
		mrc[i] = &v
		i++
	}
	return mrc
}

func safeFileOpen(path string) *os.File {
	file, err := os.Open(path)
	if err != nil {
		log.Panic("Failed to open file:", file, err)
	}
	return file
}

func unmarshalHistogramFile(path string) map[int64]*memInfoWithSlot {
	file := safeFileOpen(path)
	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		log.Panic("Failed to parse gzip format from histogram file:", path)
	}
	result := make(map[int64]*memInfoWithSlot)
	decoder := json.NewDecoder(gzipReader)
	decoder.Decode(&result)
	file.Close()
	gzipReader.Close()
	return result
}

func marshalSimulationResult(fullSimulationResults *fullSimulationResultsStruct, path string) {
	marshalObjectToJSONFile(path, fullSimulationResults)
}
