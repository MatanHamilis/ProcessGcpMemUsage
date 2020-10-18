package main

import (
	"bufio"
	"compress/gzip"
	"github.com/golang/protobuf/jsonpb"
	"log"
	"os"
)

func ParseCollectionEvents(c chan CollectionEvent, s *bufio.Scanner) {
	var v CollectionEvent
	for s.Scan() != false {
		jsonpb.UnmarshalString(s.Text(), &v)
		c <- v
	}
	close(c)
}

func ParseInstanceEvents(c chan InstanceEvent, s *bufio.Scanner) {
	var v InstanceEvent
	for s.Scan() != false {
		jsonpb.UnmarshalString(s.Text(), &v)
	}
	close(c)
}

func main() {
	f, _ := os.Open("../stam.json.gz")
	t, _ := gzip.NewReader(f)
	b := bufio.NewScanner(t)
	c := make(chan CollectionEvent)
	go ParseCollectionEvents(c, b)
	for e := range c {
		log.Println(*e.Time)
	}
}
