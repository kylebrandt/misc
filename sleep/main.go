package main

//2014 KMB

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/csv"
	"flag"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/StackExchange/scollector/opentsdb"
	"github.com/StackExchange/slog"
)

var (
	name = flag.String("name", "", "Your name, use AD form, ie kbrandt. Required")
	team = flag.String("team", "", "Your team, use AD form, ie (Required)")
	host = flag.String("host", "http://bosun:4242/api/put", "host ot send data to")
	file = flag.String("file", "", "CSV file of Sleep As Android Data")
)

//Format
//Id,Tz,From,To,Sched,Hours,Rating,Comment,Framerate,Snore,Noise,Cycles,DeepSleep,LenAdjust,Geo, ...
func parse(b bytes.Buffer, md *opentsdb.MultiDataPoint) {
	m := make(map[string]string)
	r := csv.NewReader(&b)
	lines, err := r.ReadAll()
	if err != nil {
		panic(err)
	}
	for i, f := range lines[0] {
		m[f] = lines[1][i]
	}
	l, err := time.LoadLocation(m["Tz"])
	if err != nil {
		panic(err)
	}
	t, err := time.ParseInLocation("02. 01. 2006 15:04", m["From"], l)
	if err != nil {
		panic(err)
	}
	*md = append(*md, &opentsdb.DataPoint{
		Metric:    "employee.sleep.hours",
		Timestamp: t.Unix(),
		Value:     m["Hours"],
		Tags:      opentsdb.TagSet{"name": "kbrandt", "team": "sre"},
	})
	*md = append(*md, &opentsdb.DataPoint{
		Metric:    "employee.sleep.deep_percent",
		Timestamp: t.Unix(),
		Value:     m["DeepSleep"],
		Tags:      opentsdb.TagSet{"name": "kbrandt", "team": "sre"},
	})

}

func send(batch opentsdb.MultiDataPoint, host string) {
	var client http.Client
	b, err := batch.Json()
	if err != nil {
		slog.Error(err)
		// bad JSON encoding, just give up
		return
	}
	var buf bytes.Buffer
	g := gzip.NewWriter(&buf)
	if _, err = g.Write(b); err != nil {
		slog.Error(err)
		return
	}
	if err = g.Close(); err != nil {
		slog.Error(err)
		return
	}
	req, err := http.NewRequest("POST", host, &buf)
	if err != nil {
		slog.Error(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")
	_, err = client.Do(req)
	if err != nil {
		panic(err)
	}
}

func check(name string, s *string) {
	if *s == "" {
		log.Println("Argument", name, "is required")
		os.Exit(1)
	}
}

func main() {
	flag.Parse()
	check("name", name)
	check("file", file)
	check("team", team)
	f, err := os.Open(*file)
	if err != nil {
		log.Println(err)
	}
	scanner := bufio.NewScanner(f)
	var b bytes.Buffer
	ln := 0
	var md opentsdb.MultiDataPoint
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) > 3 && line[0:2] == "Id" {
			if ln != 0 {
				parse(b, &md)
			}
			b.Reset()
			b.WriteString(line + "\n")
			scanner.Scan()
			b.WriteString(scanner.Text())
		}
		ln += 1
	}
	send(md, *host)
}
