package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	argv struct {
		help bool
		drop bool

		srv string
		maxConn int
		maxConcurrency int

		tableColumns string
		dataSize int
	}

	statInserts int64
	statBytesInserted int64
	shouldQuit int32
)

const createClickHouseStress = "CREATE TABLE IF NOT EXISTS clickhouse_stress (`date` Date DEFAULT toDate(time), `time` DateTime, `key0` Int32, `key1` Int32, `key2` Int32, `key3` Int32) ENGINE = MergeTree() ORDER BY (date, time, key0)"
const createClickHouseStressBuffer = "CREATE TABLE IF NOT EXISTS clickhouse_stress_buffer AS clickhouse_stress ENGINE = Buffer('default', 'clickhouse_stress', 16, 10, 100, 10000, 1000000, 10000000, 100000000)"
const optimizeClickHouseStressBuffer = "OPTIMIZE TABLE clickhouse_stress_buffer"
const clickHouseStressTableColumns = "clickhouse_stress_buffer(time,key0,key1,key2,key3)"
const dropClickHouseStressBuffer = "DROP TABLE clickhouse_stress_buffer"
const dropClickHouseStress = "DROP TABLE clickhouse_stress"

func init() {

	// actions
	flag.BoolVar(&argv.help, `h`, false, `show this help`)
	flag.BoolVar(&argv.drop, `drop`, false, `drop clickhouse_stress and clickhouse_stress_buffer tables`)

	flag.StringVar(&argv.srv, `srv`, "127.0.0.1:8123", "Clickhouse host:part")
	flag.IntVar(&argv.maxConn, `max-conn`, 20, `Max number of connections to clickhouse (more than 100 is not recommended)`)
	flag.IntVar(&argv.maxConcurrency, `max-concurrency`, 0, `Number of parallel goroutines to use, 0 is use 2x max-conn)`)
	//flag.StringVar(&argv.tableColumns, `table`, "clickhouse_stress_buffer(time,key0,key1,key2,key3)", "Table name with columns to insert")
	flag.IntVar(&argv.dataSize, `data-size`, 20000, "Random bytes count to insert (must be multiple of row binary size, 20 for default table)")

	flag.Parse()

	if argv.maxConn < 1 {
		log.Panicf("max-conn must be at least 1")
	}
	if argv.maxConcurrency == 0 {
		argv.maxConcurrency = argv.maxConn
	}
	if argv.maxConcurrency < 1 {
		log.Panicf("max-concurrency must be at least 1")
	}

	if argv.dataSize < 1 {
		log.Panicf("data-size must be at least 1")
	}
}

func Flush(client *http.Client, query string, body []byte) error {
	queryPrefix := url.PathEscape(query)

	reqUrl := fmt.Sprintf("http://%s/?input_format_values_interpret_expressions=0&query=%s", argv.srv, queryPrefix)

	// TODO - why application/x-www-form-urlencoded, this is wrong data format for a form, will break pedantic proxies
	resp, err := client.Post(reqUrl, "application/x-www-form-urlencoded", bytes.NewReader(body))
	if err != nil {
		log.Printf("Could not post query %s to clickhouse: %s", query, err.Error())
		return err
	}

	defer resp.Body.Close()
	var partialMessage [1024]byte
	partialMessageLen, _ := resp.Body.Read(partialMessage[:])
	_, _ = io.Copy(ioutil.Discard, resp.Body) // keepalive

	if resp.StatusCode != http.StatusOK {
		clickhouseExceptionText := resp.Header.Get("X-ClickHouse-Exception-Code")
		log.Printf("Could not post query %s to clickhouse (HTTP code %d, X-ClickHouse-Exception-Code: %s): %s", query, resp.StatusCode, clickhouseExceptionText, partialMessage[0:partialMessageLen])
		return fmt.Errorf("Could not post query %s to clickhouse (HTTP code %d, X-ClickHouse-Exception-Code: %s): %s", query, resp.StatusCode, clickhouseExceptionText, partialMessage[0:partialMessageLen])
	}
	return nil
}

func GoPrintStats(wg *sync.WaitGroup) {
	defer wg.Done()
	for ; atomic.LoadInt32(&shouldQuit) == 0; {
		time.Sleep(time.Second)
		log.Printf("stats: inserts %d, inserted %.6f MB", atomic.LoadInt64(&statInserts), float64(atomic.LoadInt64(&statBytesInserted)) / 1024 / 1024)
 	}
}

func GoOverLoadClickHouse(client *http.Client, tableColumns string, dataSize int, wg *sync.WaitGroup) {
	defer wg.Done()
	counter := 0
	for ;atomic.LoadInt32(&shouldQuit) == 0; counter += 1 {
		body := make([]byte, dataSize)
		_, _ = rand.Read(body[:])
		err := Flush(client, fmt.Sprintf("INSERT INTO %s FORMAT RowBinary", tableColumns), body[:])
		if err != nil {
			log.Printf("Error inserting counter=%d, %v", counter, err)
			time.Sleep(1 * time.Second)
			counter = 0
			continue
		}
		atomic.AddInt64(&statInserts,1)
		atomic.AddInt64(&statBytesInserted,int64(dataSize))
	}
}

func OverLoadClickHouse(client *http.Client) {
	var wg sync.WaitGroup
	wg.Add(1)
	go GoPrintStats(&wg)
	for i := 0; i < argv.maxConcurrency; i += 1 {
		wg.Add(1)
		go GoOverLoadClickHouse(client, clickHouseStressTableColumns, argv.dataSize, &wg)
	}
	chSignal := make(chan os.Signal, 1)
	signal.Notify(chSignal, syscall.SIGINT)

loop:
	for {
		sig := <-chSignal
		switch sig {
		case syscall.SIGINT:
			break loop
		}
	}
	atomic.StoreInt32(&shouldQuit, 1)
	log.Printf("Waiting for all goroutines to quit")
	wg.Wait()
}

func main() {
	if argv.help {
		flag.Usage()
		return
	}
	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: argv.maxConn,
			MaxConnsPerHost:     argv.maxConn,
		},
	}
	if argv.drop {
		err := Flush(client, dropClickHouseStressBuffer, nil)
		if err != nil {
			log.Printf("%v", err)
		}
		err = Flush(client, dropClickHouseStress, nil)
		if err != nil {
			log.Printf("%v", err)
		}
		log.Printf("Dropped tables for testing")
		return;
	}
	err := Flush(client, createClickHouseStress, nil)
	if err != nil {
		log.Printf("%v", err)
	}
	err = Flush(client, createClickHouseStressBuffer, nil)
	if err != nil {
		log.Printf("%v", err)
	}
	err = Flush(client, optimizeClickHouseStressBuffer, nil)
	if err != nil {
		log.Printf("%v", err)
	}
	OverLoadClickHouse(client)
}
