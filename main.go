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
	"github.com/gorilla/websocket"
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
		randomDataSize bool
		webSocket bool
	}

	statInserts int64
	statBytesInserted int64
	shouldQuit int32
)

const createClickHouseStress = "CREATE TABLE IF NOT EXISTS clickhouse_stress (`date` Date DEFAULT toDate(time), `time` DateTime, `key0` Int32, `key1` Int32, `key2` Int32, `key3` Int32) ENGINE = MergeTree() ORDER BY (date, time, key0, key1)"
const createClickHouseStressBuffer = "CREATE TABLE IF NOT EXISTS clickhouse_stress_buffer AS clickhouse_stress ENGINE = Buffer('default', 'clickhouse_stress', 16, 10, 100, 10000, 1000000, 10000000, 100000000)"
const optimizeClickHouseStressBuffer = "OPTIMIZE TABLE clickhouse_stress_buffer"
const clickHouseStressTableColumns = "clickhouse_stress_buffer(time,key0,key1,key2,key3)"
const dropClickHouseStressBuffer = "DROP TABLE clickhouse_stress_buffer"
const dropClickHouseStress = "DROP TABLE clickhouse_stress"

func parseArgv() {

	// actions
	flag.BoolVar(&argv.help, `h`, false, `show this help`)
	flag.BoolVar(&argv.drop, `drop`, false, `drop clickhouse_stress and clickhouse_stress_buffer tables`)

	flag.StringVar(&argv.srv, `srv`, "127.0.0.1:8123", "Clickhouse host:part")
	flag.IntVar(&argv.maxConn, `max-conn`, 50, `Max number of connections to clickhouse (more than 100 is not recommended)`)
	flag.IntVar(&argv.maxConcurrency, `max-concurrency`, 0, `Number of parallel goroutines to use, 0 is use 2x max-conn)`)
	flag.StringVar(&argv.tableColumns, `table`, clickHouseStressTableColumns, "Table name with columns to insert into. You must create tables manually if this is non-default.")
	flag.IntVar(&argv.dataSize, `data-size`, 200000, "Random bytes count to insert (must be multiple of row binary size, 20 for default table)")
	flag.BoolVar(&argv.randomDataSize, `random-data-size`, false, "Randomly select bytes count to insert, from 0 to -data-size bytes")
	flag.BoolVar(&argv.webSocket, `ws`, false, "Use /meow web socket extension")

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

	if argv.dataSize < 0 {
		log.Panicf("data-size must be at least 0")
	}
}

func Flush(client *http.Client, query string, body []byte) error {
	queryPrefix := url.PathEscape(query)

	reqUrl := fmt.Sprintf("http://%s/?input_format_values_interpret_expressions=0&query=%s", argv.srv, queryPrefix)

	log.Printf("Sending %d bytes to clickhouse", len(body))

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

func GoOverLoadClickHouse(client *http.Client, tableColumns string, dataSize int, randomDataSize bool, wg *sync.WaitGroup) {
	defer wg.Done()
	counter := 0
	query := fmt.Sprintf("INSERT INTO %s FORMAT RowBinary", tableColumns)
	wsURL := []byte("/?input_format_values_interpret_expressions=0&query=" + url.PathEscape(query))

	var (
		ws *websocket.Conn
		err error
	)
	if argv.webSocket {
		ws, _, err = websocket.DefaultDialer.Dial(fmt.Sprintf("ws://%s/meow", argv.srv), nil)
		if err != nil {
			log.Fatal("dial:", err)
		}
		defer ws.Close()
		go func(){
			for {
				_, _, err = ws.ReadMessage()
				if err != nil {
					break
				}
			}
		}()
	}

	for ;atomic.LoadInt32(&shouldQuit) == 0; counter += 1 {
		var body []byte
		if dataSize > 0 {
			ds := dataSize
			if randomDataSize {
				ds = 20 + 20*rand.Intn(dataSize/20)
			}
			body = make([]byte, ds)
			_, _ = rand.Read(body[:])
		}
		if ws != nil {
			err = ws.WriteMessage(websocket.BinaryMessage, wsURL)
			if err != nil {
				log.Printf("Error inserting into WebSocket counter=%d, %v", counter, err)
				break;
			}
			err = ws.WriteMessage(websocket.BinaryMessage, body)
			if err != nil {
				log.Printf("Error inserting into WebSocket counter=%d, %v", counter, err)
				break;
			}
		}else{
			err = Flush(client, query, body[:])
			if err != nil {
				log.Printf("Error inserting counter=%d, %v", counter, err)
				time.Sleep(1 * time.Second)
				counter = 0
				continue
			}
		}
		atomic.AddInt64(&statInserts,1)
		atomic.AddInt64(&statBytesInserted,int64(dataSize))
	}
	if ws != nil {
		err = ws.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	}
	if err != nil {
		log.Printf("Could not write close message, %v", err)
	}
}

func OverLoadClickHouse(client *http.Client) {
	var wg sync.WaitGroup
	wg.Add(1)
	go GoPrintStats(&wg)
	for i := 0; i < argv.maxConcurrency; i += 1 {
		wg.Add(1)
		go GoOverLoadClickHouse(client, argv.tableColumns, argv.dataSize, argv.randomDataSize, &wg)
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
	parseArgv()
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
	if argv.tableColumns == clickHouseStressTableColumns {
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
	}
	OverLoadClickHouse(client)
}
