package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx"
	"github.com/pbnjay/memory"
)

var (
	source   = flag.String("s", "", "Source Files Directory to be copied")
	host     = flag.String("h", "", "Hostname for destination PSQL")
	port     = flag.Int("P", 5432, "Port number")
	user     = flag.String("U", "postgres", "User for DB")
	db       = flag.String("db", "", "Database for destination PSQL")
	password = flag.String("passwd", "", "Password for destination PSQL user postgres")
	schema   = flag.String("schema", "", "Schema for Fill")
	threads  = flag.Int("threads", 10, "Number of Threads for Parallel Processing")
	spot     = flag.Bool("spot", false, "Enables Spot Writer")
	fno      = flag.Bool("fno", false, "Enables FnO Writer")
	cds      = flag.Bool("cds", false, "Enables CDS Writer")
	mcx      = flag.Bool("mcx", false, "Enables MCX Writer")
)

type Config struct {
	Host string
	Port int
	User string
	Pwd  string
	Name string
}

func main() {
	var config Config
	flag.Parse()
	config.Host = *host
	config.Name = *db
	config.Port = *port
	config.Pwd = *password
	config.User = *user
	if *spot {
		GoSpot(*source, config, *schema, *threads)
	}
	if *fno {
		Go(*source, config, *schema, *threads)
	}

}

func Go(p string, config Config, exc string, threads int) {
	//this for nse
	fmt.Println("RUNNING LOADER FOR :", p, " WITH :", threads, "threads on exchange :", exc)
	time.Sleep(time.Second * 5)
	headers := GenerateTableHeaders()
	var wg sync.WaitGroup
	var paths []string
	e := filepath.Walk(p, func(path string, info os.FileInfo, err error) error {
		if err == nil && strings.Contains(info.Name(), ".csv") {
			paths = append(paths, path)
		}
		return nil
	})

	if e != nil {
		log.Println(e)
	}

	for count, path := range paths {

		if count%(threads-1) == 0 {
			log.Println("Waiting Therads Full")
			wg.Wait()
			log.Println("Threads empty restarting")
		}
		fmt.Println(count)
		// WaitTillMemoryFree()
		wg.Add(1)
		go func(ffname string, fpath string, synchronizer *sync.WaitGroup) {
			// log.Println(ffname, " Started")
			// create table query firxt
			createTableQuery := fmt.Sprintf("CREATE UNLOGGED TABLE %s.%s (%s);", exc, ffname[:len(ffname)-4], headers)
			ccfg, _ := pgx.ParseConnectionString(fmt.Sprintf("user=%s password=%s host=%s port=%d  dbname=%s sslmode=require", config.User, config.Pwd, config.Host, config.Port, config.Name))
			db, err := pgx.Connect(ccfg)

			if err != nil {
				log.Println("Error: ", err)
			}
			defer db.Close()
			_, err = db.Exec(createTableQuery)

			if err != nil {
				// log.Println("Error: ", err)
			}
			// WaitTillMemoryFree()
			f, _ := os.Open(fpath)
			defer f.Close()
			csvreader := csv.NewReader(f)
			heads, _ := csvreader.Read()
			var rows [][]interface{}

			var newheads []string

			for _, h := range heads {
				newheads = append(newheads, strings.ToLower(h))
			}
			// WaitTillMemoryFree()
			datum, _ := csvreader.ReadAll()
			for _, dp := range datum {
				row := make([]interface{}, len(dp))
				row[0] = dp[0]
				row[1], _ = strconv.Atoi(dp[1])
				i := 2
				for i < len(dp) {
					tmp := dp[i]
					tmp = tmp[1 : len(tmp)-1]
					nums := strings.Split(tmp, ",")
					intarr := []int{}
					for _, n := range nums {
						cn, err := strconv.Atoi(strings.TrimSpace(n))
						if err != nil {
							log.Println(err)
						}
						intarr = append(intarr, cn)
					}
					row[i] = intarr
					i += 1
				}

				rows = append(rows, row)
			}
			f.Close()
			x, err := db.CopyFrom(pgx.Identifier{exc, strings.ToLower(ffname[:len(ffname)-4])}, newheads, pgx.CopyFromRows(rows))
			log.Println(ffname, " DONE: ", x, err)
			// log.Println(rows)
			db.Close()
			synchronizer.Done()
		}(strings.Split(path, "/")[len(strings.Split(path, "/"))-1], path, &wg)

	}

	wg.Wait()
}

func GoSpot(p string, config Config, exc string, threads int) {
	starttime := time.Now()
	fmt.Println("RUNNING LOADER FOR :", p, " WITH :", threads, "threads on schema :", exc)
	time.Sleep(time.Second * 5)
	headers := GenerateTableHeadersSpot()
	var wg sync.WaitGroup
	var paths []string
	var errFiles []string
	e := filepath.Walk(p, func(path string, info os.FileInfo, err error) error {
		if err == nil && strings.Contains(info.Name(), ".csv") {
			paths = append(paths, path)
		}
		return nil
	})

	if e != nil {
		log.Println(e)
	}

	for count, path := range paths {
		if count%(threads-1) == 0 {
			log.Println("Waiting Threads Full")
			wg.Wait()
			log.Println("Threads empty restarting")
		}
		// WaitTillMemoryFree()
		wg.Add(1)
		go func(ffname, fpath string, synchroniser *sync.WaitGroup) {
			createTableQuery := fmt.Sprintf("CREATE TABLE %s.%s (%s)", exc, ffname[:len(ffname)-4], headers)
			ccfg, _ := pgx.ParseConnectionString(fmt.Sprintf("user=%s password=%s host=%s port=%d  dbname=%s sslmode=require", config.User, config.Pwd, config.Host, config.Port, config.Name))
			db, err := pgx.Connect(ccfg)
			if err != nil {
				log.Println("Error: ", err)
			}
			defer db.Close()
			_, err = db.Exec(createTableQuery)
			if err != nil {
				errFiles = append(errFiles, ffname)
			}
			// WaitTillMemoryFree()
			f, _ := os.Open(fpath)
			defer f.Close()
			csvreader := csv.NewReader(f)
			heads, _ := csvreader.Read()
			var rows [][]interface{}
			var newheads []string

			for _, h := range heads {
				newheads = append(newheads, strings.ToLower(h))
			}
			datum, _ := csvreader.ReadAll()
			for _, dp := range datum {
				row := make([]interface{}, len(dp))
				row[0], _ = strconv.Atoi(dp[0])
				row[1], _ = strconv.Atoi(dp[1])
				row[2], _ = strconv.Atoi(dp[2])
				row[3], _ = strconv.Atoi(dp[3])
				row[4], _ = strconv.Atoi(dp[4])
				row[5], _ = strconv.Atoi(dp[5])
				row[6], _ = strconv.Atoi(dp[6])
				row[7], _ = strconv.Atoi(dp[7])
				row[8], _ = strconv.Atoi(dp[8])
				rows = append(rows, row)
			}
			f.Close()
			x, err := db.CopyFrom(pgx.Identifier{exc, strings.ToLower(ffname[:len(ffname)-4])}, newheads, pgx.CopyFromRows(rows))
			log.Println(ffname, " DONE: ", x, err)
			// log.Println(rows)
			db.Close()
			synchroniser.Done()
		}(strings.Split(path, "/")[len(strings.Split(path, "/"))-1], path, &wg)
	}
	fmt.Println("All Files are now done, Waiting for Completion")
	wg.Wait()
	timediff := time.Since(starttime)
	fmt.Println("*******************DONE************************")
	fmt.Println("Time Taken: ", timediff)
	fmt.Println("Files that errored out: ", errFiles)
}

func GenerateTableHeaders() string {
	i := 0
	basequery := "Instrument VARCHAR, Strike integer,"
	for i <= 375 {
		basequery = fmt.Sprintf("%s T%d integer[],", basequery, i)
		i += 1
	}
	return basequery[:len(basequery)-1]
}

func GenerateTableHeadersSpot() string {
	return "DateTime bigint, open integer,high integer,low integer,close integer, volume integer, tradedate bigint, lastclose integer, tradetime integer"
}

func WaitTillMemoryFree() {
	var wg sync.WaitGroup
	wg.Add(1)
	for {
		log.Println("Memory Free: ", memory.FreeMemory()/1000000000)
		if memory.FreeMemory()/1000000000 >= 75 {
			wg.Done()
			return
		} else {
			continue
		}
	}
}
