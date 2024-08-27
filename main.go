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
	host     = flag.String("h", "localhost", "Hostname for destination PSQL")
	port     = flag.Int("P", 5432, "Port number")
	user     = flag.String("U", "postgres", "User for DB")
	db       = flag.String("db", "postgres", "Database for destination PSQL")
	password = flag.String("passwd", "", "Password for destination PSQL user postgres")
	schema   = flag.String("schema", "", "Schema for Fill")
	threads  = flag.Int("threads", 300, "Number of Threads for Parallel Processing")
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
	GoSpot(*source, config, "nse_spot", 120)
}

func Go(p string, config Config, exc string, threads int) {
	//this for nse
	var errFiles map[string]error
	st := time.Now()
	log.Println("RUNNING LOADER FOR :", p, " WITH :", threads, "threads on exchange :", exc)
	time.Sleep(time.Second * 5)
	headers := GenerateTableHeaders()
	var wg sync.WaitGroup
	var paths []string
	log.Println("Generating File Cache")
	e := filepath.Walk(p, func(path string, info os.FileInfo, err error) error {
		if err == nil && strings.Contains(info.Name(), ".csv") {
			paths = append(paths, path)
		}
		return nil
	})
	log.Println("File Cache done, starting DB and loading...")
	if e != nil {
		log.Println(e)
	}
	ccfg, _ := pgx.ParseConnectionString(fmt.Sprintf("user=%s password=%s host=%s port=%d dbname=%s", config.User, config.Pwd, config.Host, config.Port, config.Name))
	for count, path := range paths {

		if count%(threads-1) == 0 {
			fmt.Println("Waiting Threads Full")
			wg.Wait()
			fmt.Println("Threads empty restarting")
		}
		fmt.Println(count)

		// WaitTillMemoryFree()
		wg.Add(1)
		go func(ffname string, fpath string, synchronizer *sync.WaitGroup) {
			// log.Println(ffname, " Started")
			// create table query firxt
			DelTableQuery := fmt.Sprintf("DROP TABLE %s.%s", exc, ffname[:len(ffname)-4])
			if strings.Contains(ffname, "&") {
				ffname = strings.ReplaceAll(ffname, "&", "and")
			}
			if strings.Contains(ffname, "-") {
				ffname = strings.ReplaceAll(ffname, "-", "0")
			}
			createTableQuery := fmt.Sprintf("CREATE TABLE %s.%s (%s);", exc, ffname[:len(ffname)-4], headers)

			db, err := pgx.Connect(ccfg)

			if err != nil {
				log.Println("Error: ", err)
			}
			defer db.Close()
			_, err = db.Exec(DelTableQuery)
			if err != nil {
				log.Println("Error in Delete Table on file: ", ffname, "\n err: ", err)
			} else {
				fmt.Println("Dropped")
			}

			_, err = db.Exec(createTableQuery)
			if err != nil {
				log.Println("Error in Create Table on file: ", ffname, "\n err: ", err)
				db.Close()
				synchronizer.Done()
				return

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
	log.Println("Done Loading: \n errored files: ", errFiles, " \n Time Taken: ", time.Since(st))
}

func GoSpot(p string, config Config, exc string, threads int) {
	starttime := time.Now()
	fmt.Println("RUNNING LOADER FOR :", p, " WITH :", threads, "threads on schema :", exc)
	time.Sleep(time.Second * 5)
	headers := GenerateTableHeadersSpot()
	headers = headers[:len(headers)-1]
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
			var render string
			if strings.Contains(ffname, "_candlestick") {
				ffname = strings.ReplaceAll(ffname, "_candlestick", "")
				render = ffname[:2]
				ffname = strings.ReplaceAll(ffname, fmt.Sprintf("%s_", render), "")
			}
			dropTableQuery := fmt.Sprintf("DROP TABLE %s.%s", exc, ffname[:len(ffname)-4])
			createTableQuery := fmt.Sprintf("CREATE TABLE %s.%s (%s)", exc, ffname[:len(ffname)-4], headers)
			createCandleQuery := fmt.Sprintf("CREATE TABLE %s.%s (%s)", exc, fmt.Sprintf("%s_%s_candlestick", render, ffname[:len(ffname)-4]), headers)
			ccfg, _ := pgx.ParseConnectionString(fmt.Sprintf("user=%s password=%s host=%s port=%d  dbname=%s", config.User, config.Pwd, config.Host, config.Port, config.Name))
			db, err := pgx.Connect(ccfg)
			if err != nil {
				log.Println("Error: ", err)
			}
			defer db.Close()
			_, err = db.Exec(dropTableQuery)
			if err != nil {
				errFiles = append(errFiles, ffname)
			}
			log.Println("DROPPED ", ffname)
			_, err = db.Exec(createTableQuery)
			if err != nil {
				errFiles = append(errFiles, ffname)
			}
			log.Println("CREATED ", ffname)
			_, err = db.Exec(createCandleQuery)
			if err != nil {
				errFiles = append(errFiles, ffname)
			}
			log.Println("CREATED CANDLE", ffname)
			// WaitTillMemoryFree()
			f, _ := os.Open(fpath)
			defer f.Close()
			csvreader := csv.NewReader(f)
			heads, _ := csvreader.Read()
			fmt.Println(heads)
			var rows [][]interface{}
			var newheads = []string{"datetime", "open", "high", "low", "close", "volume", "lastclose"}

			datum, _ := csvreader.ReadAll()
			for _, dp := range datum {
				row := make([]interface{}, len(dp))
				row[0], _ = strconv.Atoi(dp[0])
				row[1], _ = strconv.ParseFloat(dp[1], 32)
				row[2], _ = strconv.ParseFloat(dp[2], 32)
				row[3], _ = strconv.ParseFloat(dp[3], 32)
				row[4], _ = strconv.ParseFloat(dp[4], 32)
				row[5], _ = strconv.ParseFloat(dp[5], 32)
				row[6], _ = strconv.ParseFloat(dp[6], 32)
				// row[7], _ = strconv.Atoi(dp[7])
				// row[8], _ = strconv.Atoi(dp[8])
				rows = append(rows, row)
			}
			f.Close()
			x, err := db.CopyFrom(pgx.Identifier{exc, strings.ToLower(ffname[:len(ffname)-4])}, newheads, pgx.CopyFromRows(rows))
			log.Println(ffname, " DONE: ", x, err)
			// log.Println(rows)
			x, err = db.CopyFrom(pgx.Identifier{exc, strings.ToLower(fmt.Sprintf("%s_%s_candlestick", render, ffname[:len(ffname)-4]))}, newheads, pgx.CopyFromRows(rows))
			log.Println(ffname, " DONE: ", x, err)
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
	return "datetime bigint, open float,high float,low float,close float, volume float,lastclose float,"
}

func GenerateTableHeadersCDS(part int) string {

	basequery := "Instrument VARCHAR, Strike integer,"
	if part == 1 {
		i := 0
		for i <= 240 {
			basequery = fmt.Sprintf("%s T%d integer[],", basequery, i)
			i += 1
		}
	}
	if part == 2 {
		i := 241
		for i <= 480 {
			basequery = fmt.Sprintf("%s T%d integer[],", basequery, i)
			i += 1
		}
	}

	return basequery[:len(basequery)-1]
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

func GoCandle(p string, config Config, exc string, threads int) {
	starttime := time.Now()
	fmt.Println("RUNNING LOADER FOR :", p, " WITH :", threads, "threads on schema :", exc)
	time.Sleep(time.Second * 5)
	headers := GenerateTableHeadersSpot()
	headers = headers[:len(headers)-1]
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
			// dropTableQuery := fmt.Sprintf("DROP TABLE %s.%s", exc, ffname[:len(ffname)-4])
			csize := strings.Split(ffname[:len(ffname)-4], "_")[0]
			inst := strings.Split(ffname[:len(ffname)-4], "_")[1]
			ctype := strings.Split(ffname[:len(ffname)-4], "_")[2]
			inst = strings.ToLower(inst)
			createTableQuery := fmt.Sprintf("CREATE TABLE %s.%s_%s_%s (%s)", exc, inst, csize, ctype, headers)

			createTableQuery = fmt.Sprintf("CREATE TABLE %s.%s_%s_%s (%s)", exc, inst, csize, ctype, headers[:len(headers)-16])

			fmt.Println(createTableQuery)
			ccfg, _ := pgx.ParseConnectionString(fmt.Sprintf("user=%s password=%s host=%s port=%d  dbname=%s", config.User, config.Pwd, config.Host, config.Port, config.Name))
			db, err := pgx.Connect(ccfg)
			if err != nil {
				log.Println("Error: ", err)
			}
			defer db.Close()
			a, err := db.Exec(createTableQuery)
			fmt.Println("ExecResult: ", a)
			if err != nil {
				log.Println("Create Error ", err)
				errFiles = append(errFiles, ffname)
			} else {
				log.Println("CREATED ", ffname)
			}

			// WaitTillMemoryFree()
			f, err := os.Open(fpath)
			if err != nil {
				fmt.Println(err)
			}
			defer f.Close()
			csvreader := csv.NewReader(f)
			heads, err := csvreader.Read()
			fmt.Println(heads, err)
			var rows [][]interface{}
			var newheads = []string{"datetime", "open", "high", "low", "close", "volume", "lastclose"}

			newheads = newheads[:len(newheads)-1]

			datum, err := csvreader.ReadAll()
			// fmt.Println(datum)
			for _, dp := range datum {
				row := make([]interface{}, len(dp))
				row[0], err = strconv.Atoi(dp[0])
				if err != nil {
					fmt.Println(err)
				}

				row[1], err = strconv.ParseFloat(dp[1], 64)
				if err != nil {
					fmt.Println(err, 1)
				}

				row[2], err = strconv.ParseFloat(dp[2], 64)
				if err != nil {
					fmt.Println(err, 2)
				}

				row[3], err = strconv.ParseFloat(dp[3], 64)
				if err != nil {
					fmt.Println(err, 3)
				}

				row[4], err = strconv.ParseFloat(dp[4], 64)
				if err != nil {
					fmt.Println(err, 4)
				}

				row[5], err = strconv.ParseFloat(dp[5], 64)
				if err != nil {
					fmt.Println(err, 5)
				}
				if ctype == "HA" || ctype == "ha" || csize == "1D" {
					rows = append(rows, row)
					continue
				} else {
					if dp[6] == "" {
						row[6] = 0.0
					} else {
						row[6], err = strconv.ParseFloat(dp[6], 64)
						if err != nil {
							fmt.Println(err, 6)
						}
					}

					// row[7], _ = strconv.Atoi(dp[7])
					// row[8], _ = strconv.Atoi(dp[8])
					// fmt.Println(row)
					rows = append(rows, row)
				}

			}

			f.Close()
			// fmt.Println("table", fmt.Sprintf("%s_%s_%s", strings.ToLower(inst), csize, ctype))
			x, err := db.CopyFrom(pgx.Identifier{exc, fmt.Sprintf("%s_%s_%s", strings.ToLower(inst), strings.ToLower(csize), strings.ToLower(ctype))}, newheads, pgx.CopyFromRows(rows))
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
