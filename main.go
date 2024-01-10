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

	"github.com/gen0cide/waiter"
	"github.com/jackc/pgx"
	"github.com/pbnjay/memory"
)

var (
	source   = flag.String("s", "", "Source Files Directory to be copied")
	host     = flag.String("h", "ruphiya-db-do-user-14476372-0.b.db.ondigitalocean.com", "Hostname for destination PSQL")
	port     = flag.Int("P", 25060, "Port number")
	user     = flag.String("U", "doadmin", "User for DB")
	db       = flag.String("db", "ruphiya", "Database for destination PSQL")
	password = flag.String("passwd", "AVNS_ybQV3zZ8J4V5lDs6neT", "Password for destination PSQL user postgres")
	schema   = flag.String("schema", "", "Schema for Fill")
	threads  = flag.Int("threads", 200, "Number of Threads for Parallel Processing")
	spot     = flag.Bool("spot", false, "Enables Spot Writer")
	fno      = flag.Bool("fno", false, "Enables FnO Writer")
	cds      = flag.Bool("cds", false, "Enables CDS Writer")
	mcx      = flag.Bool("mcx", false, "Enables MCX Writer")
	candle   = flag.Bool("candle", false, "Enables Candle Writer")
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
	config.Host = "103.191.209.107"
	config.Name = "ruphiya"
	config.Port = 5432
	config.Pwd = "1NuGd02T92VbhPmFu0e3sWekVX6R0P"
	config.User = "anitix"
	// f, err := os.OpenFile("pgx_multiload.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	// if err != nil {
	// 	log.Fatal("Error in creating Logfile")
	// }
	// defer f.Close()
	// log.SetOutput(f)
	// if *spot {
	// 	GoSpot(*source, config, *schema, *threads)
	// }
	// if *fno {
	// 	Go(*source, config, *schema, *threads)
	// }
	// if *candle {
	// 	GoCandle(*source, config, *schema, *threads)
	// }
	// if *cds {
	// 	GoCDS(*source, config, *schema, *threads)
	// }
	Go("/Data", config, "nse", 120)

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
			// DelTableQuery := fmt.Sprintf("DROP TABLE %s.%s", exc, ffname[:len(ffname)-4])
			if strings.Contains(ffname[:len(ffname)-4], "&") {
				ffname = strings.ReplaceAll(ffname, "&", "and")
			}
			if strings.Contains(ffname[:len(ffname)-4], "-") {
				ffname = strings.ReplaceAll(ffname, "-", "0")
			}
			createTableQuery := fmt.Sprintf("CREATE TABLE %s.%s (%s);", exc, ffname[:len(ffname)-4], headers)

			db, err := pgx.Connect(ccfg)

			if err != nil {
				log.Println("Error: ", err)
			}
			defer db.Close()
			// _, err = db.Exec(DelTableQuery)
			// if err != nil {
			// 	log.Println("Error in Delete Table on file: ", ffname, "\n err: ", err)
			// }
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

func GoCDS(p string, config Config, exc string, threads int) {
	//this for nse
	var errFiles []string
	st := time.Now()
	log.Println("RUNNING LOADER FOR :", p, " WITH :", threads, "threads on exchange :", exc)
	headers1 := GenerateTableHeadersCDS(1)
	headers2 := GenerateTableHeadersCDS(2)
	wg := waiter.New("Uploading Files ", log.Writer())
	// var wg sync.WaitGroup
	var paths []string
	// pb := progressbar.Default(-1, "Building Cache")
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

		if count%(threads) == 0 && count > 0 {
			wg.Wait()
		}
		// WaitTillMemoryFree()
		wg.Add(1)
		go func(ffname string, fpath string, synchronizer *waiter.Waiter) {
			// log.Println(ffname, " Started")
			// create table query firxt
			var createTableQuery string
			ffname = ffname[:len(ffname)-4]
			part := strings.Split(ffname, "_")[2]

			if part == "1" {
				createTableQuery = fmt.Sprintf("CREATE UNLOGGED TABLE %s.%s (%s)", exc, ffname, headers1)
			} else {
				createTableQuery = fmt.Sprintf("CREATE UNLOGGED TABLE %s.%s (%s)", exc, ffname, headers2)
			}
			DelTableQuery := fmt.Sprintf("DROP TABLE %s.%s", exc, ffname)
			ccfg, _ := pgx.ParseConnectionString(fmt.Sprintf("user=%s password=%s host=%s port=%d  dbname=%s", config.User, config.Pwd, config.Host, config.Port, config.Name))
			db, err := pgx.Connect(ccfg)
			if err != nil {
				log.Println("Error: ", err)
			}
			_, err = db.Exec(DelTableQuery)
			if err != nil {
				// log.Println("Error in Delete Table on file: ", ffname, "\n err: ", err)
				errFiles = append(errFiles, ffname)
			}
			_, err = db.Exec(createTableQuery)
			if err != nil {
				log.Println("Error in Create Table on file: ", ffname, "\n err: ", err)
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
			db.CopyFrom(pgx.Identifier{exc, strings.ToLower(ffname)}, newheads, pgx.CopyFromRows(rows))
			// log.Println(x, err)
			db.Close()
			synchronizer.Done()

		}(strings.Split(path, "/")[len(strings.Split(path, "/"))-1], path, wg)

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
			// dropTableQuery := fmt.Sprintf("DROP TABLE %s.%s", exc, ffname[:len(ffname)-4])
			createTableQuery := fmt.Sprintf("CREATE TABLE %s.%s (%s)", exc, ffname[:len(ffname)-4], headers)
			ccfg, _ := pgx.ParseConnectionString(fmt.Sprintf("user=%s password=%s host=%s port=%d  dbname=%s", config.User, config.Pwd, config.Host, config.Port, config.Name))
			db, err := pgx.Connect(ccfg)
			if err != nil {
				log.Println("Error: ", err)
			}
			defer db.Close()
			// _, err = db.Exec(dropTableQuery)
			// if err != nil {
			// 	errFiles = append(errFiles, ffname)
			// }
			// log.Println("DROPPED ", ffname)
			_, err = db.Exec(createTableQuery)
			if err != nil {
				errFiles = append(errFiles, ffname)
			}
			log.Println("CREATED ", ffname)
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
				row[1], _ = strconv.ParseFloat(dp[1], 32)
				row[2], _ = strconv.ParseFloat(dp[2], 32)
				row[3], _ = strconv.ParseFloat(dp[3], 32)
				row[4], _ = strconv.ParseFloat(dp[4], 32)
				row[5], _ = strconv.ParseFloat(dp[5], 32)
				// row[6], _ = strconv.Atoi(dp[6])
				// row[7], _ = strconv.Atoi(dp[7])
				// row[8], _ = strconv.Atoi(dp[8])
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
	return "timestamp bigint, open float,high float,low float,close float, volume float,lastclose integer,"
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
			createTableQuery := fmt.Sprintf("CREATE TABLE %s.%s_%s (%s)", exc, inst, csize, headers)
			ccfg, _ := pgx.ParseConnectionString(fmt.Sprintf("user=%s password=%s host=%s port=%d  dbname=%s", config.User, config.Pwd, config.Host, config.Port, config.Name))
			db, err := pgx.Connect(ccfg)
			if err != nil {
				log.Println("Error: ", err)
			}
			defer db.Close()
			// _, err = db.Exec(dropTableQuery)
			// if err != nil {
			// 	errFiles = append(errFiles, ffname)
			// }
			// log.Println("DROPPED ", ffname)
			_, err = db.Exec(createTableQuery)
			if err != nil {
				errFiles = append(errFiles, ffname)
			}
			log.Println("CREATED ", ffname)
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
			if csize == "1m" {
				for _, dp := range datum {
					row := make([]interface{}, len(dp))
					row[0], _ = strconv.Atoi(dp[0])
					row[1], _ = strconv.ParseFloat(dp[1], 32)
					row[2], _ = strconv.ParseFloat(dp[2], 32)
					row[3], _ = strconv.ParseFloat(dp[3], 32)
					row[4], _ = strconv.ParseFloat(dp[4], 32)
					row[5], _ = strconv.ParseFloat(dp[5], 32)
					// row[6], _ = strconv.Atoi(dp[6])
					// row[7], _ = strconv.Atoi(dp[7])
					// row[8], _ = strconv.Atoi(dp[8])
					rows = append(rows, row)
				}
			} else {
				for _, dp := range datum {
					row := make([]interface{}, len(dp))
					row[0], _ = strconv.Atoi(dp[0])
					row[1], _ = strconv.ParseFloat(dp[1], 32)
					row[2], _ = strconv.ParseFloat(dp[2], 32)
					row[3], _ = strconv.ParseFloat(dp[3], 32)
					row[4], _ = strconv.ParseFloat(dp[4], 32)
					row[5], _ = strconv.ParseFloat(dp[5], 32)
					// row[6], _ = strconv.Atoi(dp[6])
					// row[7], _ = strconv.Atoi(dp[7])
					// row[8], _ = strconv.Atoi(dp[8])
					rows = append(rows, row)
				}
			}
			f.Close()
			x, err := db.CopyFrom(pgx.Identifier{exc, fmt.Sprintf("%s_%s", strings.ToLower(inst), csize)}, newheads, pgx.CopyFromRows(rows))
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
