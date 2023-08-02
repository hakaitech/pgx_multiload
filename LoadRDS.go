package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/jackc/pgx"
)

func Go(p string, config Config, exc string, threads int) {
	//this for nse
	headers := GenerateTableHeaders()
	counter := 0
	var wg sync.WaitGroup
	e := filepath.Walk(p, func(path string, info os.FileInfo, err error) error {
		if err == nil && strings.Contains(info.Name(), ".csv") {
			wg.Add(1)
			counter += 1
			if counter >= threads {
				log.Println("WAITING")
				wg.Wait()
				counter = 1
			}

			go func(ffname string, fpath string) {
				defer wg.Done()

				//create table query firxt
				createTableQuery := fmt.Sprintf("CREATE UNLOGGED TABLE %s.%s (%s);", exc, ffname[:len(ffname)-4], headers)
				ccfg, _ := pgx.ParseConnectionString("user=postgres password=MyPassw0rd! host=ruphiya.cvp1ajcdfcnk.ap-south-1.rds.amazonaws.com port=5432  dbname=ruphiya sslmode=require")
				db, err := pgx.Connect(ccfg)

				// pgx.ConnConfig{
				// 	Host:     config.Host,
				// 	Port:     5432,
				// 	Database: config.Name,
				// 	User:     config.User,
				// 	Password: config.Pwd,

				// }
				if err != nil {
					log.Println("Error: ", err)
				}
				defer db.Close()
				res, err := db.Exec(createTableQuery)
				log.Println(res)
				if err != nil {
					log.Println("Error: ", err)
				}

				f, _ := os.Open(fpath)
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
					fmt.Println(len(dp))
					row[0] = dp[0]
					log.Println(row[0])
					row[1], _ = strconv.Atoi(dp[1])
					i := 2
					for i < len(dp) {
						tmp := dp[i]
						tmp = tmp[1 : len(tmp)-1]
						nums := strings.Split(tmp, ",")
						intarr := []int{}
						for _, n := range nums {
							cn, _ := strconv.Atoi(n)
							intarr = append(intarr, cn)
						}
						row[i] = intarr
						i += 1
					}

					rows = append(rows, row)
				}

				x, err := db.CopyFrom(pgx.Identifier{exc, strings.ToLower(ffname[:len(ffname)-4])}, newheads, pgx.CopyFromRows(rows))
				fmt.Println(ffname, " DONE: ", x, err)
				db.Close()
			}(info.Name(), path)

		}
		return nil

	})
	wg.Wait()
	if e != nil {
		log.Println(e)
	}
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
