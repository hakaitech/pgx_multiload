// func GoCDS(p string, config Config, exc string, threads int) {
// 	//this for nse
// 	var errFiles []string
// 	st := time.Now()
// 	log.Println("RUNNING LOADER FOR :", p, " WITH :", threads, "threads on exchange :", exc)
// 	headers1 := GenerateTableHeadersCDS(1)
// 	headers2 := GenerateTableHeadersCDS(2)
// 	wg := waiter.New("Uploading Files ", log.Writer())
// 	// var wg sync.WaitGroup
// 	var paths []string
// 	// pb := progressbar.Default(-1, "Building Cache")
// 	e := filepath.Walk(p, func(path string, info os.FileInfo, err error) error {
// 		if err == nil && strings.Contains(info.Name(), ".csv") {
// 			paths = append(paths, path)
// 		}
// 		return nil
// 	})

// 	if e != nil {
// 		log.Println(e)
// 	}

// 	for count, path := range paths {

// 		if count%(threads) == 0 && count > 0 {
// 			wg.Wait()
// 		}
// 		// WaitTillMemoryFree()
// 		wg.Add(1)
// 		go func(ffname string, fpath string, synchronizer *waiter.Waiter) {
// 			// log.Println(ffname, " Started")
// 			// create table query firxt
// 			var createTableQuery string
// 			ffname = ffname[:len(ffname)-4]
// 			part := strings.Split(ffname, "_")[2]

// 			if part == "1" {
// 				createTableQuery = fmt.Sprintf("CREATE UNLOGGED TABLE %s.%s (%s)", exc, ffname, headers1)
// 			} else {
// 				createTableQuery = fmt.Sprintf("CREATE UNLOGGED TABLE %s.%s (%s)", exc, ffname, headers2)
// 			}
// 			DelTableQuery := fmt.Sprintf("DROP TABLE %s.%s", exc, ffname)
// 			ccfg, _ := pgx.ParseConnectionString(fmt.Sprintf("user=%s password=%s host=%s port=%d  dbname=%s", config.User, config.Pwd, config.Host, config.Port, config.Name))
// 			db, err := pgx.Connect(ccfg)
// 			if err != nil {
// 				log.Println("Error: ", err)
// 			}
// 			_, err = db.Exec(DelTableQuery)
// 			if err != nil {
// 				// log.Println("Error in Delete Table on file: ", ffname, "\n err: ", err)
// 				errFiles = append(errFiles, ffname)
// 			}
// 			_, err = db.Exec(createTableQuery)
// 			if err != nil {
// 				log.Println("Error in Create Table on file: ", ffname, "\n err: ", err)
// 				errFiles = append(errFiles, ffname)
// 			}
// 			// WaitTillMemoryFree()
// 			f, _ := os.Open(fpath)
// 			defer f.Close()
// 			csvreader := csv.NewReader(f)
// 			heads, _ := csvreader.Read()
// 			var rows [][]interface{}

// 			var newheads []string

// 			for _, h := range heads {
// 				newheads = append(newheads, strings.ToLower(h))
// 			}
// 			// WaitTillMemoryFree()
// 			datum, _ := csvreader.ReadAll()
// 			for _, dp := range datum {
// 				row := make([]interface{}, len(dp))
// 				row[0] = dp[0]
// 				row[1], _ = strconv.Atoi(dp[1])
// 				i := 2
// 				for i < len(dp) {
// 					tmp := dp[i]
// 					tmp = tmp[1 : len(tmp)-1]
// 					nums := strings.Split(tmp, ",")
// 					intarr := []int{}
// 					for _, n := range nums {
// 						cn, err := strconv.Atoi(strings.TrimSpace(n))
// 						if err != nil {
// 							log.Println(err)
// 						}
// 						intarr = append(intarr, cn)
// 					}
// 					row[i] = intarr
// 					i += 1
// 				}

// 				rows = append(rows, row)
// 			}
// 			f.Close()
// 			db.CopyFrom(pgx.Identifier{exc, strings.ToLower(ffname)}, newheads, pgx.CopyFromRows(rows))
// 			// log.Println(x, err)
// 			db.Close()
// 			synchronizer.Done()

// 		}(strings.Split(path, "/")[len(strings.Split(path, "/"))-1], path, wg)

// 	}
// 	wg.Wait()
// 	log.Println("Done Loading: \n errored files: ", errFiles, " \n Time Taken: ", time.Since(st))
// }
