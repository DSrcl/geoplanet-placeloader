package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

const (
	DROP_STMT   string = "DROP TABLE IF EXISTS place"
	CREATE_STMT string = `
	CREATE TABLE place (
		woeid INT,
		name VARCHAR(500),
		swlat FLOAT,
		swlng FLOAT,
		nelat FLOAT,
		nelng FLOAT,
		PRIMARY KEY(woeid)
	)	
	`
	INDEX_STMT string = `
	CREATE INDEX coord
	ON place (swlat, swlng, nelat, nelng);
	`
	INSERT_STMT string = "INSERT IGNORE INTO place (woeid, name, swlat, swlng, nelat, nelng) VALUES "
	VAL_COUNT   int    = 10000
)

type Config struct {
	DSN, Input string
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func main() {
	curDir, err := os.Getwd()
	check(err)
	configLoc := filepath.Join(curDir, "db.conf")

	configFile, err := ioutil.ReadFile(configLoc)
	check(err)
	var config Config
	json.Unmarshal(configFile, &config)

	db, err := sql.Open("mysql", config.DSN)
	defer db.Close()
	check(err)

	db.Exec(DROP_STMT)
	db.Exec(CREATE_STMT)
	db.Exec(INDEX_STMT)

	placeHolders := make([]string, VAL_COUNT)
	for i := range placeHolders {
		placeHolders[i] = "(?,?,?,?,?,?)"
	}

	insert := INSERT_STMT + strings.Join(placeHolders, ",")

	f, err := os.Open(config.Input)
	defer f.Close()
	check(err)

	reader := bufio.NewReader(f)
	scanner := bufio.NewScanner(reader)
	counter := 0
	var vals []interface{}

	fmt.Println("loading geodata into database...")

	for scanner.Scan() {
		if counter%VAL_COUNT == 0 {
			if counter > 0 {
				_, e := db.Exec(insert, vals...)
				if e != nil {
					fmt.Println(e)
				}
			}

			vals = make([]interface{}, 0, VAL_COUNT*6)
		}

		placeData := strings.Split(scanner.Text(), "\t")
		if len(placeData) < 6 {
			continue
		}
		vals = append(vals, placeData[0], placeData[1], placeData[2], placeData[3], placeData[4], placeData[5])

		counter++
	}

	fmt.Println("Finished. Loaded", counter, "places...")

}
