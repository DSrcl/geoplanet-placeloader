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
	DROP_STMT   = "DROP TABLE IF EXISTS place"
	CREATE_STMT = `
	CREATE TABLE place (
		woeid INT,
		admin1 VARCHAR(80),
		admin2 VARCHAR(80),
		admin3 VARCHAR(80),
		swlat FLOAT,
		swlng FLOAT,
		nelat FLOAT,
		nelng FLOAT,
		PRIMARY KEY(woeid),
		UNIQUE (admin1, admin2, admin3)
	)	
	`
	COORD_INDEX_STMT = `
	CREATE INDEX coord
	ON place (swlat, swlng, nelat, nelng);
	`
	NAME_INDEX_STMT = `
	CREATE INDEX place_name
	ON place (admin1, admin2, admin3);
	`
	INSERT_STMT = "INSERT IGNORE INTO place (woeid, admin1, admin2, admin3, swlat, swlng, nelat, nelng) VALUES "
	VAL_COUNT   = 5000
	FIELD_COUNT = 8
)

type Config struct {
	DSN, Input string
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func dbExecAndCheck(db *sql.DB, stmt string, args ...interface{}) {
	_, err := db.Exec(stmt, args...)
	check(err)
}

func getInsertStmt(insertCount int) string {
	placeHolders := make([]string, insertCount)
	for i := range placeHolders {
		placeHolders[i] = "(?,?,?,?,?,?,?,?)"
	}

	return INSERT_STMT + strings.Join(placeHolders, ",")
}

func main() {
	curDir, err := os.Getwd()
	check(err)
	configLoc := filepath.Join(curDir, "db.json")

	configFile, err := ioutil.ReadFile(configLoc)
	check(err)
	var config Config
	json.Unmarshal(configFile, &config)

	db, err := sql.Open("mysql", config.DSN)
	defer db.Close()
	check(err)

	dbExecAndCheck(db, DROP_STMT)
	dbExecAndCheck(db, CREATE_STMT)
	dbExecAndCheck(db, COORD_INDEX_STMT)
	dbExecAndCheck(db, NAME_INDEX_STMT)

	f, err := os.Open(config.Input)
	defer f.Close()
	check(err)

	reader := bufio.NewReader(f)
	scanner := bufio.NewScanner(reader)
	counter := 0
	vals := make([]interface{}, 0, VAL_COUNT*FIELD_COUNT)

	fmt.Println("loading geodata into database...")
	for scanner.Scan() {
		if counter > 0 && counter%VAL_COUNT == 0 {
			dbExecAndCheck(db,
				getInsertStmt(VAL_COUNT),
				vals...)
			vals = make([]interface{}, 0, VAL_COUNT*FIELD_COUNT)
		}

		placeData := strings.Split(scanner.Text(), ",")
		if len(placeData) == FIELD_COUNT {
			vals = append(vals,
				placeData[0],
				placeData[1],
				placeData[2],
				placeData[3],
				placeData[4],
				placeData[5],
				placeData[6],
				placeData[7],
			)
			counter++
		}
	}

	if counter%VAL_COUNT > 0 {
		dbExecAndCheck(db,
			getInsertStmt(counter%VAL_COUNT),
			vals...)
	}

	fmt.Println("Finished. Loaded", counter, "places...")
}
