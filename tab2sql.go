package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	_ "code.google.com/p/go-sqlite/go1/sqlite3"
)

var outfile = flag.String("o", "[stem].sqlite", "filename of output db")
var header = flag.Bool("header", false, "true if tab delimited file contains header line")
var tblname = flag.String("tbl", "[stem]", "name of sqlite table to create")

func main() {
	// process cli flags
	flag.Parse()
	log.SetFlags(log.Lshortfile)

	infile := flag.Arg(0)

	stem := filepath.Base(infile)
	stem = stem[:len(stem)-len(filepath.Ext(stem))]
	if *tblname == "[stem]" {
		*tblname = stem
	}
	if *outfile == "[stem].sqlite" {
		*outfile = stem + ".sqlite"
	}

	// read in tab delimited file and its header
	data, err := ioutil.ReadFile(infile)
	fatalif(err)
	lines := strings.Split(string(data), "\n")

	db, err := sql.Open("sqlite3", *outfile)
	fatalif(err)

	head := []string{}
	fields := strings.Split(lines[0], "\t")
	if *header {
		head = fields
		for i, v := range head {
			head[i] = sanitize(v)
		}
		lines = lines[1:]
	} else {
		for i := range fields {
			head = append(head, fmt.Sprintf("field%v", i))
		}
	}

	// create the table
	s := "CREATE TABLE " + *tblname + " ("
	for i, name := range head {
		if i > 0 {
			s += ", "
		}
		kind := " TEXT"
		if isNumber(fields[i]) {
			kind = " REAL"
		}
		s += name + kind
	}
	s += ");"

	_, err = db.Exec(s)
	fatalif(err)

	// populate the table
	tx, err := db.Begin()
	fatalif(err)

	s = "INSERT INTO " + *tblname + " VALUES ("
	for i := range head {
		if i > 0 {
			s += ", ?"
		} else {
			s += "?"
		}
	}
	s += ");"
	for j, line := range lines {
		fields := strings.Split(line, "\t")
		_, err = tx.Exec(s, convert(fields)...)
		if err != nil {
			log.Printf("line %v error: %v", j, err)
		}
	}
	fatalif(tx.Commit())
}

func isNumber(s string) bool {
	re := regexp.MustCompile(`^[0-9]+(\.[0-9]+)?$`)
	return re.Match([]byte(s))
}

func convert(fields []string) []interface{} {
	c := []interface{}{}
	for _, field := range fields {
		if isNumber(field) {
			v, err := strconv.ParseFloat(field, 64)
			fatalif(err)
			c = append(c, v)
		} else {
			c = append(c, field)
		}
	}
	return c
}

func sanitize(ident string) string {
	s := ident
	if strings.ContainsAny(s[0:1], "1234567890") {
		s = string(append([]rune{'_'}, []rune(s)...))
	}
	s = strings.Replace(s, "-", "_", -1)
	s = strings.Replace(s, "/", "_", -1)
	s = strings.Replace(s, " ", "_", -1)
	return s
}

func fatalif(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
