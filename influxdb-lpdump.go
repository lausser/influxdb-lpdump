package main

import (
	"os"
	"bufio"
	"log"
	"fmt"
	"flag"
	"encoding/json"
	//"time"

	"github.com/influxdata/influxdb/client/v2"
)


func PrettyPrint(v interface{}) {
      b, _ := json.MarshalIndent(v, "", "  ")
      println(string(b))
}

func getKeys(c client.Client, db string, series string, key string) ([]string, error) {
	//tags, err = getKeys(c, "nagflux", "metrics", "tag")
	var keys []string
	q := client.Query{  
		Command:  fmt.Sprintf("SHOW %s KEYS FROM %s", key, series),
		Database: db,
	}
	resp, err := c.Query(q)
	if err != nil {
		return keys, err
		log.Fatal(err)
	}
	for _, v := range resp.Results[0].Series[0].Values {
		keys = append(keys, fmt.Sprintf("%v", v[0]))
	}
	return keys, err
}

func buildColumnIndex(columns []string, tags []string, fields []string) map[string]int {
	colindex := make(map[string]int)
	for cidx, c := range columns {
		for _, t := range tags {
			if t == c {
				colindex[t] = cidx
			}
		}
		for _, f := range fields {
			if f == c {
				colindex[f] = cidx
			}
		}
		if c == "time" {
			colindex["time"] = cidx
		}
	}
	return colindex
}

func writeHeader(writer *bufio.Writer, database string) {
	fmt.Fprintln(writer, "# DDL")
	fmt.Fprintln(writer, "# CREATE DATABASE mytest")
	fmt.Fprintln(writer, "# CREATE RETENTION POLICY oneday ON mytest DURATION 1d REPLICATION 1")
	fmt.Fprintln(writer, "")
	fmt.Fprintln(writer, "# DML")
	fmt.Fprintf(writer, "# CONTEXT-DATABASE: %s\n", database)
	fmt.Fprintln(writer, "# CONTEXT-RETENTION-POLICY: default")
}

func main() {
	var query = flag.String("query", "SELECT * FROM metrics", "InfluxDB select statement (please use complete series)")
	var username = flag.String("username", "omdadmin", "Username (default: omdadmin)")
	var password = flag.String("password", "omd", "Password (default: omd)")
	var database = flag.String("database", "nagflux", "Influx Database (default: nagflux)")
	var filebase = flag.String("target", "/tmp/influx-dump", "Base filename, will be extended by numbers (default: /tmp/influx-dump)")
	var chunksize = flag.Int("chunksize", 200000, "Datapoints per file (default: 200000, recommended: 10000)")
	var verbose = flag.Bool("verbose", false, "Output some internals")
	flag.Parse()
	// Create a new HTTPClient
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     "http://localhost:8086",
		Username: *username,
		Password: *password,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	_, _, err = c.Ping(0) 
	if err != nil { 
		fmt.Println("Error pinging InfluxDB Cluster: ", err.Error()) 
		log.Fatal(err)
	}
	q := client.Query{  
		Command:  *query,
		Database: *database,
		Chunked: true,
		ChunkSize: 10000,
		Precision: "ms",
	}
	resp, err := c.Query(q)
	if err != nil {
		log.Fatal(err)
	}
	measurement := resp.Results[0].Series[0].Name
	tags, err := getKeys(c, *database, measurement, "tag")
	if err != nil {
		log.Fatal(err)
	}
	fields, err := getKeys(c, *database, measurement, "field")
	if err != nil {
		log.Fatal(err)
	}
	colindex := buildColumnIndex(resp.Results[0].Series[0].Columns, tags, fields)
	file_counter := 0
	line_counter := 0
	var file *os.File
	var ferr error
	var writer *bufio.Writer
	for _, v := range resp.Results[0].Series[0].Values {
		line := measurement
		for _, t := range tags {
			if v[colindex[t]] != nil {
				line += fmt.Sprintf(",%s=%s", t, v[colindex[t]])
			}
		}
		first_field := true
		for _, f := range fields {
			if v[colindex[f]] != nil {
				if first_field == true {
					line += fmt.Sprintf(" %s=%s", f, v[colindex[f]])
					first_field = false
				} else {
					line += fmt.Sprintf(",%s=%s", f, v[colindex[f]])
				}
			}
		}
		line += fmt.Sprintf(" %v", v[colindex["time"]])
		if line_counter == 0 {
			if file_counter != 0 {
				writer.Flush()
				file.Close()
				line_counter = 0
			}
			file_counter += 1
			file, ferr = os.Create(fmt.Sprintf("%s-%04d", *filebase, file_counter))
			if ferr != nil {
				log.Fatal(ferr)
			}
			if *verbose == true {
				log.Printf("open %s-%04d\n", *filebase, file_counter)
			}
			writer = bufio.NewWriter(file)
			writeHeader(writer, *database)
		}
		fmt.Fprintln(writer, line)
		line_counter += 1
		if line_counter == *chunksize {
			line_counter = 0
		}
	}
	if file_counter != 0 {
		writer.Flush()
		file.Close()
	}
	if *verbose == true {
		log.Printf("wrote %d records to %d files\n", len(resp.Results[0].Series[0].Values), file_counter)
	}
}

