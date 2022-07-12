package main

import (
	"fmt"
	"map-reduce-client/client"
	mapreduce "map-reduce-client/mapReduce"
	"os"
	"strconv"
)

func main() {
	var numberOfMapOutput int
	var jobName string
	var method string

	path := "/path/to/mapReduceWithRPC/client/"
	method = "wordcount"
	//method = "ii"
	//method = "netflix"

	column := ""
	// if neflix data
	// column := "type"
	//column := "director"
	// column := "cast"
	//column := "country"
	// column := "release_year"
	// column := "duration"
	// column := "listed_in"

	numberOfMapOutput = 8
	jobName = "teste"

	fmt.Println("Number of servers:", os.Args[1])
	fmt.Println("MapReduce method:", method, column)

	nServers, _ := strconv.Atoi(os.Args[1])
	var clients []client.Client

	for i := 0; i < nServers; i++ {
		local := "localhost:" + os.Args[i+2]
		clients = append(clients, *client.New("tcp", local))
		fmt.Println(local)
		clients[i].HealthCheck(local)
	}

	mapreduce.Run(method, jobName, numberOfMapOutput, path, column, clients)

}
