package mapper

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"map-reduce-server/common"
	"map-reduce-server/invertedIndex"
	"map-reduce-server/netflixData"
	"map-reduce-server/wordCount"
	"os"
	"sync"
)

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func DoMapStep(item common.MapStep) error {
	var mapF func(file string, contents string) []common.KeyValue
	log.Printf("UseCase %s", item.UseCase)

	if item.UseCase == "wordcount" {
		mapF = wordCount.WordCountMapF

	} else if item.UseCase == "ii" {
		mapF = invertedIndex.InvertedIndexMapF

	} else if item.UseCase == "netflix" {
		mapF = netflixData.NetflixDataMapF

	} else {
		log.Printf("Invalid UseCase %s", item.UseCase)
		return fmt.Errorf("Invalid UseCase")
	}

	doMapStep(item.JobName, item.MapStepNumber, item.File, item.NumberOfMapOutput, mapF, item.Path, item.Column)
	return nil

}

func doMapStep(
	jobName string,
	mapTaskNumber int,
	inFile string,
	numberOfMapOutput int,
	mapF func(file string, contents string) []common.KeyValue,
	path string,
	column string) {
	log.Printf(inFile)

	kvList := mapF(inFile, getContent(inFile, column))

	var wg sync.WaitGroup

	for r := 0; r < numberOfMapOutput; r++ {

		wg.Add(1)

		r := r
		go func() {
			defer wg.Done()
			doMapStepLoop(jobName, mapTaskNumber, numberOfMapOutput, kvList, r, path)
		}()

	}
	wg.Wait()

}
func doMapStepLoop(jobName string, mapTaskNumber int, nReduce int, kvList []common.KeyValue, count int, path string) {
	reduceFileName := common.MapOutputName(jobName, mapTaskNumber, count)
	fullPath := path + reduceFileName

	reduceFile, err := os.Create(fullPath)
	if err != nil {
		fmt.Println(err)
	}
	enc := json.NewEncoder(reduceFile)
	for _, kv := range kvList {
		if (int(ihash(kv.Key)) % nReduce) == count {
			err := enc.Encode(&kv)
			if err != nil {
				fmt.Println(err)
			}

		}
	}
	reduceFile.Close()

}
