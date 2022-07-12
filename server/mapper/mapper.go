package mapper

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"
	"sync"
	"map-reduce-server/wordCount"
	"map-reduce-server/invertedIndex"
	"map-reduce-server/netflixData"
	"map-reduce-server/common"
	"log"
)

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func DoMapStep(item common.MapStep, reply *common.Response) error {
	var mapF func(file string, contents string) []common.KeyValue
	log.Printf("method %s",item.Method)

	if item.Method == "wordcount" {
		mapF = wordCount.WordCountMapF

	} else if item.Method == "ii" {
		mapF = invertedIndex.InvertedIndexMapF

	} else if item.Method == "netflix" {
		mapF = netflixData.NetflixDataMapF

	}else{
		log.Printf("Invalid Method %s",item.Method)
		return fmt.Errorf("Invalid Method")
	}

	doMapStep(item.JobName, item.MapStepNumber, item.File, item.NumberOfMapOutput, mapF, item.Path,item.Column)
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