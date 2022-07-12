package reducer

import (
	"encoding/json"
	"fmt"
	"log"
	"map-reduce-server/common"
	"map-reduce-server/invertedIndex"
	"map-reduce-server/netflixData"
	"map-reduce-server/wordCount"
	"os"
	"sort"
)

func DoReduceStep(item common.ReduceStep, reply *common.Response) error {
	var reduceF func(key string, values []string) string

	if item.Method == "wordcount" {
		reduceF = wordCount.WordCountReduceF

	} else if item.Method == "ii" {
		reduceF = invertedIndex.InvertedIndexReduceF

	} else if item.Method == "netflix" {
		reduceF = netflixData.NetflixDataReduceF

	} else {
		return fmt.Errorf("invalid method")
	}
	doReduceStep(item.JobName, item.ReduceStepNumber, item.NumberOfFiles, reduceF, item.Path)

	return nil
}

func doReduceStep(
	jobName string,
	reduceTaskNumber int,
	numberOfFiles int,
	reduceF func(key string, values []string) string,
	path string) {
	mapKeyValue := make(map[string][]string)

	for m := 0; m < numberOfFiles; m++ {

		fileName := common.MapOutputName(jobName, m, reduceTaskNumber)
		fullPath := path + fileName

		//log.Printf(fullPath)

		file, _ := os.Open(fullPath)
		dec := json.NewDecoder(file)
		for {
			var kv common.KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			_, ok := mapKeyValue[kv.Key]
			if !ok {
				mapKeyValue[kv.Key] = []string{}
			}
			mapKeyValue[kv.Key] = append(mapKeyValue[kv.Key], kv.Value)
		}
		file.Close()
		var keys []string
		for k := range mapKeyValue {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		merged := common.ReduceOutputName(jobName, reduceTaskNumber)
		fullPath = path + merged
		log.Printf("Path for reduce file number %d: %s", reduceTaskNumber, fullPath)

		file, _ = os.Create(fullPath)
		enc := json.NewEncoder(file)
		for _, k := range keys {
			enc.Encode(common.KeyValue{Key: k, Value: reduceF(k, mapKeyValue[k])})
		}
		file.Close()

	}

}
