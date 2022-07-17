package mapreduce

import (
	"fmt"
	"io/ioutil"
	"map-reduce-client/client"
	"map-reduce-client/common"
	"map-reduce-client/shuffleSort"
	"strconv"
	"time"
)

func Run(useCase string, jobName string, numberOfMapOutput int, path string, column string, clients []client.Client) {
	files := common.OpenFiles(column)

	start := time.Now()
	shuffleSort.DoMap(jobName, files, numberOfMapOutput, useCase, path, column, clients)
	elapsed := time.Since(start)

	fmt.Println("Map phase took:", elapsed)

	start = time.Now()
	shuffleSort.DoReduce(jobName, numberOfMapOutput, len(files), useCase, path, clients)

	elapsed = time.Since(start)

	fmt.Println("Reduce phase took:", elapsed)

	// if useCase != "netflix" {
	//common.Merge0rderByOccurrence(numberOfMapOutput, jobName)
	// } else {
	common.MergeAlphabeticalOrder(numberOfMapOutput, jobName)
	// }
	test(useCase, jobName, len(files), column)

}
func test(useCase string, jobName string, numberOfFiles int, column string) {
	var resultFileName string
	if useCase != "netflix" {
		if useCase == "ii" {
			resultFileName = "result-" + strconv.Itoa(numberOfFiles) + "files-" + "invertedIndex" + ".txt"
		} else {
			resultFileName = "result-" + strconv.Itoa(numberOfFiles) + "files-" + "WordCount" + ".txt"

		}

	} else {
		resultFileName = "result-" + "netflixData-" + column + ".txt"

	}

	resultFile, err := ioutil.ReadFile(resultFileName)
	if err != nil {
		fmt.Println(err)
	}

	jobFile, err := ioutil.ReadFile(common.ResultName(jobName))
	if err != nil {
		fmt.Println(err)
	}
	if string(resultFile) == string(jobFile) {
		fmt.Println("it worked")
	} else {
		fmt.Println("It did not work")
	}
}
