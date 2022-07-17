package shuffleSort

import (
	"map-reduce-client/client"
	"sync"
)

func DoMap(jobName string,
	files []string,
	numberOfMapOutput int,
	useCase string,
	path string,
	column string,
	clients []client.Client) {
	var wg sync.WaitGroup

	for i, file := range files {
		wg.Add(1)
		file := file

		i := i
		j := i % len(clients)
		go func() {
			defer wg.Done()
			clients[j].DoMapStep(useCase, jobName, i, file, numberOfMapOutput, path, column)
		}()

	}
	wg.Wait()

}

func DoReduce(
	jobName string,
	numberOfMapOutput int,
	numberOfFiles int,
	useCase string,
	path string,
	clients []client.Client) {

	var wg sync.WaitGroup

	for m := 0; m < numberOfMapOutput; m++ {
		wg.Add(1)
		m := m
		j := m % len(clients)

		go func() {
			defer wg.Done()

			clients[j].DoReduceStep(useCase, jobName, m, numberOfFiles, path)

		}()

	}
	wg.Wait()
}
