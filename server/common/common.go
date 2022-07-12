package common

import "strconv"

func MapOutputName(jobName string, mapTask int, reduceTask int) string {
	return jobName + "-mapOutput-" + strconv.Itoa(mapTask) + "-" + strconv.Itoa(reduceTask)
}

func ReduceOutputName(jobName string, reduceTask int) string {
	return jobName + "-reduceOutput-" + strconv.Itoa(reduceTask)
}
 
