package common

type KeyValue struct {
	Key   string
	Value string
}

type Response struct {
	Message string
}

type MapStep struct {
	UseCase           string
	JobName           string
	File              string
	MapStepNumber     int
	NumberOfMapOutput int
	Path              string
	Column            string
}
type ReduceStep struct {
	UseCase          string
	JobName          string
	ReduceStepNumber int
	NumberOfFiles    int
	Path             string
}
