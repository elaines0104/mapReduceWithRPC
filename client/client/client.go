package client

import (
	"log"
	"net/rpc"
)

type Item struct {
	Body int
}

type Client struct {
	client *rpc.Client
}

func New(method string, url string) *Client {
	c, err := rpc.DialHTTP(method, url)
	if err != nil {
		return nil
	} else {
		return &Client{
			client: c,
		}
	}
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

func (c Client) HealthCheck(url string) {
	var reply Response

	c.client.Call("API.HealthCheck", url, &reply)
	log.Println(url, reply.Message)

}

func (c Client) DoMapStep(useCase string, jobName string, mapStepNumber int, file string, numberOfMapOutput int, path string, column string) {
	var reply Response
	item := MapStep{useCase, jobName, file, mapStepNumber, numberOfMapOutput, path, column}

	c.client.Call("API.DoMapStep", item, &reply)
	//log.Println(reply.Message)

}
func (c Client) DoReduceStep(useCase string, jobName string, reduceStepNumber int, numberOfFiles int, path string) {
	var reply Response
	item := ReduceStep{useCase, jobName, reduceStepNumber, numberOfFiles, path}

	c.client.Call("API.DoReduceStep", item, &reply)

	//log.Println(reply.Message)

}
