package main

import (
	"log"
	"map-reduce-server/common"
	"map-reduce-server/mapper"
	"map-reduce-server/reducer"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type API int

func (a *API) DoMapStep(item common.MapStep, reply *common.Response) error {
	log.Printf("DoMapStep: %d", item.MapStepNumber)
	err := mapper.DoMapStep(item)
	if err != nil {
		log.Printf("ERROR")
		reply.Message = "error"
		return err

	}
	reply.Message = "OK"

	return nil

}

func (a *API) DoReduceStep(item common.ReduceStep, reply *common.Response) error {
	log.Println("DoReduceStep: ", item.ReduceStepNumber)
	err := reducer.DoReduceStep(item)

	if err != nil {
		log.Printf("ERROR")
		reply.Message = "error"
		return err

	}
	reply.Message = "OK"

	return nil
}
func (a *API) HealthCheck(url string, reply *common.Response) error {
	log.Println("HealthCheck: ", url)
	reply.Message = "OK"
	return nil
}

func main() {
	port := ":" + os.Args[1]

	var api = new(API)
	err := rpc.Register(api)
	if err != nil {
		log.Fatal("error registering API", err)
	}
	rpc.HandleHTTP()

	listener, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatal("listener error", err)
	}

	log.Printf("Server: serving rpc on port %s", port)

	err = http.Serve(listener, nil)
	if err != nil {
		log.Fatal("error serving: ", err)
	}
}
