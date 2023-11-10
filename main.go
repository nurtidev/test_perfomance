package main

import (
	"context"
	"fmt"
	"log"
	"mep4-default/database"
	helthPb "mep4-default/helth-proto"
	service "mep4-default/its-service"
	pb "mep4-default/proto"
	"mep4-default/utils"
	"net"
	"strconv"

	consulapi "github.com/hashicorp/consul/api"
	"google.golang.org/grpc"
)

var Port = utils.Getenv("SERVICE_PORT", "50051")
var ConsulAddr = utils.Getenv("CONSUL_ADDR", "localhost:8500")
var ClickhouseAddr = utils.Getenv("CLICKHOUSE_ADDR", "http://127.0.0.1:9000")
var ConsulServiceName = utils.Getenv("SERVICE_NAME", "Mep4Default")

type healthServer struct {
	helthPb.UnimplementedHealthServer
}

func (c *healthServer) Check(ctx context.Context, in *helthPb.HealthCheckRequest) (*helthPb.HealthCheckResponse, error) {
	//log.Println("check executed")
	return &helthPb.HealthCheckResponse{Status: helthPb.HealthCheckResponse_SERVING}, nil
}

func (c *healthServer) Watch(in *helthPb.HealthCheckRequest, hwS helthPb.Health_WatchServer) error {
	log.Println("Watch executed")
	return nil
}

func main() {
	log.Println("Starting service:")
	log.Println("Service Port " + Port)
	log.Println("Consul " + ConsulAddr)
	log.Println("Clickhouse " + ClickhouseAddr)

	lis, err := net.Listen("tcp", ":"+Port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer(grpc.MaxSendMsgSize(1024*1024*1024), grpc.MaxRecvMsgSize(1024*1024*1024))
	service := &service.Service{}
	pb.RegisterGreeterServer(s, service)
	log.Printf("Server listening at %v", lis.Addr())

	if err := database.ClInit(ClickhouseAddr); err != nil {
		log.Fatalf("Clickhouse conn error: %s", err)
	} else {
		log.Println("Clickhouse connected success")
	}

	// register health check
	s.RegisterService(&helthPb.Health_ServiceDesc, &healthServer{})
	serviceRegistryWithConsul()

	go service.Run()

	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

func serviceRegistryWithConsul() {
	config := consulapi.DefaultConfig()
	config.Address = ConsulAddr
	consul, err := consulapi.NewClient(config)
	if err != nil {
		log.Println(err)
	}

	//Address of current microservice
	address := utils.Getenv("SERVICE_ADDRESS", "172.27.232.22")
	//Hostname var only for docker
	serviceID := address + ":" + Port

	tags := []string{"urlprefix-/" + pb.Greeter_ServiceDesc.ServiceName + " proto=grpc"}

	intPort, _ := strconv.Atoi(Port)
	registration := &consulapi.AgentServiceRegistration{
		ID:      serviceID,
		Name:    ConsulServiceName,
		Port:    intPort,
		Tags:    tags,
		Address: address,
		Check: &consulapi.AgentServiceCheck{
			GRPC:     fmt.Sprintf("%s:%s", address, Port),
			Interval: "10s",
			Timeout:  "30s",
		},
	}

	regiErr := consul.Agent().ServiceRegister(registration)

	if regiErr != nil {
		log.Printf("Failed to register service: %s ", address)
	} else {
		log.Printf("successfully register service: %s", address)
	}
}
