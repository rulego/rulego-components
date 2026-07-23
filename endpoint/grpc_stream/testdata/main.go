// Server-side implementation
package main

import (
	"fmt"
	"net"
	pb "testdata/api/ble/v1"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type dataServer struct {
	pb.UnimplementedDataServiceServer
}

func (s *dataServer) StreamData(req *pb.StreamRequest, stream pb.DataService_StreamDataServer) error {
	// Simulating the transmission of different types of data
	for {
		// Transmit temperature data
		tempData := &pb.DataResponse{
			Type:      "temperature",
			Payload:   []byte(`{"value": 25.5, "unit": "C"}`),
			Timestamp: time.Now().Unix(),
		}
		if err := stream.Send(tempData); err != nil {
			return err
		}

		time.Sleep(time.Second)

		// Send humidity data
		humidityData := &pb.DataResponse{
			Type:      "humidity",
			Payload:   []byte(`{"value": 60, "unit": "%"}`),
			Timestamp: time.Now().Unix(),
		}
		if err := stream.Send(humidityData); err != nil {
			return err
		}

		time.Sleep(time.Second)
	}
}

func main() {
	lis, err := net.Listen("tcp", ":9000")
	if err != nil {
		panic(err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterDataServiceServer(grpcServer, &dataServer{})

	// Register for the reflection service - add this line
	reflection.Register(grpcServer)

	fmt.Println("Server starting at :9000")
	if err := grpcServer.Serve(lis); err != nil {
		panic(err)
	}
}
