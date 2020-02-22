package lvmcs

import (
	"github.com/google/go-microservice-helpers/server"
	"github.com/google/go-microservice-helpers/tracing"
	pb "github.com/kubernetes-sigs/alibaba-cloud-csi-driver/pkg/yoda/lvmd/proto"
	"github.com/kubernetes-sigs/alibaba-cloud-csi-driver/pkg/yoda/lvmd/server"
	log "github.com/sirupsen/logrus"
	"os"
)

const (
	// LvmdPort is lvm daemon tcp port
	LvmdPort = "1736"
)

// Start start lvmd
func Start() {
	address := "0.0.0.0:" + GetLvmdPort()
	log.Infof("Lvmd Starting with socket: %s ...", address)

	err := tracing.InitTracer(address, "lvmd")
	if err != nil {
		log.Errorf("failed to init tracing interface: %v", err)
		return
	}

	svr := server.NewServer()
	serverhelpers.ListenAddress = &address
	grpcServer, _, err := serverhelpers.NewServer()
	if err != nil {
		log.Errorf("failed to init GRPC server: %v", err)
		return
	}

	pb.RegisterLVMServer(grpcServer, &svr)

	err = serverhelpers.ListenAndServe(grpcServer, nil)
	if err != nil {
		log.Errorf("failed to serve: %v", err)
		return
	}
	log.Infof("Lvmd End ...")
}

// GetLvmdPort get lvmd port
func GetLvmdPort() string {
	port := LvmdPort
	if value := os.Getenv("LVMD_PORT"); value != "" {
		port = value
	}
	return port
}
