package metric

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"net"
	"net/http"
)

func Run() {
	// Put socket in sub-directory to have more control on permissions
	const metricAddr = "0.0.0.0:32088"
	// Handle the exit signal
	listener, err := setupSocket(metricAddr)
	if err != nil {
		log.Error(err)
	}

	pvMetrics := NewMetrics()
	pvMetrics.GetPVList()
	//if count := pvMetrics.GetContainerCountInDeployment(); count < 2 {
	//	URL = "http://cortex-agent-service.maya-system.svc.cluster.local:80/api/v1/query?query="
	//}
	go pvMetrics.UpdateMetrics()
	http.HandleFunc("/metrics", pvMetrics.Report)
	if err := http.Serve(listener, nil); err != nil {
		log.Errorf("error: %v", err)
	}
}

// setupSocket will create a unix socket at the specified socket path
func setupSocket(address string) (net.Listener, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %q: %v", address, err)
	}
	log.Printf("Listening on: http://%s", address)
	return listener, nil
}
