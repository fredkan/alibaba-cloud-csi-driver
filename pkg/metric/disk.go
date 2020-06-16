package metric

import (
	log "github.com/sirupsen/logrus"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// URL is the address of cortex agent.
var (
	URL = "http://localhost:80/api/v1/query?query="
)

var Count int = 0

// Mutex is used to lock over metrics structure.
var Mutex = &sync.Mutex{}

func NewMetrics() PVMetrics {
	return PVMetrics{
		Queries: map[string]string{
			"iopsReadQuery":        "irate(openebs_reads[5m])",
			"iopsWriteQuery":       "irate(openebs_writes[5m])",
			"latencyReadQuery":     "((irate(openebs_read_time[5m]))/(irate(openebs_reads[5m])))/1000000",
			"latencyWriteQuery":    "((irate(openebs_write_time[5m]))/(irate(openebs_writes[5m])))/1000000",
			"throughputReadQuery":  "irate(openebs_read_block_count[5m])/(2048)",
			"throughputWriteQuery": "irate(openebs_write_block_count[5m])/(2048)",
		},
		PVList:    nil,
		Data:      nil,
		//ClientSet: NewClientSet(),
	}
}

// UpdateMetrics will update the metrics data and PV list
func (p *PVMetrics) UpdateMetrics() {
	for {
		p.UpdatePVMetrics()
		time.Sleep(60 * time.Second)
	}
}

// UpdatePVMetrics will update the PVMetrics struct object with the required data
func (p *PVMetrics) UpdatePVMetrics() {
	data := make(map[string]map[string]float64)
	for queryName, query := range p.Queries {
		pvMetricsvalue, err := p.GetMetrics(query)
		if err != nil {
			if Count < 5 {
				log.Error(err)
				Count = Count + 1
			}
		}

		if pvMetricsvalue == nil {
			data = nil
			log.Debugf("Failed to fetch metrics for %s", queryName)
			break
		}
		data[queryName] = pvMetricsvalue
	}

	if data != nil {
		Mutex.Lock()
		p.Data = data
		Mutex.Unlock()
		Count = 0
	}

	p.GetPVList()
}

// GetMetrics will return the metrics for the given query.
func (p *PVMetrics) GetMetrics(query string) (map[string]float64, error) {
	//response, err := http.Get(URL + query)
	//if err != nil {
	//	return nil, err
	//}
	//
	//responseBody, err := ioutil.ReadAll(response.Body)
	//if err != nil {
	//	return nil, err
	//}
	//
	//pvMetrics, err := p.UnmarshalResponse([]byte(responseBody))
	//if err != nil {
	//	return nil, err
	//}
	//
	//if len(pvMetrics.Data.Result) == 0 {
	//	return nil, errors.New("Result is empty")
	//}
	//
	//pvMetricsValue := make(map[string]float64)
	//for _, pvMetric := range pvMetrics.Data.Result {
	//	// For handling https://github.com/cortexproject/cortex/blob/1f75367734bd3fd7d106beea86f9901fd1e99750/vendor/github.com/prometheus/prometheus/promql/quantile.go#L64
	//	if pvMetric.Value[1].(string) == "NaN" || pvMetric.Value[1].(string) == "+Inf" || pvMetric.Value[1].(string) == "-Inf" {
	//		pvMetricsValue[pvMetric.Metric.OpenebsPv] = 0
	//	} else {
	//		metric, err := strconv.ParseFloat(pvMetric.Value[1].(string), 64)
	//		if err != nil {
	//			log.Error(err)
	//			pvMetricsValue[pvMetric.Metric.OpenebsPv] = 0
	//		} else {
	//			pvMetricsValue[pvMetric.Metric.OpenebsPv] = metric
	//		}
	//	}
	//}

	return nil, nil
}

// GetPVList fetch and update the list of PV.
func (p *PVMetrics) GetPVList() {
	pvList, err := p.ClientSet.CoreV1().PersistentVolumes().List(metav1.ListOptions{})
	if err != nil {
		log.Error(err)
		return
	}

	p.PVList = p.PVNameAndUID(pvList.Items)
}

// PVNameAndUID returns the name and UID of all the PVs.
func (p *PVMetrics) PVNameAndUID(pvListItems []corev1.PersistentVolume) map[string]string {
	pvList := make(map[string]string)
	for _, pv := range pvListItems {
		pvList[pv.GetName()] = string(pv.GetUID())
	}
	return pvList
}
//
//// UnmarshalResponse unmarshal the obtained Metric json.
//func (p *PVMetrics) UnmarshalResponse(response []byte) (*Metrics, error) {
//	metric := new(Metrics)
//	err := json.Unmarshal(response, &metric)
//	return metric, err
//}
//
//// NewClientSet will create a new InCluster config for k8s cluster
//func NewClientSet() kubernetes.Interface {
//	// creating in-cluster config.
//	config, err := rest.InClusterConfig()
//	if err != nil {
//		log.Error(err)
//		return nil
//	}
//
//	// create clientset of kubernetes.
//	clientSet, err := kubernetes.NewForConfig(config)
//	if err != nil {
//		log.Error(err)
//		return nil
//	}
//
//	return clientSet
//}
//
//func (p *PVMetrics) GetContainerCountInDeployment() int {
//	deploymentSpec, err := p.ClientSet.AppsV1().Deployments("maya-system").Get("openebs-monitor-plugin", metav1.GetOptions{})
//	if err != nil {
//		return 0
//	}
//	return len(deploymentSpec.Spec.Template.Spec.Containers)
//}
