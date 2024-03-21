package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"k8s.io/klog/v2"
	"strconv"
	lStr "strings"
	"time"

	lCoreV1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	clientset "k8s.io/client-go/kubernetes"

	lConsts "github.com/vngcloud/vngcloud-controller-manager/pkg/consts"

	lObjects "github.com/vngcloud/vngcloud-go-sdk/vngcloud/objects"
	lListenerV2 "github.com/vngcloud/vngcloud-go-sdk/vngcloud/services/loadbalancer/v2/listener"
	lLoadBalancerV2 "github.com/vngcloud/vngcloud-go-sdk/vngcloud/services/loadbalancer/v2/loadbalancer"
	lPoolV2 "github.com/vngcloud/vngcloud-go-sdk/vngcloud/services/loadbalancer/v2/pool"
)

type MyDuration struct {
	time.Duration
}

// PatchService makes patch request to the Service object.
func PatchService(ctx context.Context, client clientset.Interface, cur, mod *lCoreV1.Service) error {
	curJSON, err := json.Marshal(cur)
	if err != nil {
		return fmt.Errorf("failed to serialize current service object: %s", err)
	}

	modJSON, err := json.Marshal(mod)
	if err != nil {
		return fmt.Errorf("failed to serialize modified service object: %s", err)
	}

	patch, err := strategicpatch.CreateTwoWayMergePatch(curJSON, modJSON, lCoreV1.Service{})
	if err != nil {
		return fmt.Errorf("failed to create 2-way merge patch: %s", err)
	}

	if len(patch) == 0 || string(patch) == "{}" {
		return nil
	}

	_, err = client.CoreV1().Services(cur.Namespace).Patch(ctx, cur.Name, types.StrategicMergePatchType, patch, metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("failed to patch service object %s/%s: %s", cur.Namespace, cur.Name, err)
	}

	return nil
}

func IsPoolProtocolValid(pPool *lObjects.Pool, pPort lCoreV1.ServicePort, pPoolName string) bool {
	if pPool != nil &&
		lStr.EqualFold(lStr.TrimSpace(pPool.Protocol), lStr.TrimSpace(string(pPort.Protocol))) &&
		pPoolName == pPool.Name {
		return false
	}

	return true
}

func GenListenerAndPoolName(pClusterID string, pService *lCoreV1.Service, pPort lCoreV1.ServicePort) string {
	portSuffix := fmt.Sprintf("-%s-%d", pPort.Protocol, pPort.Port)
	serviceName := GenLoadBalancerName(pClusterID, pService)
	if lConsts.DEFAULT_PORTAL_NAME_LENGTH-len(serviceName)-len(portSuffix) >= 0 {
		return lStr.ToLower(serviceName + portSuffix)
	}

	delta := lConsts.DEFAULT_PORTAL_NAME_LENGTH - len(portSuffix)
	return lStr.ToLower(serviceName[:delta] + portSuffix)
}

/*
GenLoadBalancerName generates a load balancer name from a cluster ID and a service. The length of the name is limited to
50 characters. The format of the name is: clu-<cluster_id>-<service_name>_<namespace>.

PARAMS:
- pClusterID: cluster ID
- pService: service object
RETURN:
- load balancer name
*/
func GenLoadBalancerName(pClusterID string, pService *lCoreV1.Service) string {
	lbName := fmt.Sprintf(
		"%s-%s_%s",
		GenLoadBalancerPrefixName(pClusterID),
		pService.Namespace, pService.Name)
	return lStr.ToLower(lbName[:MinInt(len(lbName), lConsts.DEFAULT_PORTAL_NAME_LENGTH)])
}

func GenLoadBalancerPrefixName(pClusterID string) string {
	lbName := fmt.Sprintf(
		"%s-%s",
		lConsts.DEFAULT_LB_PREFIX_NAME,
		pClusterID[lConsts.DEFAULT_VLB_ID_PIECE_START_INDEX:lConsts.DEFAULT_VLB_ID_PIECE_START_INDEX+lConsts.DEFAULT_VLB_ID_PIECE_LENGTH])
	return lbName
}

func GenCompleteLoadBalancerName(pClusterID string, pService *lCoreV1.Service) string {
	genName := GenLoadBalancerName(pClusterID, pService)
	lbPrefixName := GenLoadBalancerPrefixName(pClusterID)
	lbName := GetStringFromServiceAnnotation(pService, lConsts.ServiceAnnotationLoadBalancerName, "")

	if len(lbName) > 0 {
		lbName = fmt.Sprintf("%s-%s", lbPrefixName, lbName)
		return lStr.ToLower(lbName[:MinInt(len(lbName), lConsts.DEFAULT_PORTAL_NAME_LENGTH)])
	} else {
		return genName
	}
}
func MinInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func ListWorkerNodes(pNodes []*lCoreV1.Node, pOnlyReadyNode bool) []*lCoreV1.Node {
	var workerNodes []*lCoreV1.Node

	for _, node := range pNodes {
		// Ignore master nodes
		if _, ok := node.GetObjectMeta().GetLabels()[lConsts.DEFAULT_K8S_MASTER_LABEL]; ok {
			continue
		}

		// If the status of node is not considered, add it to the list
		if !pOnlyReadyNode {
			workerNodes = append(workerNodes, node)
			continue
		}

		// If this worker does not have any condition, ignore it
		if len(node.Status.Conditions) < 1 {
			continue
		}

		// Only consider ready nodes
		for _, condition := range node.Status.Conditions {
			if condition.Type == lCoreV1.NodeReady && condition.Status != lCoreV1.ConditionTrue {
				continue
			}
		}

		// This is a truly well worker, add it to the list
		workerNodes = append(workerNodes, node)
	}

	return workerNodes
}

func CheckOwner(pCluster *lObjects.Cluster, pLb *lObjects.LoadBalancer, pService *lCoreV1.Service) bool {
	if pLb == nil {
		return true
	}

	if GenCompleteLoadBalancerName(pCluster.ID, pService) == pLb.Name {
		return true
	}

	return false
}

func ParsePoolAlgorithm(pOpt string) lPoolV2.CreateOptsAlgorithmOpt {
	opt := lStr.ReplaceAll(lStr.TrimSpace(lStr.ToUpper(pOpt)), "-", "_")
	switch opt {
	case string(lPoolV2.CreateOptsAlgorithmOptSourceIP):
		return lPoolV2.CreateOptsAlgorithmOptSourceIP
	case string(lPoolV2.CreateOptsAlgorithmOptLeastConn):
		return lPoolV2.CreateOptsAlgorithmOptLeastConn
	}
	return lPoolV2.CreateOptsAlgorithmOptRoundRobin
}

func ParsePoolProtocol(pPoolProtocol lCoreV1.Protocol) lPoolV2.CreateOptsProtocolOpt {
	opt := lStr.TrimSpace(lStr.ToUpper(string(pPoolProtocol)))
	switch opt {
	case string(lPoolV2.CreateOptsProtocolOptProxy):
		return lPoolV2.CreateOptsProtocolOptProxy
	case string(lPoolV2.CreateOptsProtocolOptHTTP):
		return lPoolV2.CreateOptsProtocolOptHTTP
	case string(lPoolV2.CreateOptsProtocolOptUDP):
		return lPoolV2.CreateOptsProtocolOptUDP
	}
	return lPoolV2.CreateOptsProtocolOptTCP
}

func ParseMonitorProtocol(
	pPoolProtocol lCoreV1.Protocol, pMonitorProtocol string) lPoolV2.CreateOptsHealthCheckProtocolOpt {

	switch lStr.TrimSpace(lStr.ToUpper(string(pPoolProtocol))) {
	case string(lPoolV2.CreateOptsProtocolOptUDP):
		return lPoolV2.CreateOptsHealthCheckProtocolOptPINGUDP
	}

	switch lStr.TrimSpace(lStr.ToUpper(pMonitorProtocol)) {
	case string(lPoolV2.CreateOptsHealthCheckProtocolOptHTTP):
		return lPoolV2.CreateOptsHealthCheckProtocolOptHTTP
	case string(lPoolV2.CreateOptsHealthCheckProtocolOptHTTPs):
		return lPoolV2.CreateOptsHealthCheckProtocolOptHTTPs
	case string(lPoolV2.CreateOptsHealthCheckProtocolOptPINGUDP):
		return lPoolV2.CreateOptsHealthCheckProtocolOptPINGUDP
	}

	return lPoolV2.CreateOptsHealthCheckProtocolOptTCP
}

func ParseMonitorHealthCheckMethod(pMethod string) *lPoolV2.CreateOptsHealthCheckMethodOpt {
	opt := lStr.TrimSpace(lStr.ToUpper(pMethod))
	switch opt {
	case string(lPoolV2.CreateOptsHealthCheckMethodOptPUT):
		tmp := lPoolV2.CreateOptsHealthCheckMethodOptPUT
		return &tmp
	case string(lPoolV2.CreateOptsHealthCheckMethodOptPOST):
		tmp := lPoolV2.CreateOptsHealthCheckMethodOptPOST
		return &tmp
	}

	tmp := lPoolV2.CreateOptsHealthCheckMethodOptGET
	return &tmp
}

func ParseMonitorHttpVersion(pVersion string) *lPoolV2.CreateOptsHealthCheckHttpVersionOpt {
	opt := lStr.TrimSpace(lStr.ToUpper(pVersion))
	switch opt {
	case string(lPoolV2.CreateOptsHealthCheckHttpVersionOptHttp1Minor1):
		tmp := lPoolV2.CreateOptsHealthCheckHttpVersionOptHttp1Minor1
		return &tmp
	}

	tmp := lPoolV2.CreateOptsHealthCheckHttpVersionOptHttp1
	return &tmp
}

func ParseLoadBalancerScheme(pInternal bool) lLoadBalancerV2.CreateOptsSchemeOpt {
	if pInternal {
		return lLoadBalancerV2.CreateOptsSchemeOptInternal
	}
	return lLoadBalancerV2.CreateOptsSchemeOptInternet
}

func ParseListenerProtocol(pPort lCoreV1.ServicePort) lListenerV2.CreateOptsListenerProtocolOpt {
	opt := lStr.TrimSpace(lStr.ToUpper(string(pPort.Protocol)))
	switch opt {
	case string(lListenerV2.CreateOptsListenerProtocolOptUDP):
		return lListenerV2.CreateOptsListenerProtocolOptUDP
	}

	return lListenerV2.CreateOptsListenerProtocolOptTCP
}

func GetStringFromServiceAnnotation(pService *lCoreV1.Service, annotationKey string, defaultSetting string) string {
	klog.V(4).Infof("getStringFromServiceAnnotation(%s/%s, %v, %v)", pService.Namespace, pService.Name, annotationKey, defaultSetting)
	if annotationValue, ok := pService.Annotations[annotationKey]; ok {
		//if there is an annotation for this setting, set the "setting" var to it
		// annotationValue can be empty, it is working as designed
		// it makes possible for instance provisioning loadbalancer without floatingip
		klog.V(4).Infof("Found a Service Annotation: %v = %v", annotationKey, annotationValue)
		return annotationValue
	}

	//if there is no annotation, set "settings" var to the value from cloud config
	if defaultSetting != "" {
		klog.V(4).Infof("Could not find a Service Annotation; falling back on cloud-config setting: %v = %v", annotationKey, defaultSetting)
	}

	return defaultSetting
}

func GetBoolFromServiceAnnotation(service *lCoreV1.Service, annotationKey string, defaultSetting bool) bool {
	klog.V(4).Infof("getBoolFromServiceAnnotation(%s/%s, %v, %v)", service.Namespace, service.Name, annotationKey, defaultSetting)
	if annotationValue, ok := service.Annotations[annotationKey]; ok {
		returnValue := false
		switch annotationValue {
		case "true":
			returnValue = true
		case "false":
			returnValue = false
		default:
			klog.Infof("Found a non-boolean Service Annotation: %v = %v (falling back to default setting: %v)", annotationKey, annotationValue, defaultSetting)
			return defaultSetting
		}

		klog.V(4).Infof("Found a Service Annotation: %v = %v", annotationKey, returnValue)
		return returnValue
	}
	klog.V(4).Infof("Could not find a Service Annotation; falling back to default setting: %v = %v", annotationKey, defaultSetting)
	return defaultSetting
}

func GetIntFromServiceAnnotation(service *lCoreV1.Service, annotationKey string, defaultSetting int) int {
	klog.V(4).Infof("getIntFromServiceAnnotation(%s/%s, %v, %v)", service.Namespace, service.Name, annotationKey, defaultSetting)
	if annotationValue, ok := service.Annotations[annotationKey]; ok {
		returnValue, err := strconv.Atoi(annotationValue)
		if err != nil {
			klog.Warningf("Could not parse int value from %q, failing back to default %s = %v, %v", annotationValue, annotationKey, defaultSetting, err)
			return defaultSetting
		}

		klog.V(4).Infof("Found a Service Annotation: %v = %v", annotationKey, annotationValue)
		return returnValue
	}
	klog.V(4).Infof("Could not find a Service Annotation; falling back to default setting: %v = %v", annotationKey, defaultSetting)
	return defaultSetting
}
