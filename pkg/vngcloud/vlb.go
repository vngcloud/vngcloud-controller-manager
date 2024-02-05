package vngcloud

import (
	lCtx "context"
	"fmt"

	lCoreV1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	lNetUtils "k8s.io/utils/net"

	lClient "github.com/vngcloud/vngcloud-go-sdk/client"
	lObjects "github.com/vngcloud/vngcloud-go-sdk/vngcloud/objects"
	lLoadBalancerV2 "github.com/vngcloud/vngcloud-go-sdk/vngcloud/services/loadbalancer/v2/loadbalancer"
	lPoolV2 "github.com/vngcloud/vngcloud-go-sdk/vngcloud/services/loadbalancer/v2/pool"

	lSdkClient "github.com/vngcloud/vngcloud-controller-manager/pkg/client"
	lConsts "github.com/vngcloud/vngcloud-controller-manager/pkg/consts"
	lMetrics "github.com/vngcloud/vngcloud-controller-manager/pkg/metrics"
	lUtils "github.com/vngcloud/vngcloud-controller-manager/pkg/utils"
	lErrors "github.com/vngcloud/vngcloud-controller-manager/pkg/utils/errors"
	lMetadata "github.com/vngcloud/vngcloud-controller-manager/pkg/utils/metadata"
)

type (
	// VLbOpts is default vLB configurations that are loaded from the vcontainer-ccm config file
	VLbOpts struct {
		DefaultL4PackageID               string `gcfg:"default-l4-package-id"`
		DefaultListenerAllowedCIDRs      string `gcfg:"default-listener-allowed-cidrs"`
		DefaultIdleTimeoutClient         int    `gcfg:"default-idle-timeout-client"`
		DefaultIdleTimeoutMember         int    `gcfg:"default-idle-timeout-member"`
		DefaultIdleTimeoutConnection     int    `gcfg:"default-idle-timeout-connection"`
		DefaultPoolAlgorithm             string `gcfg:"default-pool-algorithm"`
		DefaultMonitorHealthyThreshold   int    `gcfg:"default-monitor-healthy-threshold"`
		DefaultMonitorUnhealthyThreshold int    `gcfg:"default-monitor-unhealthy-threshold"`
		DefaultMonitorTimeout            int    `gcfg:"default-monitor-timeout"`
		DefaultMonitorInterval           int    `gcfg:"default-monitor-interval"`
		DefaultMonitorHttpMethod         string `gcfg:"default-monitor-http-method"`
		DefaultMonitorHttpPath           string `gcfg:"default-monitor-http-path"`
		DefaultMonitorHttpSuccessCode    string `gcfg:"default-monitor-http-success-code"`
		DefaultMonitorHttpVersion        string `gcfg:"default-monitor-http-version"`
		DefaultMonitorHttpDomainName     string `gcfg:"default-monitor-http-domain-name"`
		DefaultMonitorProtocol           string `gcfg:"default-monitor-protocol"`
	}

	// vLB is the implementation of the VNG CLOUD for actions on load balancer
	vLB struct {
		vLbSC     *lClient.ServiceClient
		vServerSC *lClient.ServiceClient

		kubeClient    kubernetes.Interface
		eventRecorder record.EventRecorder
		vLbConfig     VLbOpts
		extraInfo     *ExtraInfo
	}

	// Config is the configuration for the VNG CLOUD load balancer controller,
	// it is loaded from the vcontainer-ccm config file
	Config struct {
		Global   lSdkClient.AuthOpts // global configurations, it is loaded from Helm helpers and values.yaml
		VLB      VLbOpts             // vLB configurations, it is loaded from Helm helpers and values.yaml
		Metadata lMetadata.Opts      // metadata service config, by default is empty
	}
)

type (
	serviceConfig struct {
		internal                  bool
		lbID                      string
		preferredIPFamily         lCoreV1.IPFamily // preferred (the first) IP family indicated in service's `spec.ipFamilies`
		flavorID                  string
		lbType                    lLoadBalancerV2.CreateOptsTypeOpt
		projectID                 string
		subnetID                  string
		isOwner                   bool
		allowedCIRDs              string
		idleTimeoutClient         int
		idleTimeoutMember         int
		idleTimeoutConnection     int
		poolAlgorithm             string
		monitorHealthyThreshold   int
		monitorUnhealthyThreshold int
		monitorInterval           int
		monitorTimeout            int
		monitorHttpMethod         string
		monitorHttpPath           string
		monitorHttpSuccessCode    string
		monitorHttpVersion        string
		monitorHttpDomainName     string
		monitorProtocol           string
	}

	listenerKey struct {
		Protocol string
		Port     int
	}
)

// ****************************** IMPLEMENTATIONS OF KUBERNETES CLOUD PROVIDER INTERFACE *******************************

func (s *vLB) GetLoadBalancer(pCtx lCtx.Context, pClusterID string, pService *lCoreV1.Service) (*lCoreV1.LoadBalancerStatus, bool, error) {
	mc := lMetrics.NewMetricContext("loadbalancer", "ensure")
	klog.InfoS("GetLoadBalancer", "cluster", pClusterID, "service", klog.KObj(pService))
	status, existed, err := s.newController(pClusterID).EnsureGetLoadBalancer(pCtx, pClusterID, pService)
	return status, existed, mc.ObserveReconcile(err)
}

func (s *vLB) GetLoadBalancerName(_ lCtx.Context, pClusterID string, pService *lCoreV1.Service) string {
	return s.newController(pClusterID).GetLoadBalancerName(lCtx.Background(), pClusterID, pService)
}

func (s *vLB) EnsureLoadBalancer(
	pCtx lCtx.Context, pClusterID string, pService *lCoreV1.Service, pNodes []*lCoreV1.Node) (*lCoreV1.LoadBalancerStatus, error) {

	mc := lMetrics.NewMetricContext("loadbalancer", "ensure")
	klog.InfoS("EnsureLoadBalancer", "cluster", pClusterID, "service", klog.KObj(pService))
	status, err := s.newController(pClusterID).EnsureLoadBalancer(pCtx, pClusterID, pService, pNodes)
	return status, mc.ObserveReconcile(err)
}

// UpdateLoadBalancer updates hosts under the specified load balancer. This will be executed when user add or remove nodes
// from the cluster
func (s *vLB) UpdateLoadBalancer(pCtx lCtx.Context, pClusterID string, pService *lCoreV1.Service, pNodes []*lCoreV1.Node) error {
	klog.Infof("UpdateLoadBalancer: update load balancer for service %s/%s, the nodes are: %v",
		pService.Namespace, pService.Name, pNodes)

	mc := lMetrics.NewMetricContext("loadbalancer", "update-loadbalancer")
	klog.InfoS("UpdateLoadBalancer", "cluster", pClusterID, "service", klog.KObj(pService))
	err := s.newController(pClusterID).UpdateLoadBalancer(pCtx, pClusterID, pService, pNodes)
	return mc.ObserveReconcile(err)
}

func (s *vLB) EnsureLoadBalancerDeleted(pCtx lCtx.Context, pClusterID string, pService *lCoreV1.Service) error {
	mc := lMetrics.NewMetricContext("loadbalancer", "ensure")
	klog.InfoS("EnsureLoadBalancerDeleted", "cluster", pClusterID, "service", klog.KObj(pService))
	err := s.newController(pClusterID).EnsureLoadBalancerDeleted(pCtx, pClusterID, pService)
	return mc.ObserveReconcile(err)
}

// ************************************************** PRIVATE METHODS **************************************************
func (s *vLB) newController(pClusterID string) IController {
	controllerOnce.Do(func() {
		if len(pClusterID) > 0 {
			controller = newVKSController(
				s.vLbSC, s.vServerSC, s.kubeClient, s.eventRecorder, s.vLbConfig, s.extraInfo)
		} else {
			controller = newUserController(
				s.vLbSC, s.vServerSC, s.kubeClient, s.eventRecorder, s.vLbConfig, s.extraInfo)
		}
	})

	return controller
}

// ********************************************* DIRECTLY SUPPORT FUNCTIONS ********************************************

func nodeAddressForLB(node *lCoreV1.Node, preferredIPFamily lCoreV1.IPFamily) (string, error) {
	addrs := node.Status.Addresses
	if len(addrs) == 0 {
		return "", lErrors.NewErrNodeAddressNotFound(node.Name, "")
	}

	allowedAddrTypes := []lCoreV1.NodeAddressType{lCoreV1.NodeInternalIP, lCoreV1.NodeExternalIP}

	for _, allowedAddrType := range allowedAddrTypes {
		for _, addr := range addrs {
			if addr.Type == allowedAddrType {
				switch preferredIPFamily {
				case lCoreV1.IPv4Protocol:
					if lNetUtils.IsIPv4String(addr.Address) {
						return addr.Address, nil
					}
				case lCoreV1.IPv6Protocol:
					if lNetUtils.IsIPv6String(addr.Address) {
						return addr.Address, nil
					}
				default:
					return addr.Address, nil
				}
			}
		}
	}

	return "", lErrors.NewErrNodeAddressNotFound(node.Name, "")
}

func getNodeAddressForLB(pNode *lCoreV1.Node) (string, error) {
	addrs := pNode.Status.Addresses
	if len(addrs) == 0 {
		return "", lErrors.NewErrNodeAddressNotFound(pNode.Name, "")
	}

	for _, addr := range addrs {
		if addr.Type == lCoreV1.NodeInternalIP {
			return addr.Address, nil
		}
	}

	return addrs[0].Address, nil
}

func prepareMembers(pNodes []*lCoreV1.Node, pPort lCoreV1.ServicePort, pServiceConfig *serviceConfig) ( // params
	[]lPoolV2.Member, error) { // returns

	var poolMembers []lPoolV2.Member
	workerNodes := lUtils.ListWorkerNodes(pNodes, true)

	if len(workerNodes) < 1 {
		klog.Errorf("no worker node available for this cluster")
		return nil, lErrors.NewNoNodeAvailable()
	}

	for _, itemNode := range workerNodes {
		_, err := getNodeAddressForLB(itemNode)
		if err != nil {
			if lErrors.IsErrNodeAddressNotFound(err) {
				klog.Warningf("failed to get address for node %s: %v", itemNode.Name, err)
				continue
			} else {
				return nil, fmt.Errorf("failed to get address for node %s: %v", itemNode.Name, err)
			}
		}

		nodeAddress, err := nodeAddressForLB(itemNode, pServiceConfig.preferredIPFamily)
		if err != nil {
			if lErrors.IsErrNodeAddressNotFound(err) {
				klog.Warningf("failed to get address for node %s: %v", itemNode.Name, err)
				continue
			} else {
				return nil, fmt.Errorf("failed to get address for node %s: %v", itemNode.Name, err)
			}
		}

		// It's 0 when AllocateLoadBalancerNodePorts=False
		if pPort.NodePort != 0 {
			poolMembers = append(poolMembers, lPoolV2.Member{
				Backup:      lConsts.DEFAULT_MEMBER_BACKUP_ROLE,
				IpAddress:   nodeAddress,
				Port:        int(pPort.NodePort),
				Weight:      lConsts.DEFAULT_MEMBER_WEIGHT,
				MonitorPort: int(pPort.NodePort),
				Name:        itemNode.Name,
			})
		}
	}

	return poolMembers, nil
}

func isMemberChange(pPool *lObjects.Pool, pNodes []*lCoreV1.Node) bool {
	healthyWorkerNodes := lUtils.ListWorkerNodes(pNodes, true)
	if len(pPool.Members) != len(healthyWorkerNodes) {
		return true
	}

	memberMapping := make(map[string]bool)
	for _, member := range pPool.Members {
		memberMapping[member.Name] = true
	}

	for _, itemNode := range healthyWorkerNodes {
		if _, isPresent := memberMapping[itemNode.Name]; !isPresent {
			return true
		}
	}

	return false
}
