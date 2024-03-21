package vngcloud

import (
	lCtx "context"
	"fmt"
	lStr "strings"
	"time"

	lCoreV1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	lNetUtils "k8s.io/utils/net"

	lClient "github.com/vngcloud/vngcloud-go-sdk/client"
	lObjects "github.com/vngcloud/vngcloud-go-sdk/vngcloud/objects"
	lClusterV2 "github.com/vngcloud/vngcloud-go-sdk/vngcloud/services/coe/v2/cluster"
	lListenerV2 "github.com/vngcloud/vngcloud-go-sdk/vngcloud/services/loadbalancer/v2/listener"
	lLoadBalancerV2 "github.com/vngcloud/vngcloud-go-sdk/vngcloud/services/loadbalancer/v2/loadbalancer"
	lPoolV2 "github.com/vngcloud/vngcloud-go-sdk/vngcloud/services/loadbalancer/v2/pool"
	lSecgroupV2 "github.com/vngcloud/vngcloud-go-sdk/vngcloud/services/network/v2/secgroup"

	lSdkClient "github.com/vngcloud/vngcloud-controller-manager/pkg/client"
	lConsts "github.com/vngcloud/vngcloud-controller-manager/pkg/consts"
	lMetrics "github.com/vngcloud/vngcloud-controller-manager/pkg/metrics"
	lUtils "github.com/vngcloud/vngcloud-controller-manager/pkg/utils"
	lErrors "github.com/vngcloud/vngcloud-controller-manager/pkg/utils/errors"
	lMetadata "github.com/vngcloud/vngcloud-controller-manager/pkg/utils/metadata"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/informers"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
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
		trackLBUpdate *UpdateTracker
		clusterID     string

		serviceLister       corelisters.ServiceLister
		serviceListerSynced cache.InformerSynced
		nodeLister          corelisters.NodeLister
		nodeListerSynced    cache.InformerSynced
		stopCh              chan struct{}
		informer            informers.SharedInformerFactory
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
		// projectID                 string
		subnetID                  string
		// isOwner                   bool
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

func (s *vLB) Init() {
	kubeInformerFactory := informers.NewSharedInformerFactory(s.kubeClient, time.Second*30)
	serviceInformer := kubeInformerFactory.Core().V1().Services()
	nodeInformer := kubeInformerFactory.Core().V1().Nodes()

	s.nodeLister = nodeInformer.Lister()
	s.nodeListerSynced = nodeInformer.Informer().HasSynced
	s.serviceLister = serviceInformer.Lister()
	s.serviceListerSynced = serviceInformer.Informer().HasSynced
	s.stopCh = make(chan struct{})
	s.informer = kubeInformerFactory

	defer close(s.stopCh)
	go s.informer.Start(s.stopCh)

	// wait for the caches to synchronize before starting the worker
	if !cache.WaitForCacheSync(s.stopCh, s.serviceListerSynced, s.nodeListerSynced) {
		utilruntime.HandleError(fmt.Errorf("timed out waiting for caches to sync"))
		return
	}

	go wait.Until(s.nodeSyncLoop, 60*time.Second, s.stopCh)
	<-s.stopCh
}

// ****************************** IMPLEMENTATIONS OF KUBERNETES CLOUD PROVIDER INTERFACE *******************************

func (s *vLB) GetLoadBalancer(pCtx lCtx.Context, pClusterID string, pService *lCoreV1.Service) (*lCoreV1.LoadBalancerStatus, bool, error) {
	s.clusterID = pClusterID
	mc := lMetrics.NewMetricContext("loadbalancer", "ensure")
	klog.InfoS("GetLoadBalancer", "cluster", pClusterID, "service", klog.KObj(pService))
	status, existed, err := s.ensureGetLoadBalancer(pCtx, pClusterID, pService)
	return status, existed, mc.ObserveReconcile(err)
}

func (s *vLB) GetLoadBalancerName(_ lCtx.Context, pClusterID string, pService *lCoreV1.Service) string {
	s.clusterID = pClusterID
	return lUtils.GenCompleteLoadBalancerName(pClusterID, pService)
}

func (s *vLB) EnsureLoadBalancer(
	pCtx lCtx.Context, pClusterID string, pService *lCoreV1.Service, pNodes []*lCoreV1.Node) (*lCoreV1.LoadBalancerStatus, error) {
	s.clusterID = pClusterID
	mc := lMetrics.NewMetricContext("loadbalancer", "ensure")
	klog.InfoS("EnsureLoadBalancer", "cluster", pClusterID, "service", klog.KObj(pService))
	status, err := s.ensureLoadBalancer(pCtx, pClusterID, pService, pNodes)
	return status, mc.ObserveReconcile(err)
}

// UpdateLoadBalancer updates hosts under the specified load balancer. This will be executed when user add or remove nodes
// from the cluster
func (s *vLB) UpdateLoadBalancer(pCtx lCtx.Context, pClusterID string, pService *lCoreV1.Service, pNodes []*lCoreV1.Node) error {
	klog.Infof("UpdateLoadBalancer: update load balancer for service %s/%s, the nodes are: %v",
		pService.Namespace, pService.Name, pNodes)
	s.clusterID = pClusterID
	mc := lMetrics.NewMetricContext("loadbalancer", "update-loadbalancer")
	klog.InfoS("UpdateLoadBalancer", "cluster", pClusterID, "service", klog.KObj(pService))
	err := s.ensureUpdateLoadBalancer(pCtx, pClusterID, pService, pNodes)
	return mc.ObserveReconcile(err)
}

func (s *vLB) EnsureLoadBalancerDeleted(pCtx lCtx.Context, pClusterID string, pService *lCoreV1.Service) error {
	s.clusterID = pClusterID
	mc := lMetrics.NewMetricContext("loadbalancer", "ensure")
	klog.InfoS("EnsureLoadBalancerDeleted", "cluster", pClusterID, "service", klog.KObj(pService))
	err := s.ensureDeleteLoadBalancer(pCtx, pClusterID, pService)
	return mc.ObserveReconcile(err)
}

// ************************************************** PRIVATE METHODS **************************************************

func (s *vLB) ensureLoadBalancer(
	pCtx lCtx.Context, pClusterID string, pService *lCoreV1.Service, pNodes []*lCoreV1.Node) ( // params
	rLb *lCoreV1.LoadBalancerStatus, rErr error) { // returns

	var (
		userLb               *lObjects.LoadBalancer   // hold the loadbalancer that user want to reuse or create
		lsLbs                []*lObjects.LoadBalancer // hold the list of loadbalancer existed in the project of this cluster
		lbListeners          []*lObjects.Listener     // hold the list of listeners of the loadbalancer attach to this cluster
		createNewLb, isOwner bool                     // check the loadbalancer is created and this cluster is the owner

		svcConf            = new(serviceConfig)                                // the lb configurations CAN be applied to create or update
		curListenerMapping = make(map[listenerKey]*lObjects.Listener)          // this use to check conflict port and protocol
		lbName             = s.GetLoadBalancerName(pCtx, pClusterID, pService) // hold the loadbalancer name
	)

	// Patcher the service to prevent the service is updated by other controller
	patcher := newServicePatcher(s.kubeClient, pService)
	defer func() {
		rErr = patcher.Patch(pCtx, rErr)
	}()

	// Get the cluster info from the cluster ID that user provided from the K8s secret
	userCluster, err := lClusterV2.Get(s.vServerSC, lClusterV2.NewGetOpts(s.getProjectID(), pClusterID))
	if err != nil || userCluster == nil {
		klog.Warningf("cluster %s NOT found, please check the secret resource in the Helm template", pClusterID)
		return nil, err
	}

	// Set the default configurations for loadbalancer into the 'svcConf' variable
	if err = s.configureLoadBalancerParams(pService, pNodes, svcConf, userCluster); err != nil {
		klog.Errorf("failed to configure load balancer params for service %s/%s: %v", pService.Namespace, pService.Name, err)
		return nil, err
	}

	if len(svcConf.lbID) > 0 {
		// User uses Service Annotation to reuse the load balancer
		userLb, err = lLoadBalancerV2.Get(s.vLbSC, lLoadBalancerV2.NewGetOpts(s.getProjectID(), svcConf.lbID))
		if err != nil || userLb == nil {
			klog.Errorf("failed to get load balancer %s for service %s/%s: %v", svcConf.lbID, pService.Namespace, pService.Name, err)
			return nil, err
		}

		if svcConf.internal != userLb.Internal {
			klog.Errorf("the loadbalancer type of the original loadbalancer and service file are not match")
			return nil, lErrors.NewErrConflictServiceAndCloud("the loadbalancer type of the original loadbalancer and service file are not match")
		}

		// createNewLb = false
	} else {
		// User want you to create a new loadbalancer for this service
		klog.V(5).Infof("did not specify load balancer ID, maybe creating a new one")
		// isOwner = true
		// createNewLb = true
	}

	// If until this step, can not find any load balancer for this cluster, find entire the project
	if lsLbs, err = lLoadBalancerV2.List(s.vLbSC, lLoadBalancerV2.NewListOpts(s.getProjectID())); err != nil {
		klog.Errorf("failed to find load balancers for cluster %s: %v", pClusterID, err)
		return nil, err
	}

	// Find the loadbalancer from the list of loadbalancer that we got from the project,
	// make sure they are on the same subnet
	userLb = s.findLoadBalancer(lbName, svcConf.lbID, lsLbs, userCluster)
	isOwner = lUtils.CheckOwner(userCluster, userLb, pService)
	createNewLb = userLb == nil

	if isOwner && createNewLb {
		// The loadbalancer is created and this cluster is the owner
		if userLb, err = s.createLoadBalancer(lbName, pService, svcConf); err != nil {
			klog.Errorf("failed to create load balancer for service %s/%s: %v", pService.Namespace, pService.Name, err)
			return nil, err
		}
	}

	s.trackLBUpdate.AddUpdateTracker(userLb.UUID, fmt.Sprintf("%s/%s", pService.Namespace, pService.Name), userLb.UpdatedAt)

	// Check ports are conflict or not
	if lbListeners, err = lListenerV2.GetBasedLoadBalancer(
		s.vLbSC, lListenerV2.NewGetBasedLoadBalancerOpts(s.getProjectID(), userLb.UUID)); err != nil {
		klog.Errorf("failed to get listeners for load balancer %s: %v", userLb.UUID, err)
		return nil, err
	}

	// If this loadbalancer has some listeners
	if len(lbListeners) > 0 {
		// Loop via the listeners of this loadbalancer to get the mapping of port and protocol
		for i, itemListener := range lbListeners {
			key := listenerKey{Protocol: itemListener.Protocol, Port: itemListener.ProtocolPort}
			curListenerMapping[key] = lbListeners[i]
		}

		// Check the port and protocol conflict on the listeners of this loadbalancer
		if err = s.checkListeners(pService, curListenerMapping, userCluster, userLb); err != nil {
			klog.Errorf("the port and protocol is conflict: %v", err)
			return nil, err
		}
	}

	// Ensure pools and listener for this loadbalancer
	klog.V(5).Infof("processing listeners and pools")
	for _, port := range pService.Spec.Ports {
		// Ensure pools
		newPool, err := s.ensurePool(userCluster, userLb, pService, port, pNodes, svcConf)
		if err != nil {
			klog.Errorf("failed to create pool for load balancer %s: %v", userLb.UUID, err)
			return nil, err
		}

		// Ensure listners because of this pool change
		_, err = s.ensureListener(userCluster, userLb.UUID, newPool.UUID, lbName, port, pService, svcConf)
		if err != nil {
			klog.Errorf("failed to create listener for load balancer %s: %v", userLb.UUID, err)
			return nil, err
		}

		klog.Infof(
			"processed listener and pool with port [%d] and protocol [%s] for load balancer %s successfully", port.Port, port.Protocol, userLb.UUID)
	}
	klog.V(5).Infof("processing listeners and pools completely")
	if lUtils.GetBoolFromServiceAnnotation(pService, lConsts.ServiceAnnotationEnableSecgroupDefault, true) {
		klog.V(5).Infof("processing security group")
		// Update the security group
		if err = s.ensureSecgroup(userCluster); err != nil {
			klog.Errorf("failed to update security group for load balancer %s: %v", userLb.UUID, err)
			return nil, err
		}
	}

	klog.V(5).Infof("processing load balancer status")
	lbStatus := s.createLoadBalancerStatus(userLb.Address)

	klog.Infof(
		"load balancer %s for service %s/%s is ready to use for Kubernetes controller",
		lbName, pService.Namespace, pService.Name)
	return lbStatus, nil
}

func (s *vLB) createLoadBalancer(pLbName string, pService *lCoreV1.Service, pServiceConfig *serviceConfig) ( // params
	*lObjects.LoadBalancer, error) { // returns

	opts := &lLoadBalancerV2.CreateOpts{
		Scheme:    lUtils.ParseLoadBalancerScheme(pServiceConfig.internal),
		Type:      pServiceConfig.lbType,
		Name:      pLbName,
		PackageID: pServiceConfig.flavorID,
		SubnetID:  pServiceConfig.subnetID,
	}

	klog.Infof("creating load balancer %s for service %s", pLbName, pService.Name)
	mc := lMetrics.NewMetricContext("loadbalancer", "create")
	newLb, err := lLoadBalancerV2.Create(s.vLbSC, lLoadBalancerV2.NewCreateOpts(s.extraInfo.ProjectID, opts))

	if mc.ObserveReconcile(err) != nil {
		klog.Errorf("failed to create load balancer %s for service %s: %v", pLbName, pService.Name, err)
		return nil, err
	}

	klog.Infof("created load balancer %s for service %s, waiting for ready", newLb.UUID, pService.Name)
	newLb, err = s.waitLoadBalancerReady(newLb.UUID)
	if err != nil {
		klog.Errorf("failed to wait load balancer %s for service %s, now try to delete it: %v", newLb.UUID, pService.Name, err)
		// delete this loadbalancer
		if err2 := lLoadBalancerV2.Delete(s.vLbSC, lLoadBalancerV2.NewDeleteOpts(s.extraInfo.ProjectID, newLb.UUID)); err2 != nil {
			klog.Errorf("failed to delete load balancer %s for service %s after creating timeout: %v", newLb.UUID, pService.Name, err2)
			return nil, fmt.Errorf("failed to delete load balancer %s for service %s after creating timeout: %v", newLb.UUID, pService.Name, err2)
		}

		return nil, err
	}

	klog.Infof("created load balancer %s for service %s successfully", newLb.UUID, pService.Name)
	return newLb, nil
}

func (s *vLB) waitLoadBalancerReady(pLbID string) (*lObjects.LoadBalancer, error) {

	klog.Infof("Waiting for load balancer %s to be ready", pLbID)
	var resultLb *lObjects.LoadBalancer

	err := wait.ExponentialBackoff(wait.Backoff{
		Duration: waitLoadbalancerInitDelay,
		Factor:   waitLoadbalancerFactor,
		Steps:    waitLoadbalancerActiveSteps,
	}, func() (done bool, err error) {
		mc := lMetrics.NewMetricContext("loadbalancer", "get")
		lb, err := lLoadBalancerV2.Get(s.vLbSC, lLoadBalancerV2.NewGetOpts(s.getProjectID(), pLbID))
		if mc.ObserveReconcile(err) != nil {
			klog.Errorf("failed to get load balancer %s: %v", pLbID, err)
			return false, err
		}

		if lStr.ToUpper(lb.Status) == ACTIVE_LOADBALANCER_STATUS {
			klog.Infof("Load balancer %s is ready", pLbID)
			resultLb = lb
			return true, nil
		}

		klog.Infof("Load balancer %s is not ready yet, wating...", pLbID)
		return false, nil
	})

	if wait.Interrupted(err) {
		err = fmt.Errorf("timeout waiting for the loadbalancer %s with lb status %s", pLbID, resultLb.Status)
	}

	return resultLb, err
}

func (s *vLB) ensurePool(
	pCluster *lObjects.Cluster, pLb *lObjects.LoadBalancer, pService *lCoreV1.Service, pPort lCoreV1.ServicePort,
	pNodes []*lCoreV1.Node, pServiceConfig *serviceConfig) (*lObjects.Pool, error) {

	// Get the pool name of the service

	var (
		userPool *lObjects.Pool

		poolName = lUtils.GenListenerAndPoolName(pCluster.ID, pService, pPort)
	)

	// Get all pools of this loadbalancer based on the load balancer uuid
	pools, err := lPoolV2.ListPoolsBasedLoadBalancer(
		s.vLbSC, lPoolV2.NewListPoolsBasedLoadBalancerOpts(s.extraInfo.ProjectID, pLb.UUID))

	if err != nil {
		klog.Errorf("failed to list pools for load balancer %s: %v", pLb.UUID, err)
		return nil, err
	}

	// Loop via pool
	for _, itemPool := range pools {
		if itemPool.Name == poolName {
			userPool = itemPool
		}

		// If this pool is not valid
		if !lUtils.IsPoolProtocolValid(itemPool, pPort, poolName) {
			err = lPoolV2.Delete(s.vLbSC, lPoolV2.NewDeleteOpts(s.extraInfo.ProjectID, pLb.UUID, itemPool.UUID))
			if err != nil {
				klog.Errorf("failed to delete pool %s for service %s: %v", itemPool.Name, pService.Name, err)
				return nil, err
			}

			userPool = nil
			break
		}
	}

	_, err = s.waitLoadBalancerReady(pLb.UUID)
	if err != nil {
		klog.Errorf("failed to wait load balancer %s for service %s: %v", pLb.UUID, pService.Name, err)
		return nil, err
	}

	if userPool == nil {
		poolMembers, err := prepareMembers(pNodes, pPort, pServiceConfig)
		if err != nil {
			klog.Errorf("failed to prepare members for pool %s: %v", pService.Name, err)
			return nil, err
		}

		poolProtocol := lUtils.ParsePoolProtocol(pPort.Protocol)
		userPool, err = lPoolV2.Create(s.vLbSC, lPoolV2.NewCreateOpts(s.extraInfo.ProjectID, pLb.UUID, &lPoolV2.CreateOpts{
			Algorithm:    lUtils.ParsePoolAlgorithm(pServiceConfig.poolAlgorithm),
			PoolName:     poolName,
			PoolProtocol: poolProtocol,
			Members:      poolMembers,
			HealthMonitor: lPoolV2.HealthMonitor{
				HealthCheckProtocol: lUtils.ParseMonitorProtocol(pPort.Protocol, pServiceConfig.monitorProtocol),
				HealthCheckMethod:   lUtils.ParseMonitorHealthCheckMethod(pServiceConfig.monitorHttpMethod),
				HttpVersion:         lUtils.ParseMonitorHttpVersion(pServiceConfig.monitorHttpVersion),
				HealthyThreshold:    pServiceConfig.monitorHealthyThreshold,
				UnhealthyThreshold:  pServiceConfig.monitorUnhealthyThreshold,
				Timeout:             pServiceConfig.monitorTimeout,
				Interval:            pServiceConfig.monitorInterval,
				HealthCheckPath:     &pServiceConfig.monitorHttpPath,
				DomainName:          &pServiceConfig.monitorHttpDomainName,
				SuccessCode:         &pServiceConfig.monitorHttpSuccessCode,
			},
		}))

		klog.Infof("Created pool with success code %s for service, waiting the loadbalancer update completely", pServiceConfig.monitorHttpSuccessCode)

		if err != nil {
			klog.Errorf("failed to create pool %s for service %s: %v", pService.Name, pService.Name, err)
			return nil, err
		}

		klog.Infof("Created pool %s for service %s, waiting the loadbalancer update completely", pService.Name, pService.Name)
	} else {
		klog.Infof("Pool %s for service %s existed, check members", pService.Name, pService.Name)
		poolChange := isMemberChange(userPool, pNodes)
		if poolChange {
			poolMembers, err := prepareMembers(pNodes, pPort, pServiceConfig)
			if err != nil {
				klog.Errorf("failed to prepare members for pool %s: %v", pService.Name, err)
				return nil, err
			}

			if err = lPoolV2.UpdatePoolMembers(s.vLbSC, lPoolV2.NewUpdatePoolMembersOpts(
				s.extraInfo.ProjectID, pLb.UUID, userPool.UUID, &lPoolV2.UpdatePoolMembersOpts{
					Members: poolMembers,
				})); err != nil {

				if lPoolV2.IsErrPoolMemberUnchanged(err) {
					klog.Infof("pool %s for service %s has no change", userPool.Name, pService.Name)
					return userPool, nil
				}

				klog.Errorf("failed to update pool %s for service %s: %v", pService.Name, pService.Name, err)
				return nil, err
			}
		} else {
			klog.Infof("pool %s for service %s has no change", userPool.Name, pService.Name)
			return userPool, nil
		}
	}

	_, err = s.waitLoadBalancerReady(pLb.UUID)
	if err != nil {
		klog.Errorf("failed to wait load balancer %s for service %s: %v", pLb.UUID, pService.Name, err)
		return nil, err
	}

	return userPool, nil
}

func (s *vLB) ensureListener(
	pCluster *lObjects.Cluster, pLbID, pPoolID, pLbName string, pPort lCoreV1.ServicePort, // params
	pService *lCoreV1.Service, pServiceConfig *serviceConfig) ( // params
	*lObjects.Listener, error) { // returns

	var userListener *lObjects.Listener = nil
	listenerName := lUtils.GenListenerAndPoolName(pCluster.ID, pService, pPort)
	listeners, err := lListenerV2.GetBasedLoadBalancer(s.vLbSC, lListenerV2.NewGetBasedLoadBalancerOpts(s.extraInfo.ProjectID, pLbID))
	if err != nil {
		klog.Errorf("failed to list listeners for load balancer %s: %v", pLbID, err)
		return nil, err
	}

	for _, itemListener := range listeners {
		if itemListener.Name == listenerName {
			userListener = itemListener
			break
		}
	}

	if userListener == nil {
		mc := lMetrics.NewMetricContext("listener", "create")
		userListener, err = lListenerV2.Create(s.vLbSC, lListenerV2.NewCreateOpts(
			s.extraInfo.ProjectID,
			pLbID,
			&lListenerV2.CreateOpts{
				AllowedCidrs:         pServiceConfig.allowedCIRDs,
				DefaultPoolId:        pPoolID,
				ListenerName:         lUtils.GenListenerAndPoolName(pCluster.ID, pService, pPort),
				ListenerProtocol:     lUtils.ParseListenerProtocol(pPort),
				ListenerProtocolPort: int(pPort.Port),
				TimeoutClient:        pServiceConfig.idleTimeoutClient,
				TimeoutMember:        pServiceConfig.idleTimeoutMember,
				TimeoutConnection:    pServiceConfig.idleTimeoutConnection,
			},
		))

		if mc.ObserveReconcile(err) != nil {
			klog.Errorf("failed to create listener %s: %v", pLbName, err)
			return nil, err
		}

	}

	_, err = s.waitLoadBalancerReady(pLbID)
	if err != nil {
		klog.Errorf("failed to wait load balancer %s for service %s: %v", pLbID, pService.Name, err)
		return nil, err
	}

	return userListener, nil
}

func (s *vLB) createLoadBalancerStatus(addr string) *lCoreV1.LoadBalancerStatus {
	status := &lCoreV1.LoadBalancerStatus{}
	// Default to IP
	status.Ingress = []lCoreV1.LoadBalancerIngress{{IP: addr}}
	return status
}

func (s *vLB) getProjectID() string {
	return s.extraInfo.ProjectID
}

func (s *vLB) ensureDeleteLoadBalancer(pCtx lCtx.Context, pClusterID string, pService *lCoreV1.Service) error {

	var (
		userLb *lObjects.LoadBalancer // hold the loadbalancer that user want to reuse or create

		lbID = lUtils.GetStringFromServiceAnnotation(pService, lConsts.ServiceAnnotationLoadBalancerID, "")
	)

	// Get the cluster info from the cluster ID that user provided from the K8s secret
	userCluster, err := lClusterV2.Get(s.vServerSC, lClusterV2.NewGetOpts(s.getProjectID(), pClusterID))
	if lClusterV2.IsErrClusterNotFound(err) {
		klog.Warningf("cluster %s not found, please check the secret resource in the Helm template", pClusterID)
		return err
	}

	if len(lbID) > 0 {
		if userLb, err = lLoadBalancerV2.Get(s.vLbSC, lLoadBalancerV2.NewGetOpts(s.getProjectID(), lbID)); err != nil {
			klog.Errorf("failed to get load balancer %s for service %s/%s when try to delete this loadbalancer: %v",
				lbID, pService.Namespace, pService.Name, err)
			return err
		}
	} else {
		// Get this loadbalancer name
		userLbs, err := lLoadBalancerV2.List(s.vLbSC, lLoadBalancerV2.NewListOpts(s.getProjectID()))
		if err != nil {
			klog.Warningf("failed to list load balancers for cluster %s in the subnet %s: %v", pClusterID, userCluster.SubnetID, err)
			return err
		}

		if userLbs == nil || len(userLbs) < 1 {
			klog.Infof("no load balancer found for cluster %s", pClusterID)
			return nil
		}

		// Get the loadbalancer by subnetID and loadbalancer name
		lbName := s.GetLoadBalancerName(pCtx, pClusterID, pService)
		userLb = s.findLoadBalancer(lbName, lbID, userLbs, userCluster)
	}

	if userLb != nil {
		// Remove the update tracker
		s.trackLBUpdate.RemoveUpdateTracker(userLb.UUID, fmt.Sprintf("%s/%s", pService.Namespace, pService.Name))

		canDelete := s.canDeleteThisLoadBalancer(userLb, userCluster, pService)
		if !canDelete {
			klog.Infof(
				"the loadbalancer %s is not owned by cluster %s or is used by other services, will delete these cluster listeners", userLb.UUID, pClusterID)

			// Get all listeners of this loadbalancer
			lbListeners, err := lListenerV2.GetBasedLoadBalancer(s.vLbSC, lListenerV2.NewGetBasedLoadBalancerOpts(s.getProjectID(), userLb.UUID))
			if err != nil {
				klog.Errorf("can not list listeners of loadbalancer %s to delete: %v", userLb.UUID, err)
				return err
			}

			// Check if this loadbalancer has no listeners
			if lbListeners == nil || len(lbListeners) < 1 {
				klog.Infof("the loadbalancer %s has no listeners", userLb.UUID)
				return nil
			}

			// Delete all listeners of this loadbalancer
			for _, itemListener := range lbListeners {
				if err := s.deleteListenersAndPools(userCluster, itemListener, userLb, pService); err != nil {
					klog.Errorf("failed to delete listener and pool %s for loadbalancer %s: %v", itemListener.UUID, userLb.UUID, err)
					return err
				}
			}

			return nil
		}

		// Delete this loadbalancer
		klog.V(5).Infof("deleting load balancer [UUID:%s]", userLb.UUID)
		err := lLoadBalancerV2.Delete(s.vLbSC, lLoadBalancerV2.NewDeleteOpts(s.getProjectID(), userLb.UUID))
		if err != nil {
			klog.Errorf("failed to delete load balancer [UUID:%s]: %v", userLb.UUID, err)
			return err
		}

		klog.V(5).Infof("deleted load balancer [UUID:%s] successfully", userLb.UUID)
	}

	// The loadbalancer has been deleted completely
	return nil
}

func (s *vLB) ensureGetLoadBalancer(pCtx lCtx.Context, pClusterID string, pService *lCoreV1.Service) (*lCoreV1.LoadBalancerStatus, bool, error) {

	// Get the loadbalancer UUID
	lbID := lUtils.GetStringFromServiceAnnotation(pService, lConsts.ServiceAnnotationLoadBalancerID, "")

	// Get the cluster info from the cluster ID that user provided from the K8s secret
	userCluster, err := lClusterV2.Get(s.vServerSC, lClusterV2.NewGetOpts(s.getProjectID(), pClusterID))
	if lClusterV2.IsErrClusterNotFound(err) {
		klog.Warningf("cluster %s not found, please check the secret resource in the Helm template", pClusterID)
		return nil, false, err
	}

	// Get this loadbalancer name
	userLbs, err := lLoadBalancerV2.List(s.vLbSC, lLoadBalancerV2.NewListOpts(s.getProjectID()))
	if err != nil {
		klog.Warningf("failed to list load balancers for cluster %s in the subnet %s: %v", pClusterID, userCluster.SubnetID, err)
		return nil, false, err
	}

	// Get the loadbalancer by subnetID and loadbalancer name
	lbName := s.GetLoadBalancerName(pCtx, pClusterID, pService)
	for _, itemLb := range userLbs {
		if s.findLoadBalancer(lbName, lbID, userLbs, userCluster) != nil {
			status := &lCoreV1.LoadBalancerStatus{}
			status.Ingress = []lCoreV1.LoadBalancerIngress{{IP: itemLb.Address}}
			return status, true, nil
		}
	}

	klog.Infof("Load balancer %s is not existed", lbName)
	return nil, false, nil
}

func (s *vLB) ensureSecgroup(pCluster *lObjects.Cluster) error {
	defaultSecgroup, err := s.findDefaultSecgroup()
	if err != nil {
		klog.Errorf("failed to find default secgroup: %v", err)
		return err
	}

	alreadyAttach := false
	for _, secgroup := range pCluster.MinionClusterSecGroupIDList {
		if secgroup == defaultSecgroup.UUID {
			alreadyAttach = true
			break
		}
	}

	if !alreadyAttach {
		lstSecgroupID := append(pCluster.MinionClusterSecGroupIDList, defaultSecgroup.UUID)
		klog.Infof("Attaching default secgroup %v to cluster %s", lstSecgroupID, pCluster.ID)
		// Attach the secgroup to entire minion node groups
		_, err = lClusterV2.UpdateSecgroup(
			s.vServerSC,
			lClusterV2.NewUpdateSecgroupOpts(
				s.getProjectID(),
				&lClusterV2.UpdateSecgroupOpts{
					ClusterID:   pCluster.ID,
					Master:      false,
					SecGroupIds: lstSecgroupID}))

		if err != nil {
			klog.Errorf("failed to attach secgroup %s to cluster %s: %v", defaultSecgroup.UUID, pCluster.ID, err)
			return err
		}
	}

	klog.Infof("Attached default secgroup %s to cluster %s successfully", defaultSecgroup.UUID, pCluster.ID)

	return nil
}

func (s *vLB) findDefaultSecgroup() (*lObjects.Secgroup, error) {
	secgroups, err := lSecgroupV2.List(s.vServerSC, lSecgroupV2.NewListOpts(s.getProjectID(), lConsts.DEFAULT_SECGROP_NAME))
	if err != nil {
		klog.Errorf("failed to list secgroup by secgroup name [%s]", lConsts.DEFAULT_SECGROP_NAME)
		return nil, err
	}

	if len(secgroups) < 1 {
		klog.Errorf("the project [%s] has no default secgroup", s.getProjectID())
		return nil, lErrors.NewErrNoDefaultSecgroup(s.getProjectID(), "")
	}

	for _, secgroup := range secgroups {
		if lStr.TrimSpace(lStr.ToLower(secgroup.Description)) == lConsts.DEFAULT_SECGROUP_DESCRIPTION && secgroup.Name == lConsts.DEFAULT_SECGROP_NAME {
			return secgroup, nil
		}
	}

	klog.Errorf("the project [%s] has no default secgroup", s.getProjectID())
	return nil, lErrors.NewErrNoDefaultSecgroup(s.getProjectID(), "")
}

func (s *vLB) configureLoadBalancerParams(pService *lCoreV1.Service, pNodes []*lCoreV1.Node, // params
	pServiceConfig *serviceConfig, pCluster *lObjects.Cluster) error { // returns

	// Check if there is any node available
	workerNodes := lUtils.ListWorkerNodes(pNodes, true)
	if len(workerNodes) < 1 {
		klog.Errorf("no worker node available for this cluster")
		return lErrors.NewNoNodeAvailable()
	}

	// Check if the service spec has any port, if not, return error
	ports := pService.Spec.Ports
	if len(ports) <= 0 {
		return lErrors.NewErrServicePortEmpty()
	}

	// Since the plugin does not support multiple load-balancers per service yet, the first IP family will determine the IP family of the load-balancer
	if len(pService.Spec.IPFamilies) > 0 {
		pServiceConfig.preferredIPFamily = pService.Spec.IPFamilies[0]
	}

	// Get the loadbalancer ID from the service annotation, default is empty
	pServiceConfig.lbID = lUtils.GetStringFromServiceAnnotation(
		pService, lConsts.ServiceAnnotationLoadBalancerID, "")

	// Check if user want to create an internal load-balancer
	pServiceConfig.internal = lUtils.GetBoolFromServiceAnnotation(
		pService, lConsts.ServiceAnnotationLoadBalancerInternal, false)

	// If the service is IPv6 family, the load-balancer must be internal
	if pServiceConfig.preferredIPFamily == lCoreV1.IPv6Protocol {
		pServiceConfig.internal = true
	}

	// Get the subnet ID from the cluster
	pServiceConfig.subnetID = pCluster.SubnetID

	// Set option loadbalancer type is Layer 4 in the request option
	pServiceConfig.lbType = lLoadBalancerV2.CreateOptsTypeOptLayer4

	// Get the flavor ID from the service annotation, default is get from the cloud config file
	pServiceConfig.flavorID = lUtils.GetStringFromServiceAnnotation(
		pService, lConsts.ServiceAnnotationPackageID, s.vLbConfig.DefaultL4PackageID)

	// Get the allowed CIDRs from the service annotation, default is get from the cloud config file
	pServiceConfig.allowedCIRDs = lUtils.GetStringFromServiceAnnotation(
		pService, lConsts.ServiceAnnotationListenerAllowedCIDRs, s.vLbConfig.DefaultListenerAllowedCIDRs)

	// Set default for the idle timeout
	pServiceConfig.idleTimeoutClient = lUtils.GetIntFromServiceAnnotation(
		pService, lConsts.ServiceAnnotationIdleTimeoutClient, s.vLbConfig.DefaultIdleTimeoutClient)

	pServiceConfig.idleTimeoutMember = lUtils.GetIntFromServiceAnnotation(
		pService, lConsts.ServiceAnnotationIdleTimeoutMember, s.vLbConfig.DefaultIdleTimeoutMember)

	pServiceConfig.idleTimeoutConnection = lUtils.GetIntFromServiceAnnotation(
		pService, lConsts.ServiceAnnotationIdleTimeoutConnection, s.vLbConfig.DefaultIdleTimeoutConnection)

	// Set the pool algorithm, default is get from the cloud config file if not set in the service annotation
	pServiceConfig.poolAlgorithm = lUtils.GetStringFromServiceAnnotation(
		pService, lConsts.ServiceAnnotationPoolAlgorithm, s.vLbConfig.DefaultPoolAlgorithm)

	// Set the pool healthcheck options
	pServiceConfig.monitorHealthyThreshold = lUtils.GetIntFromServiceAnnotation(
		pService, lConsts.ServiceAnnotationHealthyThreshold, s.vLbConfig.DefaultMonitorHealthyThreshold)

	// Set the monitor unhealthy threshold, default is get from the cloud config file if not set in the service annotation
	pServiceConfig.monitorUnhealthyThreshold = lUtils.GetIntFromServiceAnnotation(
		pService, lConsts.ServiceAnnotationMonitorUnhealthyThreshold, s.vLbConfig.DefaultMonitorUnhealthyThreshold)

	// Set the monitor timeout, default is get from the cloud config file if not set in the service annotation
	pServiceConfig.monitorTimeout = lUtils.GetIntFromServiceAnnotation(
		pService, lConsts.ServiceAnnotationMonitorTimeout, s.vLbConfig.DefaultMonitorTimeout)

	// Set the monitor interval, default is get from the cloud config file if not set in the service annotation
	pServiceConfig.monitorInterval = lUtils.GetIntFromServiceAnnotation(
		pService, lConsts.ServiceAnnotationMonitorInterval, s.vLbConfig.DefaultMonitorInterval)

	// Set the monitor http and its relevant options
	pServiceConfig.monitorHttpMethod = lUtils.GetStringFromServiceAnnotation(
		pService, lConsts.ServiceAnnotationMonitorHttpMethod, s.vLbConfig.DefaultMonitorHttpMethod)

	pServiceConfig.monitorHttpPath = lUtils.GetStringFromServiceAnnotation(
		pService, lConsts.ServiceAnnotationMonitorHttpPath, s.vLbConfig.DefaultMonitorHttpPath)

	pServiceConfig.monitorHttpSuccessCode = lUtils.GetStringFromServiceAnnotation(
		pService, lConsts.ServiceAnnotationMonitorHttpSuccessCode, s.vLbConfig.DefaultMonitorHttpSuccessCode)

	pServiceConfig.monitorHttpDomainName = lUtils.GetStringFromServiceAnnotation(
		pService, lConsts.ServiceAnnotationMonitorHttpDomainName, s.vLbConfig.DefaultMonitorHttpDomainName)

	pServiceConfig.monitorHttpVersion = lUtils.GetStringFromServiceAnnotation(
		pService, lConsts.ServiceAnnotationMonitorHttpVersion, s.vLbConfig.DefaultMonitorHttpVersion)

	pServiceConfig.monitorProtocol = lUtils.GetStringFromServiceAnnotation(
		pService, lConsts.ServiceAnnotationMonitorProtocol, s.vLbConfig.DefaultMonitorProtocol)

	return nil
}

func (s *vLB) findLoadBalancer(pLbName, pLbID string, pLbs []*lObjects.LoadBalancer, // params
	pCluster *lObjects.Cluster) *lObjects.LoadBalancer { // returns

	if pCluster == nil || len(pLbs) < 1 {
		return nil
	}

	for _, lb := range pLbs {
		// This solves the case when the cluster is the owner of the loadbalancer
		if lb.SubnetID == pCluster.SubnetID && lb.Name == pLbName {
			return lb
		}

		// This solves the case when user reuses this loadbalancer from the annotations
		if lb.UUID == pLbID && lb.SubnetID == pCluster.SubnetID {
			return lb
		}
	}

	return nil
}

// checkListeners checks if there is conflict for ports.
func (s *vLB) checkListeners(
	pService *lCoreV1.Service, pListenerMapping map[listenerKey]*lObjects.Listener, // params
	pCluster *lObjects.Cluster, pLb *lObjects.LoadBalancer) error { // params and returns

	// Loop via the pair of port and protocol in the spec
	for _, svcPort := range pService.Spec.Ports {
		key := listenerKey{Protocol: string(svcPort.Protocol), Port: int(svcPort.Port)}
		listenerName := lUtils.GenListenerAndPoolName(pCluster.ID, pService, svcPort)

		// If the port and protocol is existed
		if lbListener, isPresent := pListenerMapping[key]; isPresent {
			// Conflict the listener with other services
			if lbListener.Name != listenerName {
				klog.Errorf("the port %d and protocol %s is conflict", svcPort.Port, svcPort.Protocol)
				return lErrors.NewErrConflictService(int(svcPort.Port), string(svcPort.Protocol), "")
			} else {
				// not conflict, continue to check
				continue
			}
		} else {
			klog.V(5).Infof("the port %d and protocol %s in the manifest "+
				"MAY BE conflicted with other services, is checking...", svcPort.Port, svcPort.Protocol)

			// Loop via entire listeners in the loadbalancer
			for key, itemLb := range pListenerMapping {
				checkPort := key.Port == int(svcPort.Port)
				checkProtocol := key.Protocol == string(svcPort.Protocol)
				checkName := itemLb.Name == listenerName

				// If same name but port or protocol conflict => delete
				if checkName && (!checkPort || !checkProtocol) {
					// If the listener is already existed, but the port and protocol is not correct
					klog.Errorf(
						"the port %d and protocol %s is conflict, checking to delete old resources", svcPort.Port, svcPort.Protocol)
					poolID := lbListener.DefaultPoolId

					// MUST delete the listener first
					if err := lListenerV2.Delete(
						s.vLbSC, lListenerV2.NewDeleteOpts(s.getProjectID(), pLb.UUID, lbListener.UUID)); err != nil {
						klog.Errorf("failed to delete listener %s for load balancer %s: %v", lbListener.UUID, pLb.UUID, err)
						return err
					}

					klog.V(5).Infof("waiting to delete listener %s for load balancer %s complete", lbListener.UUID, pLb.UUID)
					if _, err := s.waitLoadBalancerReady(pLb.UUID); err != nil {
						klog.Errorf("failed to wait load balancer %s for service %s: %v", pLb.UUID, pService.Name, err)
						return err
					}

					// ... and then delete the pool later
					if err := lPoolV2.Delete(
						s.vLbSC, lPoolV2.NewDeleteOpts(s.getProjectID(), pLb.UUID, poolID)); err != nil {
						klog.Errorf("failed to delete pool %s for load balancer %s: %v", poolID, pLb.UUID, err)
						return err
					}

					klog.V(5).Infof("waiting to delete pool %s for load balancer %s complete", poolID, pLb.UUID)
					if _, err := s.waitLoadBalancerReady(pLb.UUID); err != nil {
						klog.Errorf("failed to wait load balancer %s for service %s: %v", pLb.UUID, pService.Name, err)
						return err
					}
				} else if !checkName {
					// Must ignore the case port and protocol is not same, and protocol is same but port is not same
					if (!checkProtocol && !checkPort) || (checkProtocol && !checkPort) {
						continue
					} else {
						// Name is not same but same port and protocol
						klog.Errorf("the port %d and protocol %s is conflict", svcPort.Port, svcPort.Protocol)
						return lErrors.NewErrConflictService(int(svcPort.Port), string(svcPort.Protocol), "")
					}
				}
			}
		}
	}

	klog.Infof("the port and protocol is not conflict")
	return nil
}

func (s *vLB) canDeleteThisLoadBalancer(pLb *lObjects.LoadBalancer, pCluster *lObjects.Cluster, pService *lCoreV1.Service) bool {
	// Get all listeners from this loadbalancer based on the load balancer UUID
	lbListeners, err := lListenerV2.GetBasedLoadBalancer(s.vLbSC, lListenerV2.NewGetBasedLoadBalancerOpts(s.getProjectID(), pLb.UUID))
	if err != nil {
		klog.Errorf("failed to get listeners for load balancer %s: %v", pLb.UUID, err)
		return false
	}

	// Check if this cluster is the owner of this loadbalancer
	if owner := lUtils.CheckOwner(pCluster, pLb, pService); !owner {
		klog.Infof("this loadbalancer is not owned by this cluster")
		return false
	}

	// Until now, this cluster is the owner of this load-balancer

	// This loadbalancer has not any listeners => CAN delete
	if lbListeners == nil || len(lbListeners) < 1 {
		return true
	}

	// Mapping entire the pair of port and protocol that are using in the k8s spec with the listener name
	currentPortMapping := make(map[listenerKey]string)
	for _, port := range pService.Spec.Ports {
		key := listenerKey{Protocol: string(port.Protocol), Port: int(port.Port)}
		currentPortMapping[key] = lUtils.GenListenerAndPoolName(pCluster.ID, pService, port)
	}

	// Loop via entire the listeners of this loadbalancer
	for _, itemListener := range lbListeners {
		port := itemListener.ProtocolPort
		protocol := itemListener.Protocol
		name := itemListener.Name
		mappingName, isPresent := currentPortMapping[listenerKey{Protocol: protocol, Port: port}]

		// If the port and protocol is existed but the name is not the same => CAN NOT delete
		//     -OR-
		// If the port and protocol is not existed => CAN NOT delete
		if (isPresent && name != mappingName) || !isPresent {
			klog.Infof("the port %d and protocol %s is being used by other service", port, protocol)
			return false
		}
	}

	return true
}

func (s *vLB) deleteListenersAndPools(
	pCluster *lObjects.Cluster, pListener *lObjects.Listener, pLb *lObjects.LoadBalancer, pService *lCoreV1.Service) error {
	if pListener == nil {
		return nil
	}

	for _, itemPort := range pService.Spec.Ports {
		listenerName := lUtils.GenListenerAndPoolName(pCluster.ID, pService, itemPort)
		poolID := pListener.DefaultPoolId

		klog.Infof("Deleting listener %s for loadbalancer %s", listenerName, pLb.UUID)
		if pListener.Name == listenerName {
			if err := lListenerV2.Delete(
				s.vLbSC, lListenerV2.NewDeleteOpts(s.getProjectID(), pLb.UUID, pListener.UUID)); err != nil {
				klog.Errorf("failed to delete listener %s for load balancer %s: %v", pListener.UUID, pLb.UUID, err)
				return err
			}

			klog.Infof("deleted listener %s (%s) for load balancer %s successfully", pListener.UUID, listenerName, pLb.UUID)
		} else {
			continue
		}

		if _, err := s.waitLoadBalancerReady(pLb.UUID); err != nil {
			klog.Errorf("failed to wait load balancer %s for service %s: %v", pLb.UUID, pService.Name, err)
			return err
		}

		if err := lPoolV2.Delete(s.vLbSC, lPoolV2.NewDeleteOpts(s.getProjectID(), pLb.UUID, poolID)); err != nil {
			klog.Errorf("failed to delete pool %s for load balancer %s: %v", poolID, pLb.UUID, err)
			return err
		}

		if _, err := s.waitLoadBalancerReady(pLb.UUID); err != nil {
			klog.Errorf("failed to wait load balancer %s for service %s: %v", pLb.UUID, pService.Name, err)
			return err
		}

		klog.Infof("deleted listener %s and pool %s for load balancer %s successfully", pListener.UUID, poolID, pLb.UUID)
		break
	}

	return nil
}

func (s *vLB) ensureUpdateLoadBalancer(pCtx lCtx.Context, pClusterID string, pService *lCoreV1.Service, pNodes []*lCoreV1.Node) error {

	// Get the loadbalancer ID
	lbID := lUtils.GetStringFromServiceAnnotation(pService, lConsts.ServiceAnnotationLoadBalancerID, "")

	// Get the cluster info from the cluster ID that user provided from the K8s secret
	userCluster, err := lClusterV2.Get(s.vServerSC, lClusterV2.NewGetOpts(s.getProjectID(), pClusterID))
	if err != nil || userCluster == nil {
		klog.Errorf("cluster %s NOT found, please check the secret resource in the Helm template", pClusterID)
		return err
	}

	// Get this loadbalancer name
	lbName := s.GetLoadBalancerName(pCtx, pClusterID, pService)
	lstLbs, err := lLoadBalancerV2.List(s.vLbSC, lLoadBalancerV2.NewListOpts(s.getProjectID()))
	if err != nil {
		klog.Errorf(
			"failed to list load balancers for cluster %s in the subnet %s: %v", pClusterID, userCluster.SubnetID, err)
		return err
	}

	if lstLbs == nil || len(lstLbs) < 1 {
		klog.Infof("no load balancer found for cluster %s", pClusterID)
		return nil
	}

	userLb := s.findLoadBalancer(lbName, lbID, lstLbs, userCluster)
	if userLb == nil {
		klog.Infof("Load balancer %s is not existed", lbName)
		return nil
	}

	// Get all pools of this loadbalancer based on the load balancer uuid
	lstPools, err := lPoolV2.ListPoolsBasedLoadBalancer(
		s.vLbSC, lPoolV2.NewListPoolsBasedLoadBalancerOpts(s.getProjectID(), userLb.UUID))
	if err != nil {
		klog.Errorf("failed to list pools for load balancer %s when updating the Kubernetes services: %v", userLb.UUID, err)
		return err
	}

	for _, itemPort := range pService.Spec.Ports {
		// Loop via pool
		poolName := lUtils.GenListenerAndPoolName(userCluster.ID, pService, itemPort)
		if err := s.updatePoolMembers(poolName, userLb, lstPools, pNodes, itemPort, pService); err != nil {
			klog.Errorf(
				"failed to update pool %s for service %s in ensure update loadbalancer action: %v", poolName, pService.Name, err)
			return err
		}
	}

	return nil
}

func (s *vLB) updatePoolMembers(
	pPoolName string, pLb *lObjects.LoadBalancer, pPools []*lObjects.Pool, pNodes []*lCoreV1.Node, // params
	pPort lCoreV1.ServicePort, pService *lCoreV1.Service) error { // params + returns

	svcConfig := new(serviceConfig)
	svcConfig.preferredIPFamily = lCoreV1.IPv4Protocol

	if len(pService.Spec.IPFamilies) > 0 {
		svcConfig.preferredIPFamily = pService.Spec.IPFamilies[0]
	}

	for _, itemPool := range pPools {
		if itemPool.Name == pPoolName && isMemberChange(itemPool, pNodes) {
			poolMembers, err := prepareMembers(pNodes, pPort, svcConfig)
			if err != nil {
				klog.Errorf("failed to prepare members for pool %s: %v", pService.Name, err)
				return err
			}

			if err = lPoolV2.UpdatePoolMembers(s.vLbSC, lPoolV2.NewUpdatePoolMembersOpts(
				s.extraInfo.ProjectID, pLb.UUID, itemPool.UUID, &lPoolV2.UpdatePoolMembersOpts{
					Members: poolMembers,
				})); err != nil {
				klog.Errorf("failed to update pool %s for service %s: %v", pService.Name, pService.Name, err)
				return err
			} else {
				klog.Infof(
					"updating pool %s for service %s, waiting loadbalancer complete", pService.Name, pService.Name)
				if _, err = s.waitLoadBalancerReady(pLb.UUID); err != nil {
					klog.Errorf("failed to wait load balancer %s for service %s: %v", pLb.UUID, pService.Name, err)
					return err
				}
			}
		}
	}

	return nil
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
	[]*lPoolV2.Member, error) { // returns

	var poolMembers []*lPoolV2.Member
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
			poolMembers = append(poolMembers, &lPoolV2.Member{
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

func (s *vLB) nodeSyncLoop() {
	klog.Infoln("------------ nodeSyncLoop() ------------")
	isReApply := false

	lbs, err := lLoadBalancerV2.List(s.vLbSC, lLoadBalancerV2.NewListOpts(s.getProjectID()))
	if err != nil {
		klog.Errorf("failed to find load balancers for cluster %s: %v", s.clusterID, err)
		return
	}
	reapplyIngress := s.trackLBUpdate.GetReapplyIngress(lbs)
	if len(reapplyIngress) > 0 {
		isReApply = true
		klog.Infof("Detected change in load balancer update tracker")
	}

	if !isReApply {
		return
	}

	readyWorkerNodes, err := listNodeWithPredicate(s.nodeLister, getNodeConditionPredicate())
	if err != nil {
		klog.Errorf("Failed to retrieve current set of nodes from node lister: %v", err)
		return
	}

	services, err := listServiceWithPredicate(s.serviceLister, getServiceConditionPredicate())
	if err != nil {
		klog.Errorf("Failed to retrieve current set of services from service lister: %v", err)
	}

	for _, service := range services {
		if _, err := s.EnsureLoadBalancer(lCtx.Background(), s.clusterID, service, readyWorkerNodes); err != nil {
			klog.Errorf("Failed to reapply load balancer for service %s: %v", service.Name, err)
		}
	}
}
