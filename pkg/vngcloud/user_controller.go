package vngcloud

import (
	lCtx "context"
	"fmt"
	lConsts "github.com/vngcloud/vngcloud-controller-manager/pkg/consts"
	lUtils "github.com/vngcloud/vngcloud-controller-manager/pkg/utils"
	"github.com/vngcloud/vngcloud-controller-manager/pkg/utils/errors"
	lClient "github.com/vngcloud/vngcloud-go-sdk/client"
	lLoadBalancerV2 "github.com/vngcloud/vngcloud-go-sdk/vngcloud/services/loadbalancer/v2/loadbalancer"
	lCoreV1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
)

type userController struct {
	vLbSC     *lClient.ServiceClient
	vServerSC *lClient.ServiceClient

	kubeClient    kubernetes.Interface
	eventRecorder record.EventRecorder
	vLbConfig     VLbOpts
	extraInfo     *ExtraInfo
}

func newUserController(
	pLbSc, pServerSc *lClient.ServiceClient, pKubeClient kubernetes.Interface,
	pRecorder record.EventRecorder, vlbConfig VLbOpts, extraInfo *ExtraInfo) IController {
	return &userController{
		vLbSC:         pLbSc,
		vServerSC:     pServerSc,
		kubeClient:    pKubeClient,
		eventRecorder: pRecorder,
		vLbConfig:     vlbConfig,
		extraInfo:     extraInfo,
	}
}

func (s *userController) EnsureGetLoadBalancer(_ lCtx.Context, _ string, pService *lCoreV1.Service) (*lCoreV1.LoadBalancerStatus, bool, error) {
	// Get the loadbalancer UUID
	lbID := lUtils.GetStringFromServiceAnnotation(pService, lConsts.ServiceAnnotationLoadBalancerID, "")
	if len(lbID) < 1 {
		klog.Errorf("load balancer ID is not found in the service annotation")
		return nil, false, nil
	}

	lb, err := lLoadBalancerV2.Get(s.vLbSC, lLoadBalancerV2.NewGetOpts(s.getProjectID(), lbID))
	if err != nil {
		klog.Errorf("failed to get load balancer %s: %v", lbID, err)
		return nil, false, err
	}

	if lb == nil {
		klog.Errorf("load balancer %s not found", lbID)
		return nil, false, errors.NewErrLoadBalancerNotFound(fmt.Sprintf("not found load balancer [%s]", lbID))
	}

	return &lCoreV1.LoadBalancerStatus{
		Ingress: []lCoreV1.LoadBalancerIngress{
			{
				IP: lb.Address,
			},
		},
	}, true, nil
}
func (s *userController) GetLoadBalancerName(_ lCtx.Context, _ string, pService *lCoreV1.Service) string {
	lbID := lUtils.GetStringFromServiceAnnotation(pService, lConsts.ServiceAnnotationLoadBalancerID, "")
	if len(lbID) < 1 {
		klog.Errorf("load balancer ID is not found in the service annotation")
		return ""
	}

	lb, err := lLoadBalancerV2.Get(s.vLbSC, lLoadBalancerV2.NewGetOpts(s.getProjectID(), lbID))
	if err != nil {
		klog.Errorf("failed to get load balancer %s: %v", lbID, err)
		return ""
	}

	if lb == nil {
		klog.Errorf("load balancer %s not found", lbID)
		return ""
	}

	return lb.Name
}
func (s *userController) EnsureLoadBalancer(pCtx lCtx.Context, pClusterID string, pService *lCoreV1.Service, pNodes []*lCoreV1.Node) (*lCoreV1.LoadBalancerStatus, error) {
	return nil, nil
}
func (s *userController) UpdateLoadBalancer(pCtx lCtx.Context, pClusterID string, pService *lCoreV1.Service, pNodes []*lCoreV1.Node) error {
	return nil
}
func (s *userController) EnsureLoadBalancerDeleted(pCtx lCtx.Context, pClusterID string, pService *lCoreV1.Service) error {
	return nil
}

func (s *userController) getProjectID() string {
	return s.extraInfo.ProjectID
}
