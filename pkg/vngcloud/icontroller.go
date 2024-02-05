package vngcloud

import (
	lCtx "context"
	lSync "sync"

	lCoreV1 "k8s.io/api/core/v1"
)

type IController interface {
	EnsureGetLoadBalancer(pCtx lCtx.Context, pClusterID string, pService *lCoreV1.Service) (*lCoreV1.LoadBalancerStatus, bool, error)
	GetLoadBalancerName(pCtx lCtx.Context, pClusterID string, pService *lCoreV1.Service) string
	EnsureLoadBalancer(pCtx lCtx.Context, pClusterID string, pService *lCoreV1.Service, pNodes []*lCoreV1.Node) (*lCoreV1.LoadBalancerStatus, error)
	UpdateLoadBalancer(pCtx lCtx.Context, pClusterID string, pService *lCoreV1.Service, pNodes []*lCoreV1.Node) error
	EnsureLoadBalancerDeleted(pCtx lCtx.Context, pClusterID string, pService *lCoreV1.Service) error
}

var (
	controller     IController
	controllerOnce lSync.Once
)
