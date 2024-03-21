package vngcloud

import (
	lCoreV1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	corelisters "k8s.io/client-go/listers/core/v1"
)

type NodeConditionPredicate func(node *lCoreV1.Node) bool
type ServiceConditionPredicate func(service *lCoreV1.Service) bool

func listNodeWithPredicate(nodeLister corelisters.NodeLister, predicate NodeConditionPredicate) ([]*lCoreV1.Node, error) {
	nodes, err := nodeLister.List(labels.Everything())
	if err != nil {
		return nil, err
	}

	var filtered []*lCoreV1.Node
	for i := range nodes {
		if predicate(nodes[i]) {
			filtered = append(filtered, nodes[i])
		}
	}

	return filtered, nil
}

func listServiceWithPredicate(serviceLister corelisters.ServiceLister, predicate ServiceConditionPredicate) ([]*lCoreV1.Service, error) {
	services, err := serviceLister.List(labels.Everything())
	if err != nil {
		return nil, err
	}

	var filtered []*lCoreV1.Service
	for i := range services {
		if predicate(services[i]) {
			filtered = append(filtered, services[i])
		}
	}

	return filtered, nil
}

func getNodeConditionPredicate() NodeConditionPredicate {
	return func(node *lCoreV1.Node) bool {
		// We add the master to the node list, but its unschedulable.  So we use this to filter
		// the master.
		if node.Spec.Unschedulable {
			return false
		}

		// Recognize nodes labeled as not suitable for LB, and filter them also, as we were doing previously.
		if _, hasExcludeLBRoleLabel := node.Labels[LabelNodeExcludeLB]; hasExcludeLBRoleLabel {
			return false
		}

		// Deprecated in favor of LabelNodeExcludeLB, kept for consistency and will be removed later
		if _, hasNodeRoleMasterLabel := node.Labels[DeprecatedLabelNodeRoleMaster]; hasNodeRoleMasterLabel {
			return false
		}

		// If we have no info, don't accept
		if len(node.Status.Conditions) == 0 {
			return false
		}
		for _, cond := range node.Status.Conditions {
			// We consider the node for load balancing only when its NodeReady condition status
			// is ConditionTrue
			if cond.Type == lCoreV1.NodeReady && cond.Status != lCoreV1.ConditionTrue {
				// log.WithFields(log.Fields{"name": node.Name, "status": cond.Status}).Info("ignoring node")
				return false
			}
		}
		return true
	}
}

func getServiceConditionPredicate() ServiceConditionPredicate {
	return func(service *lCoreV1.Service) bool {
		// We only consider services with type LoadBalancer
		return service.Spec.Type == lCoreV1.ServiceTypeLoadBalancer
	}
}

const (
	// LabelNodeExcludeLB specifies that a node should not be used to create a Loadbalancer on
	// https://github.com/kubernetes/cloud-provider/blob/25867882d509131a6fdeaf812ceebfd0f19015dd/controllers/service/controller.go#L673
	LabelNodeExcludeLB = "node.kubernetes.io/exclude-from-external-load-balancers"

	// DeprecatedLabelNodeRoleMaster specifies that a node is a master
	// It's copied over to kubeadm until it's merged in core: https://github.com/kubernetes/kubernetes/pull/39112
	// Deprecated in favor of LabelNodeExcludeLB
	DeprecatedLabelNodeRoleMaster = "node-role.kubernetes.io/master"
)
