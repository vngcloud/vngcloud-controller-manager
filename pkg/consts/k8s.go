package consts

const (
	DEFAULT_K8S_MASTER_LABEL              = "node-role.kubernetes.io/master"
	DEFAULT_K8S_SERVICE_ANNOTATION_PREFIX = "vks.vngcloud.vn"
)

// Annotations
const (
	ServiceAnnotationLoadBalancerID            = DEFAULT_K8S_SERVICE_ANNOTATION_PREFIX + "/load-balancer-id"        // set via annotation
	ServiceAnnotationLoadBalancerName          = DEFAULT_K8S_SERVICE_ANNOTATION_PREFIX + "/load-balancer-name"      // only set via the annotation
	ServiceAnnotationPackageID                 = DEFAULT_K8S_SERVICE_ANNOTATION_PREFIX + "/package-id"              // both annotation and cloud-config
	ServiceAnnotationEnableSecgroupDefault     = DEFAULT_K8S_SERVICE_ANNOTATION_PREFIX + "/enable-secgroup-default" // set via annotation
	ServiceAnnotationIdleTimeoutClient         = DEFAULT_K8S_SERVICE_ANNOTATION_PREFIX + "/idle-timeout-client"     // both annotation and cloud-config
	ServiceAnnotationIdleTimeoutMember         = DEFAULT_K8S_SERVICE_ANNOTATION_PREFIX + "/idle-timeout-member"     // both annotation and cloud-config
	ServiceAnnotationIdleTimeoutConnection     = DEFAULT_K8S_SERVICE_ANNOTATION_PREFIX + "/idle-timeout-connection" // both annotation and cloud-config
	ServiceAnnotationListenerAllowedCIDRs      = DEFAULT_K8S_SERVICE_ANNOTATION_PREFIX + "/listener-allowed-cidrs"  // both annotation and cloud-config
	ServiceAnnotationPoolAlgorithm             = DEFAULT_K8S_SERVICE_ANNOTATION_PREFIX + "/pool-algorithm"          // both annotation and cloud-config
	ServiceAnnotationHealthyThreshold          = DEFAULT_K8S_SERVICE_ANNOTATION_PREFIX + "/monitor-healthy-threshold"
	ServiceAnnotationMonitorUnhealthyThreshold = DEFAULT_K8S_SERVICE_ANNOTATION_PREFIX + "/monitor-unhealthy-threshold"
	ServiceAnnotationMonitorTimeout            = DEFAULT_K8S_SERVICE_ANNOTATION_PREFIX + "/monitor-timeout"
	ServiceAnnotationMonitorInterval           = DEFAULT_K8S_SERVICE_ANNOTATION_PREFIX + "/monitor-interval"
	ServiceAnnotationLoadBalancerInternal      = DEFAULT_K8S_SERVICE_ANNOTATION_PREFIX + "/internal-load-balancer"
	ServiceAnnotationMonitorHttpMethod         = DEFAULT_K8S_SERVICE_ANNOTATION_PREFIX + "/monitor-http-method"
	ServiceAnnotationMonitorHttpPath           = DEFAULT_K8S_SERVICE_ANNOTATION_PREFIX + "/monitor-http-path"
	ServiceAnnotationMonitorHttpSuccessCode    = DEFAULT_K8S_SERVICE_ANNOTATION_PREFIX + "/monitor-http-success-code"
	ServiceAnnotationMonitorHttpVersion        = DEFAULT_K8S_SERVICE_ANNOTATION_PREFIX + "/monitor-http-version"
	ServiceAnnotationMonitorHttpDomainName     = DEFAULT_K8S_SERVICE_ANNOTATION_PREFIX + "/monitor-http-domain-name"
	ServiceAnnotationMonitorProtocol           = DEFAULT_K8S_SERVICE_ANNOTATION_PREFIX + "/monitor-protocol"
)
