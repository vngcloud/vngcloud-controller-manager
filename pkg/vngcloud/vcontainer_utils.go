package vngcloud

import (
	"fmt"
	"github.com/cuongpiger/joat/utils"
	"github.com/vngcloud/vngcloud-controller-manager/pkg/client"
	lvconCcmMetrics "github.com/vngcloud/vngcloud-controller-manager/pkg/metrics"
	"github.com/vngcloud/vngcloud-controller-manager/pkg/utils/metadata"
	client2 "github.com/vngcloud/vngcloud-go-sdk/client"
	lvconSdkErr "github.com/vngcloud/vngcloud-go-sdk/error/utils"
	"github.com/vngcloud/vngcloud-go-sdk/vngcloud"
	lPortal "github.com/vngcloud/vngcloud-go-sdk/vngcloud/services/portal/v1"
	gcfg "gopkg.in/gcfg.v1"
	"io"
	lcloudProvider "k8s.io/cloud-provider"
	"k8s.io/klog/v2"
)

// ************************************************* PUBLIC FUNCTIONS **************************************************

func NewVContainer(pCfg Config) (*VContainer, error) {
	provider, err := client.NewVContainerClient(&pCfg.Global)
	if err != nil {
		klog.Errorf("failed to init VContainer client: %v", err)
		return nil, err
	}

	metadator := metadata.GetMetadataProvider(pCfg.Metadata.SearchOrder)
	extraInfo, err := setupPortalInfo(
		provider,
		metadator,
		utils.NormalizeURL(pCfg.Global.VServerURL)+"vserver-gateway/v1")

	if err != nil {
		klog.Errorf("failed to setup portal info: %v", err)
		return nil, err
	}

	return &VContainer{
		provider:  provider,
		vLbOpts:   pCfg.VLB,
		config:    &pCfg,
		extraInfo: extraInfo,
	}, nil
}

// ************************************************* PRIVATE FUNCTIONS *************************************************

func init() {
	fmt.Println("CUONGDM3: init the vcontainer-ccm")
	// Register metrics
	lvconCcmMetrics.RegisterMetrics("vcontainer-ccm")

	// Register VNG-CLOUD cloud provider
	lcloudProvider.RegisterCloudProvider(
		PROVIDER_NAME,
		func(cfg io.Reader) (lcloudProvider.Interface, error) {
			config, err := readConfig(cfg)
			if err != nil {
				klog.Warningf("failed to read config file: %v", err)
				return nil, err
			}

			config.Metadata = getMetadataOption(config.Metadata)
			cloud, err := NewVContainer(config)
			if err != nil {
				klog.Warningf("failed to init VContainer: %v", err)
			}

			return cloud, err
		},
	)
}

func readConfig(pCfg io.Reader) (Config, error) {
	if pCfg == nil {
		return Config{}, lvconSdkErr.NewErrEmptyConfig("", "config file is empty")
	}

	// Set default
	config := Config{}

	err := gcfg.FatalOnly(gcfg.ReadInto(&config, pCfg))
	if err != nil {
		return Config{}, err
	}

	// Log the config
	klog.V(5).Infof("read config from file")
	client.LogCfg(config.Global)

	if config.Metadata.SearchOrder == "" {
		config.Metadata.SearchOrder = fmt.Sprintf("%s,%s", metadata.ConfigDriveID, metadata.MetadataID)
	}

	return config, nil
}

func getMetadataOption(pMetadata metadata.Opts) metadata.Opts {
	if pMetadata.SearchOrder == "" {
		pMetadata.SearchOrder = fmt.Sprintf("%s,%s", metadata.ConfigDriveID, metadata.MetadataID)
	}
	klog.Info("getMetadataOption; metadataOpts is ", pMetadata)
	return pMetadata
}

func setupPortalInfo(pProvider *client2.ProviderClient, pMedatata metadata.IMetadata, pPortalURL string) (*ExtraInfo, error) {
	projectID, err := pMedatata.GetProjectID()
	if err != nil {
		return nil, err
	}
	klog.Infof("SetupPortalInfo; projectID is %s", projectID)

	portalClient, _ := vngcloud.NewServiceClient(pPortalURL, pProvider, "portal")
	portalInfo, err := lPortal.Get(portalClient, projectID)

	if err != nil {
		return nil, err
	}

	if portalInfo == nil {
		return nil, fmt.Errorf("can not get portal information")
	}

	return &ExtraInfo{
		ProjectID: portalInfo.ProjectID,
		UserID:    portalInfo.UserID,
	}, nil
}
