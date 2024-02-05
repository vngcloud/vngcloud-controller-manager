package errors

import vconError "github.com/vngcloud/vngcloud-go-sdk/error"

// ********************************************* ErrConflictServiceAndCloud ********************************************

func NewErrLoadBalancerNotFound(pInfo string) vconError.IErrorBuilder {
	err := new(ErrLoadBalancerNotFound)
	err.Info = pInfo
	return err
}

func IsErrLoadBalancerNotFound(pErr error) bool {
	_, ok := pErr.(*ErrLoadBalancerNotFound)
	return ok
}

func (s *ErrLoadBalancerNotFound) Error() string {
	s.DefaultError = "not found load balancer"
	return s.ChoseErrString()
}

type ErrLoadBalancerNotFound struct {
	vconError.BaseError
}
