package auth

import (
	"fmt"

	"github.com/casbin/casbin/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Authorizer struct {
	enforcer *casbin.Enforcer
}

func New(modelFile, policyFile string) (*Authorizer, error) {
	enforcer, err := casbin.NewEnforcer(modelFile, policyFile)
	if err != nil {
		return nil, err
	}
	return &Authorizer{enforcer: enforcer}, nil
}

func (authorizer *Authorizer) Authorize(subject, object, action string) error {
	allowed, err := authorizer.enforcer.Enforce(subject, object, action)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	if !allowed {
		return status.Error(codes.PermissionDenied, fmt.Sprintf("%s not permitted to %s %s", subject, action, object))
	}
	return nil
}
