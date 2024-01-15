/*
Copyright 2024 The Karmada Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package resourcequota

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/klog/v2"

	"github.com/karmada-io/karmada/pkg/estimator/pb"
	"github.com/karmada-io/karmada/pkg/estimator/server/framework"
	"github.com/karmada-io/karmada/pkg/features"
	"github.com/karmada-io/karmada/pkg/util"
	corev1helper "github.com/karmada-io/karmada/pkg/util/lifted"
)

const (
	// Name is the name of the plugin used in Registry and configurations.
	Name                   = "ResourceQuotaEstimator"
	resourceRequestsPrefix = "requests."
	resourceLimitsPrefix   = "limits."
)

type resourceQuotaEstimator struct {
	enabled  bool
	rqLister corelisters.ResourceQuotaLister
}

var _ framework.EstimateReplicasPlugin = &resourceQuotaEstimator{}

// New initializes a new plugin and returns it.
func New(fh framework.Handle) (framework.Plugin, error) {
	enabled := features.FeatureGate.Enabled(features.ResourceQuotaEstimate)
	fmt.Println(fmt.Sprintf("YaoTest1 line 56 enable %t", enabled))
	if !enabled {
		// Disabled, won't do anything.
		return &resourceQuotaEstimator{}, nil
	}
	return &resourceQuotaEstimator{
		enabled:  enabled,
		rqLister: fh.SharedInformerFactory().Core().V1().ResourceQuotas().Lister(),
	}, nil
}

// Name returns name of the plugin. It is used in logs, etc.
func (pl *resourceQuotaEstimator) Name() string {
	return Name
}

func (pl *resourceQuotaEstimator) Estimate(ctx context.Context, replicaRequirements *pb.ReplicaRequirements) (int32, error) {
	var result int32 = math.MaxInt32
	logger := klog.FromContext(ctx)
	if !pl.enabled {
		logger.V(5).Info("Estimator Plugin", "name", Name, "enabled", pl.enabled)
		return result, nil
	}
	namespace := replicaRequirements.Namespace
	priorityClassName := replicaRequirements.PriorityClassName
	fmt.Printf(fmt.Sprintf("YaoTest1 line 80 namespace is %s priorityClass is %s \n", namespace, priorityClassName))

	rqList, err := pl.rqLister.ResourceQuotas(namespace).List(labels.Everything())
	if err != nil {
		logger.Error(err, "fail to list resource quota", "namespace", namespace)
		return result, err
	}
	for _, rq := range rqList {
		rqEvaluator := newResourceQuotaEvaluator(ctx, rq, priorityClassName)
		replicaCount := rqEvaluator.Evaluate(replicaRequirements)
		if replicaCount < result {
			result = replicaCount
		}
	}
	fmt.Printf(fmt.Sprintf("YaoTest1 line 94 replicaCount is %d \n", result))
	return result, nil
}

type resourceQuotaEvaluator struct {
	resourceRequest []*util.Resource
}

func newResourceQuotaEvaluator(ctx context.Context, rq *corev1.ResourceQuota, priorityClassName string) *resourceQuotaEvaluator {
	logger := klog.FromContext(ctx)
	selectors := getScopeSelectorsFromQuota(rq)
	var resources []*util.Resource

	for _, selector := range selectors {
		matchScope, err := matchesScope(selector, priorityClassName)
		if err != nil {
			logger.Error(err, "matchesScope failed")
			continue
		}
		if !matchScope {
			continue
		}
		matchResource := matchingResources(resourceNames(rq.Status.Hard))
		resource := convert(rq, matchResource)
		resources = append(resources, resource)
	}
	return &resourceQuotaEvaluator{
		resourceRequest: resources,
	}
}

func (e *resourceQuotaEvaluator) Evaluate(replicaRequirements *pb.ReplicaRequirements) int32 {
	var result int32 = math.MaxInt32
	for _, resource := range e.resourceRequest {
		allowed := int32(resource.MaxDivided(replicaRequirements.ResourceRequest))
		if allowed < result {
			result = allowed
		}
	}
	return result
}

func convert(rq *corev1.ResourceQuota, resourceNames []corev1.ResourceName) *util.Resource {
	hardResourceList := corev1.ResourceList{}
	usedResourceList := corev1.ResourceList{}
	for _, resourceName := range resourceNames {
		rNameStr := string(resourceName)
		//skip limits because pb.ReplicaRequirements only support requested resource
		if strings.HasPrefix(rNameStr, resourceLimitsPrefix) {
			continue
		}
		//requests.cpu is same as cpu
		//requests.memory is same as memory
		//we merge them together
		trimmedResourceName := corev1.ResourceName(strings.TrimPrefix(rNameStr, resourceRequestsPrefix))
		hardResource, hardResourceOk := rq.Status.Hard[resourceName]
		usedResource, usedResourceOk := rq.Status.Used[resourceName]
		if !hardResourceOk || !usedResourceOk {
			continue
		}
		hardResourceList[trimmedResourceName] = hardResource
		usedResourceList[trimmedResourceName] = usedResource
	}
	hard := util.NewResource(hardResourceList)
	used := util.NewResource(usedResourceList)
	if hard == nil || used == nil {
		return nil
	}
	resource := hard.SubResource(used)
	resource.AllowedPodNumber = math.MaxInt64
	return resource
}

// resourceNames returns a list of all resource names in the ResourceList
func resourceNames(resources corev1.ResourceList) []corev1.ResourceName {
	result := []corev1.ResourceName{}
	for resourceName := range resources {
		result = append(result, resourceName)
	}
	return result
}

func getScopeSelectorsFromQuota(quota *corev1.ResourceQuota) []corev1.ScopedResourceSelectorRequirement {
	selectors := []corev1.ScopedResourceSelectorRequirement{}
	for _, scope := range quota.Spec.Scopes {
		selectors = append(selectors, corev1.ScopedResourceSelectorRequirement{
			ScopeName: scope,
			Operator:  corev1.ScopeSelectorOpExists})
	}
	if quota.Spec.ScopeSelector != nil {
		selectors = append(selectors, quota.Spec.ScopeSelector.MatchExpressions...)
	}
	return selectors
}

// matchesScope is a function that knows how to evaluate if a pod matches a scope
func matchesScope(selector corev1.ScopedResourceSelectorRequirement, priorityClassName string) (bool, error) {
	switch selector.ScopeName {
	case corev1.ResourceQuotaScopeTerminating:
		return false, nil
	case corev1.ResourceQuotaScopeNotTerminating:
		return false, nil
	case corev1.ResourceQuotaScopeBestEffort:
		return false, nil
	case corev1.ResourceQuotaScopeNotBestEffort:
		return false, nil
	case corev1.ResourceQuotaScopePriorityClass:
		if selector.Operator == corev1.ScopeSelectorOpExists {
			// This is just checking for existence of a priorityClass on the pod,
			// no need to take the overhead of selector parsing/evaluation.
			return len(priorityClassName) != 0, nil
		}
		return matchesSelector(priorityClassName, selector)
	case corev1.ResourceQuotaScopeCrossNamespacePodAffinity:
		return false, nil
	}
	return false, nil
}

func matchesSelector(priorityClassName string, selector corev1.ScopedResourceSelectorRequirement) (bool, error) {
	labelSelector, err := scopedResourceSelectorRequirementsAsSelector(selector)
	if err != nil {
		return false, fmt.Errorf("failed to parse and convert selector: %v", err)
	}
	var m map[string]string
	if len(priorityClassName) != 0 {
		m = map[string]string{string(corev1.ResourceQuotaScopePriorityClass): priorityClassName}
	}
	if labelSelector.Matches(labels.Set(m)) {
		return true, nil
	}
	return false, nil
}

// scopedResourceSelectorRequirementsAsSelector converts the ScopedResourceSelectorRequirement api type into a struct that implements
// labels.Selector.
func scopedResourceSelectorRequirementsAsSelector(ssr corev1.ScopedResourceSelectorRequirement) (labels.Selector, error) {
	selector := labels.NewSelector()
	var op selection.Operator
	switch ssr.Operator {
	case corev1.ScopeSelectorOpIn:
		op = selection.In
	case corev1.ScopeSelectorOpNotIn:
		op = selection.NotIn
	case corev1.ScopeSelectorOpExists:
		op = selection.Exists
	case corev1.ScopeSelectorOpDoesNotExist:
		op = selection.DoesNotExist
	default:
		return nil, fmt.Errorf("%q is not a valid scope selector operator", ssr.Operator)
	}
	r, err := labels.NewRequirement(string(ssr.ScopeName), op, ssr.Values)
	if err != nil {
		return nil, err
	}
	selector = selector.Add(*r)
	return selector, nil
}

// computeResources are the set of resources managed by quota associated with pods.
var computeResources = []corev1.ResourceName{
	corev1.ResourceCPU,
	corev1.ResourceMemory,
	corev1.ResourceEphemeralStorage,
	corev1.ResourceRequestsCPU,
	corev1.ResourceRequestsMemory,
	corev1.ResourceRequestsEphemeralStorage,
	corev1.ResourceLimitsCPU,
	corev1.ResourceLimitsMemory,
	corev1.ResourceLimitsEphemeralStorage,
}

// matchingResources takes the input specified list of resources and returns the set of resources it matches.
func matchingResources(input []corev1.ResourceName) []corev1.ResourceName {
	result := intersection(input, computeResources)
	for _, resource := range input {
		// for extended resources
		if strings.HasPrefix(string(resource), resourceRequestsPrefix) {
			trimmedResourceName := corev1.ResourceName(strings.TrimPrefix(string(resource), resourceRequestsPrefix))
			if corev1helper.IsExtendedResourceName(trimmedResourceName) {
				result = append(result, resource)
			}
		}
		if strings.HasPrefix(string(resource), resourceLimitsPrefix) {
			trimmedResourceName := corev1.ResourceName(strings.TrimPrefix(string(resource), resourceLimitsPrefix))
			if corev1helper.IsExtendedResourceName(trimmedResourceName) {
				result = append(result, resource)
			}
		}
	}
	return result
}

// intersection returns the intersection of both list of resources, deduped and sorted
func intersection(a []corev1.ResourceName, b []corev1.ResourceName) []corev1.ResourceName {
	result := make([]corev1.ResourceName, 0, len(a))
	for _, item := range a {
		if contains(result, item) {
			continue
		}
		if !contains(b, item) {
			continue
		}
		result = append(result, item)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

// contains returns true if the specified item is in the list of items
func contains(items []corev1.ResourceName, item corev1.ResourceName) bool {
	for _, i := range items {
		if i == item {
			return true
		}
	}
	return false
}
