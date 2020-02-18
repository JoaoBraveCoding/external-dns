/*
Copyright 2018 The Kubernetes Authors.

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

package source

import (
	"fmt"
	"os"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/kubernetes/pkg/util/async"

	"sigs.k8s.io/external-dns/endpoint"
)

// crdSource is an implementation of Source that provides endpoints by listing
// specified CRD and fetching Endpoints embedded in Spec.
type crdSource struct {
	client      kubernetes.Interface
	namespace   string
	crdInformer kubeinformers.GenericInformer
	runner                   *async.BoundedFrequencyRunner
}

func addKnownTypes(scheme *runtime.Scheme, groupVersion schema.GroupVersion) error {
	scheme.AddKnownTypes(groupVersion,
		&endpoint.DNSEndpoint{},
		&endpoint.DNSEndpointList{},
	)
	metav1.AddToGroupVersion(scheme, groupVersion)
	return nil
}

// NewCRDSource creates a new crdSource with the given config.
func NewCRDSource(kubeClient kubernetes.Interface, kubeConfig, kubeMaster, apiVersion, kind string, namespace string) (Source, error) {

	if kubeConfig == "" {
		if _, err := os.Stat(clientcmd.RecommendedHomeFile); err == nil {
			kubeConfig = clientcmd.RecommendedHomeFile
		}
	}

	groupVersion, err := schema.ParseGroupVersion(apiVersion)
	if err != nil {
		return nil, err
	}
	groupVersionResource := schema.GroupVersionResource{Group: groupVersion.Group, Version: groupVersion.Version, Resource: kind}

	scheme := runtime.NewScheme()
	addKnownTypes(scheme, groupVersion)

	// Use shared informer to listen for add/update/delete of ingresses in the specified namespace.
	// Set resync period to 0, to prevent processing when nothing has changed.
	informerFactory := kubeinformers.NewSharedInformerFactoryWithOptions(kubeClient, 0, kubeinformers.WithNamespace(namespace))
	crdInformer, err := informerFactory.ForResource(groupVersionResource)
	if err != nil {
		return nil, err
	}

	// Add default resource event handlers to properly initialize informer.
	crdInformer.Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
			},
		},
	)

	// TODO informer is not explicitly stopped since controller is not passing in its channel.
	informerFactory.Start(wait.NeverStop)

	// wait for the local cache to be populated.
	err = wait.Poll(time.Second, 60*time.Second, func() (bool, error) {
		return crdInformer.Informer().HasSynced(), nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to sync cache: %v", err)
	}

	sc := &crdSource{
		client:      kubeClient,
		namespace:   namespace,
		crdInformer: crdInformer,
	}
	return sc, nil
}

func (sc *crdSource) AddEventHandler(handler func() error, stopChan <-chan struct{}, minInterval time.Duration) {
	// Add custom resource event handler
	log.Debug("Adding (bounded) event handler for CRD")

	maxInterval := 24 * time.Hour // handler will be called if it has not run in 24 hours
	burst := 2                    // allow up to two handler burst calls
	log.Debugf("Adding handler to BoundedFrequencyRunner with minInterval: %v, syncPeriod: %v, bursts: %d",
		minInterval, maxInterval, burst)
	sc.runner = async.NewBoundedFrequencyRunner("crd-handler", func() {
		_ = handler()
	}, minInterval, maxInterval, burst)
	go sc.runner.Loop(stopChan)

	// run the handler function as soon as the BoundedFrequencyRunner will allow when an update occurs
	sc.crdInformer.Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				sc.runner.Run()
			},
			UpdateFunc: func(old interface{}, new interface{}) {
				sc.runner.Run()
			},
			DeleteFunc: func(obj interface{}) {
				sc.runner.Run()
			},
		},
	)
}

// Endpoints returns endpoint objects.
func (sc *crdSource) Endpoints() ([]*endpoint.Endpoint, error) {
	endpoints := []*endpoint.Endpoint{}

	crds, err := sc.crdInformer.Lister().ByNamespace(sc.namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}


	for _, dnsEndpoint := range crds {
		// Make sure that all endpoints have targets for A or CNAME type
		crdEndpoints := []*endpoint.Endpoint{}
		for _, ep := range dnsEndpoint.Spec.Endpoints {
			if (ep.RecordType == "CNAME" || ep.RecordType == "A" || ep.RecordType == "AAAA") && len(ep.Targets) < 1 {
				log.Warnf("Endpoint %s with DNSName %s has an empty list of targets", dnsEndpoint.ObjectMeta.Name, ep.DNSName)
				continue
			}

			illegalTarget := false
			for _, target := range ep.Targets {
				if strings.HasSuffix(target, ".") {
					illegalTarget = true
					break
				}
			}
			if illegalTarget {
				log.Warnf("Endpoint %s with DNSName %s has an illegal target. The subdomain must consist of lower case alphanumeric characters, '-' or '.', and must start and end with an alphanumeric character (e.g. 'example.com')", dnsEndpoint.ObjectMeta.Name, ep.DNSName)
				continue
			}

			if ep.Labels == nil {
				ep.Labels = endpoint.NewLabels()
			}

			crdEndpoints = append(crdEndpoints, ep)
		}

		sc.setResourceLabel(&dnsEndpoint, crdEndpoints)
		endpoints = append(endpoints, crdEndpoints...)

		if dnsEndpoint.Status.ObservedGeneration == dnsEndpoint.Generation {
			continue
		}

		dnsEndpoint.Status.ObservedGeneration = dnsEndpoint.Generation
		// Update the ObservedGeneration
		_, err = sc.UpdateStatus(&dnsEndpoint)
		if err != nil {
			log.Warnf("Could not update ObservedGeneration of the CRD: %v", err)
		}
	}

	return endpoints, nil
}

func (sc *crdSource) setResourceLabel(crd *endpoint.DNSEndpoint, endpoints []*endpoint.Endpoint) {
	for _, ep := range endpoints {
		ep.Labels[endpoint.ResourceLabelKey] = fmt.Sprintf("crd/%s/%s", crd.ObjectMeta.Namespace, crd.ObjectMeta.Name)
	}
}