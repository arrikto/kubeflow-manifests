package tests_test

import (
	"sigs.k8s.io/kustomize/k8sdeps/kunstruct"
	"sigs.k8s.io/kustomize/k8sdeps/transformer"
	"sigs.k8s.io/kustomize/pkg/fs"
	"sigs.k8s.io/kustomize/pkg/loader"
	"sigs.k8s.io/kustomize/pkg/resmap"
	"sigs.k8s.io/kustomize/pkg/resource"
	"sigs.k8s.io/kustomize/pkg/target"
	"testing"
)

func writeKatibUiBase(th *KustTestHarness) {
	th.writeF("/manifests/katib-v1alpha2/katib-ui/base/katib-ui-deployment.yaml", `
apiVersion: extensions/v1beta1
kind: Deployment
metadata:
  name: katib-ui
  labels:
    app: katib
    component: ui
spec:
  replicas: 1
  template:
    metadata:
      name: katib-ui
      labels:
        app: katib
        component: ui
    spec:
      containers:
      - name: katib-ui
        image: gcr.io/kubeflow-images-public/katib/v1alpha2/katib-ui:v0.1.2-alpha-289-g14dad8b
        imagePullPolicy: IfNotPresent
        command:
          - './katib-ui'
        ports:
        - name: ui
          containerPort: 80
      serviceAccountName: katib-ui
`)
	th.writeF("/manifests/katib-v1alpha2/katib-ui/base/katib-ui-rbac.yaml", `
kind: ClusterRole
apiVersion: rbac.authorization.k8s.io/v1
metadata:
  name: katib-ui
rules:
- apiGroups:
  - ""
  resources:
  - configmaps
  verbs:
  - "*"
- apiGroups:
  - kubeflow.org
  resources:
  - experiments
  - trials
  verbs:
  - "*"
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: katib-ui
---
kind: ClusterRoleBinding
apiVersion: rbac.authorization.k8s.io/v1
metadata:
  name: katib-ui
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: katib-ui
subjects:
- kind: ServiceAccount
  name: katib-ui
`)
	th.writeF("/manifests/katib-v1alpha2/katib-ui/base/katib-ui-service.yaml", `
apiVersion: v1
kind: Service
metadata:
  name: katib-ui
  labels:
    app: katib
    component: ui
spec:
  type: ClusterIP
  ports:
    - port: 80
      protocol: TCP
      name: ui
  selector:
    app: katib
    component: ui
`)
	th.writeF("/manifests/katib-v1alpha2/katib-ui/base/params.yaml", `
varReference:
- path: data/config
  kind: ConfigMap
- path: data/config
  kind: Deployment
- path: metadata/annotations/getambassador.io\/config
  kind: Service
`)
	th.writeF("/manifests/katib-v1alpha2/katib-ui/base/params.env", `
clusterDomain=cluster.local
`)
	th.writeK("/manifests/katib-v1alpha2/katib-ui/base", `
namespace: kubeflow
resources:
- katib-ui-deployment.yaml
- katib-ui-rbac.yaml
- katib-ui-service.yaml
configMapGenerator:
- name: katib-parameters
  env: params.env
generatorOptions:
  disableNameSuffixHash: true
images:
  - name: gcr.io/kubeflow-images-public/katib/v1alpha2/katib-ui
    newTag: v0.1.2-alpha-289-g14dad8b
vars:
- name: clusterDomain
  objref:
    kind: ConfigMap
    name: katib-parameters
    apiVersion: v1
  fieldref:
    fieldpath: data.clusterDomain
- name: namespace
  objref:
    kind: Service
    name: katib-ui
    apiVersion: v1
  fieldref:
    fieldpath: metadata.namespace
configurations:
- params.yaml
`)
}

func TestKatibUiBase(t *testing.T) {
	th := NewKustTestHarness(t, "/manifests/katib-v1alpha2/katib-ui/base")
	writeKatibUiBase(th)
	m, err := th.makeKustTarget().MakeCustomizedResMap()
	if err != nil {
		t.Fatalf("Err: %v", err)
	}
	targetPath := "../katib-v1alpha2/katib-ui/base"
	fsys := fs.MakeRealFS()
	_loader, loaderErr := loader.NewLoader(targetPath, fsys)
	if loaderErr != nil {
		t.Fatalf("could not load kustomize loader: %v", loaderErr)
	}
	rf := resmap.NewFactory(resource.NewFactory(kunstruct.NewKunstructuredFactoryImpl()))
	kt, err := target.NewKustTarget(_loader, rf, transformer.NewFactoryImpl())
	if err != nil {
		th.t.Fatalf("Unexpected construction error %v", err)
	}
	n, err := kt.MakeCustomizedResMap()
	if err != nil {
		t.Fatalf("Err: %v", err)
	}
	expected, err := n.EncodeAsYaml()
	th.assertActualEqualsExpected(m, string(expected))
}
