package main

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

const saDir = "/var/run/secrets/kubernetes.io/serviceaccount/"

// kube is a minimal in-cluster Kubernetes API client: just enough to read a
// Deployment's status and scale the Redis Deployment for the outage demo.
// Permissions are limited by the Role in console.yaml.
type kube struct {
	ns string
	c  *http.Client
	ok bool
}

func newKube(ns string) *kube {
	k := &kube{ns: ns}
	ca, err := os.ReadFile(saDir + "ca.crt")
	if err != nil {
		return k // not running in a cluster: outage controls disabled
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(ca)
	k.c = &http.Client{Timeout: 3 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}}
	k.ok = true
	return k
}

func (k *kube) do(method, path, ctype string, body []byte, out any) error {
	if !k.ok {
		return fmt.Errorf("not running inside Kubernetes")
	}
	tok, err := os.ReadFile(saDir + "token") // re-read: projected tokens rotate
	if err != nil {
		return err
	}
	req, _ := http.NewRequest(method, "https://kubernetes.default.svc"+path, bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+string(tok))
	if ctype != "" {
		req.Header.Set("Content-Type", ctype)
	}
	resp, err := k.c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("kubernetes API %s %s: %s", method, path, resp.Status)
	}
	if out != nil {
		return json.Unmarshal(b, out)
	}
	return nil
}

type deployStatus struct {
	Desired int `json:"desired"`
	Ready   int `json:"ready"`
}

func (k *kube) deployment(name string) (deployStatus, error) {
	var d struct {
		Spec   struct{ Replicas int } `json:"spec"`
		Status struct {
			ReadyReplicas int `json:"readyReplicas"`
		} `json:"status"`
	}
	err := k.do("GET", "/apis/apps/v1/namespaces/"+k.ns+"/deployments/"+name, "", nil, &d)
	return deployStatus{d.Spec.Replicas, d.Status.ReadyReplicas}, err
}

func (k *kube) scale(name string, n int) error {
	body := []byte(fmt.Sprintf(`{"spec":{"replicas":%d}}`, n))
	return k.do("PATCH", "/apis/apps/v1/namespaces/"+k.ns+"/deployments/"+name+"/scale",
		"application/merge-patch+json", body, nil)
}
