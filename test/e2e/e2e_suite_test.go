//go:build e2e
// +build e2e

/*
Copyright 2026.

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

package e2e

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/rogeeoh/soft-drain/test/utils"
)

// managerImage is the manager image to be built and loaded for testing.
var managerImage = "example.com/soft-drain:v0.0.1"

// workloadImage is the image used for the workloads that get drained.
const workloadImage = "nginx:alpine"

func kindClusterName() string {
	if v := os.Getenv("KIND_CLUSTER"); v != "" {
		return v
	}
	return "soft-drain-test-e2e"
}

func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	_, _ = fmt.Fprintf(GinkgoWriter, "Starting soft-drain e2e test suite\n")
	RunSpecs(t, "e2e suite")
}

var _ = BeforeSuite(func() {
	// Real-cluster guard 1: build a suite-owned kubeconfig and pin it for the whole process.
	// Relying on the user kubeconfig's current-context means a context switched from another
	// shell mid-run would send deploy/undeploy to that cluster.
	kubeconfig := filepath.Join(GinkgoT().TempDir(), "kubeconfig")
	_, err := utils.Run(exec.Command("kind", "export", "kubeconfig",
		"--name", kindClusterName(), "--kubeconfig", kubeconfig))
	Expect(err).NotTo(HaveOccurred(), "Failed to export the kind cluster kubeconfig")
	os.Setenv("KUBECONFIG", kubeconfig)

	// Real-cluster guard 2: verify the pinned kubeconfig really points at the dedicated kind cluster
	out, err := utils.Run(exec.Command("kubectl", "config", "current-context"))
	Expect(err).NotTo(HaveOccurred(), "Failed to read current kubectl context")
	Expect(strings.TrimSpace(out)).To(Equal("kind-"+kindClusterName()),
		"e2e must run against the dedicated kind cluster, current context is %q", strings.TrimSpace(out))

	By("building the manager image")
	cmd := exec.Command("make", "docker-build", fmt.Sprintf("IMG=%s", managerImage))
	_, err = utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to build the manager image")

	By("loading the manager image on Kind")
	Expect(utils.LoadImageToKindClusterWithName(managerImage)).To(Succeed(),
		"Failed to load the manager image into Kind")

	By("preloading the workload image on Kind")
	if _, err := utils.Run(exec.Command("docker", "pull", workloadImage)); err == nil {
		_ = utils.LoadImageToKindClusterWithName(workloadImage)
	}
})
