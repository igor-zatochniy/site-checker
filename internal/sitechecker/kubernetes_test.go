package sitechecker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

type deploymentProbe struct {
	HTTPGet struct {
		Path string
		Port string
	} `yaml:"httpGet"`
	InitialDelaySeconds int `yaml:"initialDelaySeconds"`
	PeriodSeconds       int `yaml:"periodSeconds"`
	FailureThreshold    int `yaml:"failureThreshold"`
}

type deploymentManifest struct {
	Kind     string
	Metadata struct{ Name string }
	Data     map[string]string
	Spec     struct {
		ProgressDeadlineSeconds int `yaml:"progressDeadlineSeconds"`
		Template                struct {
			Spec struct {
				Containers []struct {
					Env            []struct{ Name, Value string }
					StartupProbe   *deploymentProbe `yaml:"startupProbe"`
					LivenessProbe  *deploymentProbe `yaml:"livenessProbe"`
					ReadinessProbe *deploymentProbe `yaml:"readinessProbe"`
				}
			}
		}
	}
}

func TestKubernetesStartupBudgets(t *testing.T) {
	base := filepath.Join("..", "..", "deploy", "kubernetes")
	config := decodeDeploymentManifests(t, readTestFile(t, filepath.Join(base, "configmap.yaml")))[0].Data
	for _, path := range []string{"api-deployment.yaml", "scheduler-deployment.yaml", "worker-deployment.yaml", "alerts/deployment.yaml"} {
		t.Run(path, func(t *testing.T) {
			manifest := decodeDeploymentManifests(t, readTestFile(t, filepath.Join(base, filepath.FromSlash(path))))[0]
			assertDeploymentStartupBudget(t, manifest, config)
		})
	}
}

func TestKubernetesRenderedStartupBudgets(t *testing.T) {
	if os.Getenv("KUBERNETES_RENDER_TEST") != "1" {
		t.Skip("set KUBERNETES_RENDER_TEST=1 to validate rendered deployment paths with kubectl")
	}
	baseConfig := decodeDeploymentManifests(t, readTestFile(t, filepath.Join("..", "..", "deploy", "kubernetes", "configmap.yaml")))[0].Data
	for _, tc := range []struct {
		path  string
		roles int
	}{
		{"deploy/kubernetes", 3},
		{"deploy/kubernetes-overlays/local-demo", 3},
		{"deploy/kubernetes-overlays/trusted-private-egress", 3},
		{"deploy/kubernetes/alerts", 1},
	} {
		t.Run(tc.path, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, "kubectl", "kustomize", tc.path)
			cmd.Dir = filepath.Join("..", "..")
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("render manifests: %v\n%s", err, output)
			}
			manifests := decodeDeploymentManifests(t, string(output))
			config := baseConfig
			for _, manifest := range manifests {
				if manifest.Kind == "ConfigMap" && manifest.Metadata.Name == "site-checker-config" {
					config = manifest.Data
				}
			}
			roles := 0
			for _, manifest := range manifests {
				if manifest.Kind != "Deployment" || manifest.Metadata.Name == "site-checker-postgres" || manifest.Metadata.Name == "site-checker-rabbitmq" {
					continue
				}
				assertDeploymentStartupBudget(t, manifest, config)
				roles++
			}
			if roles != tc.roles {
				t.Fatalf("checked %d application roles, want %d", roles, tc.roles)
			}
		})
	}
}

func decodeDeploymentManifests(t *testing.T, content string) []deploymentManifest {
	t.Helper()
	decoder := yaml.NewDecoder(strings.NewReader(content))
	var manifests []deploymentManifest
	for {
		var manifest deploymentManifest
		if err := decoder.Decode(&manifest); errors.Is(err, io.EOF) {
			return manifests
		} else if err != nil {
			t.Fatal(err)
		}
		manifests = append(manifests, manifest)
	}
}

func assertDeploymentStartupBudget(t *testing.T, manifest deploymentManifest, config map[string]string) {
	t.Helper()
	if manifest.Kind != "Deployment" || len(manifest.Spec.Template.Spec.Containers) != 1 {
		t.Fatalf("%s: expected one application container", manifest.Metadata.Name)
	}
	container := manifest.Spec.Template.Spec.Containers[0]
	settings := maps.Clone(config)
	for _, env := range container.Env {
		settings[env.Name] = env.Value
	}
	role := settings["APP_ROLE"]
	if !slices.Contains([]string{"api", "scheduler", "worker", "alert-dispatcher"}, role) {
		t.Fatalf("%s: unexpected role %q", manifest.Metadata.Name, role)
	}
	duration := func(name string) time.Duration {
		value, err := time.ParseDuration(settings[name])
		if err != nil || value <= 0 {
			t.Fatalf("%s: invalid or missing %s: %q", manifest.Metadata.Name, name, settings[name])
		}
		return value
	}
	// Connection initialization and migrations have separate deadlines; schema
	// verification and the initial monitor count each consume an operation budget.
	budget := duration("DATABASE_MIGRATION_TIMEOUT") + 2*duration("DATABASE_OPERATION_TIMEOUT")
	if settings["RUN_MIGRATIONS"] == "true" {
		budget += duration("DATABASE_MIGRATION_TIMEOUT")
	}
	if role == "worker" || role == "scheduler" {
		budget += duration("RABBITMQ_CONNECT_TIMEOUT")
	}
	budget += 30 * time.Second
	if settings["SEED_DEFAULT_LINKS"] != "false" || settings["SEED_URLS_FILE"] != "" {
		t.Fatal("startup seeding needs an additional explicit startup budget")
	}
	if err := validateStartupProbe(container.StartupProbe, budget); err != nil {
		t.Fatalf("%s: %v", manifest.Metadata.Name, err)
	}
	if container.LivenessProbe == nil || container.LivenessProbe.HTTPGet != container.StartupProbe.HTTPGet ||
		container.ReadinessProbe == nil || container.ReadinessProbe.HTTPGet.Path != "/readyz" {
		t.Fatalf("%s: startup, liveness, or readiness endpoint is incorrect", manifest.Metadata.Name)
	}
	progressDeadline := manifest.Spec.ProgressDeadlineSeconds
	if progressDeadline == 0 {
		progressDeadline = 600
	}
	startupWindow := container.StartupProbe.InitialDelaySeconds + container.StartupProbe.PeriodSeconds*container.StartupProbe.FailureThreshold
	if progressDeadline <= startupWindow+container.ReadinessProbe.InitialDelaySeconds+container.ReadinessProbe.PeriodSeconds {
		t.Fatalf("%s: rollout deadline does not cover startup and readiness", manifest.Metadata.Name)
	}
}

func validateStartupProbe(probe *deploymentProbe, budget time.Duration) error {
	if probe == nil || probe.HTTPGet.Path != "/healthz" || probe.HTTPGet.Port != "http" {
		return errors.New("startup probe must check /healthz on the application HTTP port")
	}
	if probe.PeriodSeconds <= 0 || probe.FailureThreshold <= 0 {
		return errors.New("startup probe needs explicit positive period and failure threshold")
	}
	// Account conservatively for a first probe that fails immediately.
	earliestFailure := time.Duration(probe.InitialDelaySeconds+(probe.FailureThreshold-1)*probe.PeriodSeconds) * time.Second
	if earliestFailure <= budget {
		return fmt.Errorf("startup probe can restart the container at %s, before startup budget %s", earliestFailure, budget)
	}
	return nil
}

func TestStartupProbeRejectsMigrationOnlyBudget(t *testing.T) {
	probe := &deploymentProbe{PeriodSeconds: 10, FailureThreshold: 18}
	probe.HTTPGet.Path, probe.HTTPGet.Port = "/healthz", "http"
	budget := 2*defaultDatabaseMigrationTimeout + 2*defaultDatabaseOperationTimeout + defaultRabbitMQConnectTimeout + 30*time.Second
	if err := validateStartupProbe(nil, budget); err == nil {
		t.Fatal("missing startup probe accepted")
	}
	if err := validateStartupProbe(probe, budget); err == nil {
		t.Fatal("a three-minute probe incorrectly covers two separate two-minute database deadlines")
	}
	probe.FailureThreshold = 36
	if err := validateStartupProbe(probe, budget); err != nil {
		t.Fatal(err)
	}
}

func TestKEDABrokerIngressIsScopedToOperator(t *testing.T) {
	type selector struct {
		MatchLabels map[string]string `yaml:"matchLabels"`
	}
	var policy struct {
		Kind     string
		Metadata struct{ Namespace string }
		Spec     struct {
			PodSelector selector `yaml:"podSelector"`
			PolicyTypes []string `yaml:"policyTypes"`
			Ingress     []struct {
				From []struct {
					NamespaceSelector selector `yaml:"namespaceSelector"`
					PodSelector       selector `yaml:"podSelector"`
				}
				Ports []struct {
					Protocol string
					Port     int
				}
			}
		}
	}
	content := readTestFile(t, filepath.Join("..", "..", "deploy", "kubernetes", "keda", "rabbitmq-ingress.yaml"))
	if err := yaml.Unmarshal([]byte(content), &policy); err != nil {
		t.Fatal(err)
	}
	if policy.Kind != "NetworkPolicy" || policy.Metadata.Namespace != "site-checker" ||
		!maps.Equal(policy.Spec.PodSelector.MatchLabels, map[string]string{"app.kubernetes.io/name": "site-checker", "app.kubernetes.io/component": "rabbitmq"}) ||
		!slices.Equal(policy.Spec.PolicyTypes, []string{"Ingress"}) || len(policy.Spec.Ingress) != 1 {
		t.Fatal("KEDA ingress must target only the local Site Checker broker")
	}
	rule := policy.Spec.Ingress[0]
	if len(rule.From) != 1 ||
		!maps.Equal(rule.From[0].NamespaceSelector.MatchLabels, map[string]string{"kubernetes.io/metadata.name": "keda"}) ||
		!maps.Equal(rule.From[0].PodSelector.MatchLabels, map[string]string{"app.kubernetes.io/name": "keda-operator"}) {
		t.Fatal("KEDA namespace and operator selectors must be combined in one peer")
	}
	if len(rule.Ports) != 1 || rule.Ports[0].Protocol != "TCP" || rule.Ports[0].Port != 5672 {
		t.Fatal("KEDA ingress must allow only AMQP on TCP 5672")
	}
}
