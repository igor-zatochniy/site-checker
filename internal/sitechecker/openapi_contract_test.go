package sitechecker

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestAPIEmptyListsMatchOpenAPI(t *testing.T) {
	repo := NewInMemoryMonitorRepository(NewNetworkPolicy(Config{}))
	assertEmptyListResponses(t, t.Context(), repo)
}

func assertEmptyListResponses(t *testing.T, ctx context.Context, repo MonitorRepository) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	service := NewMonitorService(repo, nil, NewMetrics("test", "test", "test", 0), AlertPolicy{}, logger)
	mux := http.NewServeMux()
	NewAPIHandler(service, "", logger).Register(mux)
	var spec struct {
		Components struct {
			Schemas map[string]openAPIObjectSchema `yaml:"schemas"`
		} `yaml:"components"`
	}
	if err := yaml.Unmarshal(openAPISpec, &spec); err != nil {
		t.Fatal(err)
	}
	check := func(path, schema string, total, offset int) {
		t.Helper()
		t.Run(path, func(t *testing.T) {
			if spec.Components.Schemas[schema].Properties["items"].Type != "array" {
				t.Fatal("OpenAPI list schema must require an array")
			}
			recorder := httptest.NewRecorder()
			mux.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil).WithContext(ctx))
			if recorder.Code != http.StatusOK {
				t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body)
			}
			var response struct {
				Items  json.RawMessage `json:"items"`
				Total  int             `json:"total"`
				Offset int             `json:"offset"`
				Limit  int             `json:"limit"`
			}
			if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			if string(response.Items) != "[]" {
				t.Fatalf("wire items = %s, want []", response.Items)
			}
			if response.Total != total || response.Offset != offset || response.Limit != defaultPageLimit {
				t.Fatalf("incorrect pagination metadata: %s", recorder.Body)
			}
		})
	}
	check("/api/v1/monitors", "MonitorListResponse", 0, 0)
	check("/api/v1/incidents", "IncidentListResponse", 0, 0)
	monitor, err := repo.Create(ctx, MonitorInput{
		URL: "http://1.1.1.1/empty-list-contract", IntervalSeconds: 60,
		TimeoutSeconds: 5, ExpectedStatus: 200,
	})
	if err != nil {
		t.Fatal(err)
	}
	check("/api/v1/monitors/"+monitor.ID+"/checks", "CheckListResponse", 0, 0)
	check("/api/v1/monitors?offset=1", "MonitorListResponse", 1, 1)
	check("/api/v1/monitors?offset=100", "MonitorListResponse", 1, 100)
	check("/api/v1/monitors/"+monitor.ID+"/checks?offset=100", "CheckListResponse", 0, 100)
	check("/api/v1/incidents?offset=100", "IncidentListResponse", 0, 100)
}

type openAPIObjectSchema struct {
	Ref                  string                         `yaml:"$ref"`
	Type                 string                         `yaml:"type"`
	AdditionalProperties *bool                          `yaml:"additionalProperties"`
	Required             []string                       `yaml:"required"`
	Properties           map[string]openAPIObjectSchema `yaml:"properties"`
	AllOf                []openAPIObjectSchema          `yaml:"allOf"`
}

func TestOpenAPIMonitorSchemaAcceptsAPIResponses(t *testing.T) {
	var spec struct {
		Components struct {
			Schemas map[string]openAPIObjectSchema `yaml:"schemas"`
		} `yaml:"components"`
	}
	if err := yaml.Unmarshal(openAPISpec, &spec); err != nil {
		t.Fatal(err)
	}
	schemas := spec.Components.Schemas
	for _, name := range []string{"MonitorInput", "MonitorPatch"} {
		schema := schemas[name]
		if schema.AdditionalProperties == nil || *schema.AdditionalProperties {
			t.Fatalf("%s must reject unknown request fields", name)
		}
	}

	cfg := testCheckerConfig(t)
	store := NewMonitorStore(NewNetworkPolicy(cfg))
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	service := NewMonitorService(NewInMemoryMonitorRepositoryFromStore(store), nil, NewMetrics("test", "test", "test", 0), AlertPolicy{}, logger)
	mux := http.NewServeMux()
	NewAPIHandler(service, "", logger).Register(mux)
	request := func(method, path, body string, wantStatus int) map[string]json.RawMessage {
		t.Helper()
		recorder := httptest.NewRecorder()
		mux.ServeHTTP(recorder, httptest.NewRequest(method, path, strings.NewReader(body)))
		if recorder.Code != wantStatus {
			t.Fatalf("%s %s status=%d body=%s", method, path, recorder.Code, recorder.Body)
		}
		var response map[string]json.RawMessage
		if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		return response
	}
	created := request(http.MethodPost, "/api/v1/monitors",
		`{"url":"https://example.com","interval_seconds":60,"timeout_seconds":5,"expected_status":200}`, http.StatusCreated)
	assertOpenAPIObjectShape(t, schemas, schemas["Monitor"], created)
	var id string
	if err := json.Unmarshal(created["id"], &id); err != nil {
		t.Fatal(err)
	}
	for _, response := range []map[string]json.RawMessage{
		request(http.MethodGet, "/api/v1/monitors/"+id, "", http.StatusOK),
		request(http.MethodPatch, "/api/v1/monitors/"+id, `{"enabled":false}`, http.StatusOK),
	} {
		assertOpenAPIObjectShape(t, schemas, schemas["Monitor"], response)
	}
	listed := request(http.MethodGet, "/api/v1/monitors", "", http.StatusOK)
	var items []map[string]json.RawMessage
	if err := json.Unmarshal(listed["items"], &items); err != nil || len(items) != 1 {
		t.Fatalf("list items=%v error=%v", items, err)
	}
	assertOpenAPIObjectShape(t, schemas, schemas["Monitor"], items[0])

	monitor, err := store.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	monitor.LastStatusCode, monitor.LastLatencyMS = 503, 42
	monitor.LastCheckedAt, monitor.LastError = time.Now().UTC(), "unexpected status"
	encoded, err := json.Marshal(monitor)
	if err != nil {
		t.Fatal(err)
	}
	var fullResponse map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fullResponse); err != nil {
		t.Fatal(err)
	}
	assertOpenAPIObjectShape(t, schemas, schemas["Monitor"], fullResponse)
	request(http.MethodPost, "/api/v1/monitors",
		`{"url":"https://other.example.com","interval_seconds":60,"timeout_seconds":5,"expected_status":200,"unknown":true}`, http.StatusBadRequest)
	request(http.MethodPatch, "/api/v1/monitors/"+id, `{"unknown":true}`, http.StatusBadRequest)
}

// Check each object branch separately: allOf does not merge the properties
// allowed by an additionalProperties:false branch.
func assertOpenAPIObjectShape(t *testing.T, schemas map[string]openAPIObjectSchema, schema openAPIObjectSchema, object map[string]json.RawMessage) {
	t.Helper()
	if schema.Ref != "" {
		name := strings.TrimPrefix(schema.Ref, "#/components/schemas/")
		resolved, ok := schemas[name]
		if !ok {
			t.Fatalf("unresolved schema reference %q", schema.Ref)
		}
		assertOpenAPIObjectShape(t, schemas, resolved, object)
	}
	for _, required := range schema.Required {
		if _, ok := object[required]; !ok {
			t.Errorf("response is missing required field %q", required)
		}
	}
	if schema.AdditionalProperties != nil && !*schema.AdditionalProperties {
		for name := range object {
			if _, ok := schema.Properties[name]; !ok {
				t.Errorf("response field %q is forbidden by a closed schema branch", name)
			}
		}
	}
	for _, branch := range schema.AllOf {
		assertOpenAPIObjectShape(t, schemas, branch, object)
	}
}
