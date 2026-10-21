package sitechecker

import (
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
