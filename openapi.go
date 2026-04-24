package main

import (
	_ "embed"
	"net/http"
)

//go:embed api/openapi.yaml
var openAPISpec []byte

func RegisterOpenAPI(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/openapi.yaml", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
		_, _ = w.Write(openAPISpec)
	})
}
