.PHONY: test race vet staticcheck vulncheck build tidy-check integration docker-build docker-smoke k8s-dry-run compose-up compose-down ci

IMAGE ?= site-checker:local
CONTAINER ?= site-checker-smoke
KUBECTL ?= kubectl

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...

staticcheck:
	staticcheck ./...

vulncheck:
	govulncheck ./...

build:
	go build ./...

tidy-check:
	go mod tidy -diff

integration:
	go test -tags=integration ./...

docker-build:
	docker build -t $(IMAGE) .

docker-smoke: docker-build
	-docker rm -f $(CONTAINER)
	docker run -d --name $(CONTAINER) -e APP_ROLE=api -e API_KEY=secret -e HEALTH_ADDR=:8080 $(IMAGE)
	docker exec $(CONTAINER) wget -qO- http://127.0.0.1:8080/healthz
	docker exec $(CONTAINER) wget -qO- http://127.0.0.1:8080/readyz
	docker exec $(CONTAINER) wget -qO- http://127.0.0.1:8080/metrics
	docker exec $(CONTAINER) wget -qO- --header='X-API-Key: secret' http://127.0.0.1:8080/api/v1/monitors
	docker stop -t 30 $(CONTAINER)
	docker rm $(CONTAINER)

k8s-dry-run:
	$(KUBECTL) apply --dry-run=client -k deploy/kubernetes/

compose-up:
	docker compose up --build

compose-down:
	docker compose down -v

ci: test race vet staticcheck vulncheck build tidy-check
