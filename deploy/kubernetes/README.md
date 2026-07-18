# Kubernetes Deployment

This directory contains a split backend-platform deployment for Site Checker:

- API deployment: 2 replicas.
- Scheduler deployment: 1 replica.
- Worker deployment: 3 replicas by default.
- Alert Dispatcher deployment: 1 replica for persisted webhook delivery.
- PostgreSQL demo deployment for monitors, check history, incidents, and the alert outbox.
- RabbitMQ demo deployment for check jobs and dead-letter handling.
- Optional KEDA scaling by RabbitMQ queue length.
- NetworkPolicy default-deny baseline with explicit ingress and egress allowances.
- ConfigMap, Services, probes, resource requests and limits.
- Non-root containers, dropped Linux capabilities, rolling updates, and graceful termination.

The repository includes `secret.example.yaml` as a template only. Create an untracked `secret.yaml` from it, or use External Secrets / SOPS / Sealed Secrets, before applying the base.

Apply locally:

```bash
kubectl apply -k deploy/kubernetes/
```

The Kustomize base contains a fixed image tag and never uses `latest`. For every `v*` Git tag, CI pushes the release image, records its registry digest, and uploads a rendered manifest named `site-checker-kubernetes-vX.Y.Z.yaml`. Production deployments should apply that artifact because all Site Checker roles reference the immutable `image@sha256:...` value:

```bash
kubectl apply -f site-checker-kubernetes-vX.Y.Z.yaml
```

Check workloads:

```bash
kubectl -n site-checker get pods
kubectl -n site-checker get svc
kubectl -n site-checker get networkpolicy
```

Optional KEDA scaling requires the KEDA operator and CRDs:

```bash
kubectl apply -f deploy/kubernetes/keda/
kubectl -n site-checker get scaledobject
```

Scale demonstration:

```bash
kubectl -n site-checker scale deployment/site-checker-worker --replicas=1
kubectl -n site-checker scale deployment/site-checker-worker --replicas=3
kubectl -n site-checker scale deployment/site-checker-worker --replicas=6
```

Expected behavior:

- 1 worker: queue backlog can grow under load.
- 3 workers: queue backlog should stabilize for moderate load.
- 6 workers: backlog should drain faster if outbound checks are the bottleneck.

The included PostgreSQL and RabbitMQ manifests are suitable for local demonstration. For production, prefer managed PostgreSQL and RabbitMQ or hardened StatefulSets with backups, persistence, TLS, monitoring, and secret rotation.

The included `Secret` uses local-demo placeholder values. Production deployments should use External Secrets Operator, SOPS, Sealed Secrets, or a managed secret store.

Set `ALERT_WEBHOOK_URL` in the managed secret to enable transactional webhook alerts. When it is empty, workers do not create alert events and the dispatcher remains idle.

Remove:

```bash
kubectl delete namespace site-checker
```
