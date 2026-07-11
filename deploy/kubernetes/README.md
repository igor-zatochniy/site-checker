# Kubernetes Deployment

This directory contains a production-style baseline deployment for the current Site Checker binary:

- `Deployment`
- `Service`
- `ConfigMap`
- `Secret`
- resource requests and limits
- liveness and readiness probes
- non-root `securityContext`
- dropped Linux capabilities
- rolling update settings
- graceful termination window

The current application keeps monitor state in memory, so this manifest intentionally uses `replicas: 1`. Scale-out with separate API, scheduler, and worker workloads should be introduced together with PostgreSQL and RabbitMQ so multiple replicas do not duplicate checks or lose monitor state.

Apply locally:

```bash
kubectl apply -f deploy/kubernetes/
```

Remove:

```bash
kubectl delete namespace site-checker
```
