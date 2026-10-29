# Kubernetes Deployment

This directory contains a split backend-platform deployment for Site Checker:

- API deployment: 2 replicas.
- Scheduler deployment: 1 replica.
- Worker deployment: 3 replicas by default.
- Optional Alert Dispatcher deployment: 1 replica for persisted webhook delivery.
- Optional local-demo overlay with PostgreSQL and RabbitMQ deployments.
- Optional KEDA scaling by RabbitMQ queue length.
- NetworkPolicy default-deny baseline with explicit ingress and egress allowances.
- ConfigMap, Services, probes, resource requests and limits.
- Non-root containers, dropped Linux capabilities, rolling updates, and graceful termination.
- Scheduler-owned bounded retention for expired PostgreSQL history.

The repository includes `secret.example.yaml` as a template only. Copy it to an untracked local overlay Secret before applying local manifests. The checked-in base stays secret-free so CI can render release artifacts and production deployments can provide secrets through External Secrets, SOPS, Sealed Secrets, or a managed secret store.

Apply locally:

```bash
mkdir -p deploy/kubernetes/local
cp deploy/kubernetes/secret.example.yaml deploy/kubernetes/local/secret.yaml
kubectl apply -f deploy/kubernetes/namespace.yaml
kubectl apply -f deploy/kubernetes/local/secret.yaml
kubectl apply -k deploy/kubernetes-overlays/local-demo/
```

The Kustomize base contains only Site Checker application roles; demo PostgreSQL and RabbitMQ are supplied exclusively by `kubernetes-overlays/local-demo`. The base also contains a fixed image tag and never uses `latest`.

For every previously unused `v*` Git tag, CI pushes the release image, records its registry digest, and creates a GitHub Release with a rendered manifest named `site-checker-kubernetes-vX.Y.Z.yaml`, the optional alert manifest, the managed-backend policy template, the SBOM, and SHA-256 checksums. Production deployments should apply the GitHub Release manifest because all Site Checker roles reference the immutable `image@sha256:...` value:

```bash
kubectl apply -f site-checker-kubernetes-vX.Y.Z.yaml
```

The release bundle also contains `site-checker-managed-backends-egress-vX.Y.Z.yaml`. Before deploying with managed PostgreSQL or RabbitMQ, replace its documentation-only CIDRs with the smallest environment-specific endpoint CIDRs, create the namespace and managed Secret, apply the egress policy, and then apply the digest-pinned application manifest:

```bash
kubectl apply -f deploy/kubernetes/namespace.yaml
# Configure site-checker-secret through the production secret manager first.
# Edit the two example CIDRs before applying this policy.
kubectl apply -f site-checker-managed-backends-egress-vX.Y.Z.yaml
kubectl apply -f site-checker-kubernetes-vX.Y.Z.yaml
```

The managed-backend policy permits API and Alert Dispatcher access to PostgreSQL on TCP 5432, plus Scheduler and Worker access to PostgreSQL and RabbitMQ on TCP 5432, 5671, and 5672. The checked-in TEST-NET CIDRs intentionally match no production backend and keep the template fail-closed until they are replaced. Include every address returned by managed-service DNS and update the policy before endpoint rotation.

### Startup and migration budget

Every application role, including the optional Alert Dispatcher, has a `/healthz` startup probe with a six-minute window (`periodSeconds: 10`, `failureThreshold: 36`). Liveness and readiness probes begin only after startup succeeds. The HTTP listener opens after repository initialization, migrations, schema verification, the initial monitor count, and RabbitMQ initialization where required.

The ConfigMap explicitly sets `DATABASE_MIGRATION_TIMEOUT=2m` and `DATABASE_OPERATION_TIMEOUT=15s`. Allow for two separate migration-timeout budgets: PostgreSQL connection initialization and the migration transaction. With the schema check and initial count (two operation budgets), RabbitMQ connection (`5s`), and a `30s` margin, the default worst-case startup allowance is `305s`. The startup probe covers this allowance without increasing the runtime liveness timeout.

If these timeouts are increased or startup seeding is enabled, increase the startup probe window for every affected role as well. Keep the Deployment `progressDeadlineSeconds` (default `600s`) longer than the startup window and readiness delay. A startup probe does not extend the application's own migration timeout; a migration that exceeds that timeout still fails startup and rolls back.

Use a completed `v0.1.5` or newer release for new production deployments. The `v0.1.4` image does not include RabbitMQ startup-error credential protection or the empty-list API response fixes. The immutable `v0.1.3` artifacts also predate both these startup probes and the current Go security updates; changing YAML alone cannot update a released binary. Upgrade through a new release and follow the publication-fencing procedure below instead of overwriting the old image or release assets.

### Безпечне оновлення publication fencing

Перед першим оновленням із міграцією `009_publication_claim.sql` зупиніть **усі scheduler-процеси старої версії**, включно з іншими кластерами, зовнішніми процесами та роллю `all`. Дочекайтеся їх повного завершення: старий scheduler не перевіряє `publication_token` і може змінити стан завдання, вже захопленого новою версією. API та workers можуть завершувати наявну роботу під час цієї паузи.

Поки старі scheduler-процеси зупинені, застосуйте міграції новою версією з `RUN_MIGRATIONS=true`, а потім запустіть оновлені scheduler-процеси. Не відновлюйте стару версію scheduler після початку нових publication claims.

Перед оновленням перевірте резервну копію PostgreSQL. Міграція `009` лише додає колонку та її опис; вона не видаляє завдання чи історію перевірок. Чинна retention-політика продовжує діяти й не замінює резервне копіювання.

### Interrupted release publication

A release is deployable only after its CI run succeeds and the GitHub Release contains the complete manifest, digest, SBOM, and checksum bundle. A registry tag alone is not a completed release.

If CI stops after pushing the version image but before completing the GitHub Release, the current workflow deliberately refuses to reuse that tag. Keep the previous completed release deployed. Prepare a new patch version, align both Kustomizations and all four application image tags, commit that change, and run the release workflow through a new, unused Git tag. Do not delete or move the old Git tag, overwrite registry images, or replace published release assets to make a rerun pass. This is the supported recovery procedure; automatic resumption of a partially published version is not implemented.

Check workloads:

```bash
kubectl -n site-checker get pods
kubectl -n site-checker get svc
kubectl -n site-checker get networkpolicy
```

Optional KEDA scaling requires the KEDA operator and CRDs:

The optional `keda/rabbitmq-ingress.yaml` admits only pods labelled `app.kubernetes.io/name=keda-operator` in the `keda` namespace to the bundled RabbitMQ pods on TCP 5672. These selectors match the default KEDA Helm installation. If the operator uses a different namespace or pod label, adjust both selectors before applying the directory. They must remain in the same `from` entry so the namespace and pod restrictions are combined.

For managed RabbitMQ, this local-broker ingress policy has no effect: allow the KEDA operator to reach the managed AMQP endpoint through its namespace egress policy and the broker firewall. Application egress rules in the `site-checker` namespace do not grant access to the KEDA operator.

```bash
kubectl -n keda get pods -l app.kubernetes.io/name=keda-operator
kubectl apply -f deploy/kubernetes/keda/
kubectl -n site-checker wait --for=condition=Ready scaledobject/site-checker-worker --timeout=120s
kubectl -n site-checker describe scaledobject site-checker-worker
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

The PostgreSQL and RabbitMQ manifests in the local-demo overlay are suitable only for local demonstration. Production must use managed services or hardened StatefulSets with backups, persistence, TLS, monitoring, and secret rotation. External managed services also require the managed-backends egress policy described above; the default-deny base intentionally does not allow arbitrary database or broker destinations.

The base NetworkPolicy allows checked and webhook destinations through public IPv4 ranges on TCP 80/443 only. It intentionally has no IPv6 egress rule because Kubernetes NetworkPolicy cannot express the same public-only boundary without an explicit IPv6 CIDR policy. Clusters that require IPv6 monitoring must add CNI-specific IPv6 rules with equivalent exclusions for loopback, link-local, unique-local, documentation, multicast, and other non-public ranges before enabling IPv6 destinations.

The included `secret.example.yaml` uses local-demo placeholder values only. Production deployments should use External Secrets Operator, SOPS, Sealed Secrets, or a managed secret store.

`ALLOW_PRIVATE_NETWORKS=true` alone does not override Kubernetes egress policy. To monitor a trusted private destination, edit the CIDR and ports in `../kubernetes-overlays/trusted-private-egress/trusted-private-egress.yaml`, then apply that overlay instead of the base:

```bash
kubectl apply -k deploy/kubernetes-overlays/trusted-private-egress/
```

Keep each `ipBlock` limited to the required destination range. The overlay enables application trust mode and adds matching egress only for Worker and Alert Dispatcher pods; it is intentionally not part of the default deployment.

The base does not start the optional Alert Dispatcher. Set a non-empty `ALERT_WEBHOOK_URL` in the managed Secret before enabling it; the process intentionally fails startup when this required setting is missing. Then apply the optional deployment:

```bash
kubectl -n site-checker rollout restart deployment/site-checker-worker
kubectl -n site-checker rollout status deployment/site-checker-worker
kubectl apply -k deploy/kubernetes/alerts/
```

Workers create transactional alert events only when the same webhook setting is configured for them. Kubernetes does not refresh environment variables in running containers after a Secret update, so the worker rollout is mandatory when alerts are enabled or the webhook value changes after initial deployment.
For production releases, apply the digest-pinned `site-checker-alerts-vX.Y.Z.yaml` GitHub Release asset instead of the source overlay.

The scheduler enables retention with these defaults: check results 90 days, terminal jobs and delivered/dead outbox events 30 days, and resolved incidents 365 days. Cleanup deletes at most 10,000 rows per table each minute and never removes active jobs, pending/processing alerts, open incidents, or incidents that still have outbox rows. Adjust the retention values in `configmap.yaml` before deployment when longer history is required.

Remove:

```bash
kubectl delete namespace site-checker
```
