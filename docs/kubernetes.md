# Kubernetes Deployment

This guide covers deploying Trident self-hosted on Kubernetes using the official Helm chart.

## Prerequisites

| Requirement | Minimum version | Notes |
|-------------|----------------|-------|
| Kubernetes | 1.25+ | EKS, GKE, AKS, or self-hosted |
| Helm | 3.12+ | `brew install helm` |
| PostgreSQL | 15+ | Operator-managed (e.g. CloudNativePG) or managed service |
| Redis | 7+ | Operator-managed (e.g. Redis Operator) or managed service |

Trident's Helm chart packages the four stateless services only — it does **not** bundle Postgres or Redis. Provision those separately before installing the chart.

## Quick Start

### 1. Add the chart repository (once published)

```bash
helm repo add trident https://telocel-labs.github.io/trident
helm repo update
```

For now, install directly from the cloned repo:

```bash
git clone https://github.com/telocel-labs/trident
cd trident
```

### 2. Create the secrets

Trident uses the `existingSecret` pattern — sensitive values are read from a Kubernetes Secret rather than passed through Helm values.

```bash
kubectl create secret generic trident-secrets \
  --from-literal=DATABASE_URL="postgres://trident:password@postgres-host:5432/trident" \
  --from-literal=REDIS_URL="redis://redis-host:6379" \
  --from-literal=ADMIN_API_KEY="$(openssl rand -hex 32)"
```

### 3. Install the chart

```bash
helm install trident ./helm/trident \
  --namespace trident \
  --create-namespace \
  --set goApi.image.tag=v0.1.0 \
  --set indexer.image.tag=v0.1.0 \
  --set grpcApi.image.tag=v0.1.0
```

This first runs a migration Job to bring the schema up to date, then rolls
out the app Deployments only once it succeeds — see
[Database migrations](#migrations) below for details, including how to skip
it if migrations are managed externally.

### 4. Verify the deployment

```bash
kubectl -n trident get pods
kubectl -n trident get hpa
```

Expected output:

```
NAME                                    READY   STATUS    RESTARTS   AGE
trident-go-api-7d9f8c4b5-abcde         1/1     Running   0          2m
trident-go-api-7d9f8c4b5-fghij         1/1     Running   0          2m
trident-grpc-api-6c8b7d5f4-klmno       1/1     Running   0          2m
trident-indexer-5b4d9c3a2-pqrst        1/1     Running   0          2m
trident-nginx-4a3c8b7d6-uvwxy          1/1     Running   0          2m
```

## Database migrations {#migrations}

Before any app Deployment (go-api/grpc-api/indexer) rolls out, `helm
install`/`helm upgrade` runs a `pre-install,pre-upgrade` Helm hook Job
(`helm/trident/templates/migration-job.yaml`) that applies
`database/migrations/*.sql` against `DATABASE_URL` (read from
`global.existingSecret`, same as every other component). This guarantees a
deterministic schema-before-app ordering (issue #308) — you never get app
pods starting against a schema they don't expect.

### Why this is safe to run on every install/upgrade

The Job's image (built from `database/Dockerfile`) runs `sqlx migrate run
--source /migrations`, not a raw re-apply loop. `sqlx migrate run` records
every applied migration in the database's own `_sqlx_migrations` tracking
table, so re-running the Job on every `helm upgrade` only applies migrations
that aren't already recorded there — a true no-op when nothing changed.
This is the same mechanism `make migrate` uses locally when `sqlx-cli` is
installed (see the Makefile's `migrate` target); the raw-`psql`-over-
`schema.sql`-plus-migrations fallback in that same target is only used
locally when `sqlx-cli` isn't available and is deliberately **not** what
this Helm hook runs, since it isn't idempotent.

### Hook behavior

- `helm.sh/hook-weight: "-5"` runs it ahead of every other resource in the
  chart.
- `helm.sh/hook-delete-policy: before-hook-creation,hook-succeeded` deletes
  a *successful* Job's Pod automatically (and any stale Job left from a
  prior failed release, before creating the new one) but **deliberately
  leaves a failed Job in place** — nothing in this chart marks the hook to
  ignore failures, so a failing migration fails `helm install`/`helm
  upgrade` outright and blocks the app Deployments from ever rolling out
  against a schema that didn't migrate cleanly.
- `restartPolicy: Never` with `backoffLimit: 2` bounds retries instead of
  looping forever on a bad migration.

### Debugging a failed migration Job

Because a failed Job is left behind (`hook-succeeded`, not `hook-failed`,
in the delete policy), you can inspect it after a failed release:

```bash
kubectl -n trident get jobs -l app.kubernetes.io/component=migrate
kubectl -n trident logs job/trident-migrate
kubectl -n trident describe job/trident-migrate
```

Once you've fixed the underlying issue (a bad migration file, an
unreachable database, etc.), delete the failed Job and re-run
`helm upgrade`/`helm install` — `before-hook-creation` will then replace it
with a fresh attempt:

```bash
kubectl -n trident delete job trident-migrate
helm upgrade trident ./helm/trident --reuse-values
```

### Disabling the hook for externally-managed migrations

If migrations are already applied by a separate CI/CD pipeline, a DBA-owned
process, or any other external mechanism, disable the chart's hook entirely
so it doesn't also try to apply them:

```bash
helm upgrade trident ./helm/trident --reuse-values --set migrations.enabled=false
```

or in `custom-values.yaml`:

```yaml
migrations:
  enabled: false
```

When disabled, the migration Job template renders nothing — no other
chart behavior changes.

## Configuration

### Using an Ingress controller instead of Nginx

Disable the bundled Nginx deployment and enable the Ingress resource:

```yaml
# custom-values.yaml
nginx:
  enabled: false

ingress:
  enabled: true
  className: nginx
  annotations:
    cert-manager.io/cluster-issuer: letsencrypt-prod
    # TLS termination + HSTS at the ingress-nginx controller (issue #320).
    nginx.ingress.kubernetes.io/ssl-redirect: "true"
    nginx.ingress.kubernetes.io/force-ssl-redirect: "true"
    nginx.ingress.kubernetes.io/hsts: "true"
    nginx.ingress.kubernetes.io/hsts-max-age: "31536000"
    nginx.ingress.kubernetes.io/hsts-include-subdomains: "true"
    nginx.ingress.kubernetes.io/hsts-preload: "true"
  host: api.trident.example.com
  tls:
    - secretName: trident-tls
      hosts:
        - api.trident.example.com
```

```bash
helm upgrade trident ./helm/trident -f custom-values.yaml
```

## TLS termination, HSTS, and internal mTLS {#tls}

All public traffic terminates TLS at the edge — either the bundled Nginx
deployment (`nginx.enabled: true`, the default) or the Ingress resource
above. Internal gRPC traffic between the Go API and the Rust gRPC service
stays inside the cluster network and is plaintext by default, with an
optional mTLS mode. This section documents both.

### Public-edge TLS + HSTS

- **Bundled Nginx** (`docker/nginx/nginx.conf`): listens on 80 (redirects to
  443) and 443 (TLS, `ssl_protocols TLSv1.2 TLSv1.3`, certs mounted at
  `/etc/nginx/certs/{fullchain,privkey}.pem`). Every 443 response now sends
  `Strict-Transport-Security: max-age=31536000; includeSubDomains; preload`
  so browsers refuse to downgrade this host to plain HTTP for a year,
  including its subdomains, and it can be submitted to browsers' HSTS
  preload lists once you're confident every subdomain is HTTPS-only.
- **Ingress** (`ingress.enabled: true`): configure the controller-level
  equivalent, as in the `custom-values.yaml` example above — the
  `nginx.ingress.kubernetes.io/hsts*` annotations for ingress-nginx, or the
  equivalent for your controller (ALB, GKE, etc.).

### Internal gRPC is not reachable externally

Neither the bundled Nginx `server {}` block nor the Ingress `rules[]` expose
a path to the gRPC port (`grpcApi.service.port`, default 5000/50051) — the
gRPC Service is `ClusterIP`, so it has no external address at all, and no
Ingress/Nginx rule forwards to it. The only externally reachable paths are
the ones the Go API's HTTP server explicitly registers (`/v1/*`, `/ws`), and
`/internal/*` is additionally denied at both layers as defense in depth (see
below) even though it's also served over the same ClusterIP-only path.

`/internal/status` (services/api/handlers/status.go, issue #316) is
internal-only in three independent layers:
1. The handler itself requires `X-Internal-Key` to match `INTERNAL_API_KEY`
   (constant-time compare; fails closed — an unset `INTERNAL_API_KEY` rejects
   every request, it never means "no auth required").
2. `docker/nginx/nginx.conf` has an explicit `location /internal/ { deny all; return 403; }`.
3. `helm/trident/templates/ingress.yaml` routes `/internal/` to a dedicated,
   more-specific path rule ahead of the catch-all `/`, so a controller-level
   deny (e.g. `nginx.ingress.kubernetes.io/configuration-snippet` returning
   403, or an equivalent NetworkPolicy) can target it precisely.

To verify in your own cluster:
```bash
# Should NOT succeed from outside the cluster:
curl -k https://api.trident.example.com/internal/status   # -> 403 (nginx/ingress deny)
# From inside the cluster, still requires the key:
kubectl run -it --rm curl --image=curlimages/curl --restart=Never -- \
  curl -s -o /dev/null -w '%{http_code}\n' http://trident-go-api:3000/internal/status  # -> 401 without X-Internal-Key
```

### Internal mTLS between the Go API and the Rust gRPC service (optional)

Off by default: `internalMTLS.enabled: false` in `values.yaml`, meaning
plaintext gRPC over the cluster-internal network (the model above already
ensures that network isn't reachable from outside). Turn it on for
defense-in-depth (e.g. compliance requirements, multi-tenant clusters, zero
trust network policies):

```yaml
# custom-values.yaml
internalMTLS:
  enabled: true
```

This requires five PEM-encoded keys to already exist in `global.existingSecret`
(populated via `kubectl create secret`, the external-secrets operator, or the
CSI driver — see [Secrets management](#secrets); never commit these to
`values.yaml`):

| Secret key | Used by | Purpose |
|---|---|---|
| `INTERNAL_CA_CERT` | both | CA bundle used to verify the peer's certificate |
| `INTERNAL_SERVER_CERT` / `INTERNAL_SERVER_KEY` | grpc-api | Server identity presented to the Go API |
| `INTERNAL_CLIENT_CERT` / `INTERNAL_CLIENT_KEY` | go-api | Client identity presented to the gRPC service |

When enabled, the chart mounts these (renamed to `ca.crt`/`server.crt`/etc.)
into both deployments at `internalMTLS.mountPath` (default
`/etc/trident/mtls`) and sets `GRPC_MTLS_ENABLED=true` plus the matching
`GRPC_MTLS_*` path env vars. The Rust gRPC server
(`crates/api/src/main.rs`) requires and verifies a client certificate via
`tonic::transport::ServerTlsConfig::client_ca_root`; the Go API
(`services/api/grpc/client.go`) presents a client certificate and verifies
the server's certificate against the same CA. If `GRPC_MTLS_ENABLED=true`
but any of the three required paths for that side are missing or unreadable,
both sides fail to start/connect rather than silently falling back to
plaintext.

**Generating certs for a first test** (self-signed CA, for non-production use):
```bash
openssl req -x509 -newkey rsa:4096 -days 365 -nodes \
  -keyout ca.key -out ca.crt -subj "/CN=trident-internal-ca"

for role in server client; do
  openssl req -newkey rsa:4096 -nodes -keyout ${role}.key -out ${role}.csr \
    -subj "/CN=trident-${role}"
  openssl x509 -req -in ${role}.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
    -out ${role}.crt -days 365
done

kubectl create secret generic trident-secrets \
  --from-literal=DATABASE_URL=... --from-literal=REDIS_URL=... --from-literal=ADMIN_API_KEY=... \
  --from-file=INTERNAL_CA_CERT=ca.crt \
  --from-file=INTERNAL_SERVER_CERT=server.crt --from-file=INTERNAL_SERVER_KEY=server.key \
  --from-file=INTERNAL_CLIENT_CERT=client.crt --from-file=INTERNAL_CLIENT_KEY=client.key
```
In production, issue these from a real CA (e.g. your organization's Vault PKI
secrets engine, or cert-manager's `Certificate`/`Issuer` CRDs targeting an
internal `ClusterIssuer`) rather than the ad hoc openssl commands above.

### Cert rotation

- **Public edge (Nginx)**: replace the `fullchain.pem`/`privkey.pem` files
  mounted at `/etc/nginx/certs/` (typically via a Secret volume) and reload
  Nginx (`nginx -s reload` or a rolling pod restart) — no downtime if
  `nginx.replicaCount > 1` or the PodDisruptionBudget is respected.
- **Public edge (Ingress + cert-manager)**: automatic — cert-manager renews
  well before expiry and updates the referenced `tls.secretName`; the
  ingress controller picks up the new cert without a restart.
- **Internal mTLS**: same rotation mechanics as any other value in
  `global.existingSecret` — see [Rotating secrets](#secrets) below. Because
  both the server and client sides re-read cert files from a mounted volume,
  a `kubectl rollout restart` of both `trident-go-api` and `trident-grpc-api`
  deployments is required after the Secret updates (unlike `DATABASE_URL`
  etc., these are read from disk once at process TLS-config time, not on
  every request) — projected/mounted Secret volumes update automatically
  within the kubelet's sync period, but the running process must be
  restarted to pick up the new files.

### Horizontal Pod Autoscaler

The Go API scales automatically between 2 and 10 replicas based on CPU utilisation (target: 70%). Configure via:

```yaml
goApi:
  hpa:
    minReplicas: 2
    maxReplicas: 10
    targetCPUUtilizationPercentage: 70
```

Ensure the [Metrics Server](https://github.com/kubernetes-sigs/metrics-server) is installed in your cluster for HPA to function:

```bash
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
```

### Resource requests and limits

Adjust per your cluster capacity. Default values are conservative for development:

```yaml
goApi:
  resources:
    requests:
      cpu: "100m"
      memory: "128Mi"
    limits:
      cpu: "500m"
      memory: "256Mi"
```

### Multiple API key support

Create API keys via the admin endpoint after deployment:

```bash
ADMIN_KEY=$(kubectl get secret trident-secrets -o jsonpath='{.data.ADMIN_API_KEY}' | base64 -d)
TRIDENT_HOST="http://$(kubectl get svc trident-nginx -o jsonpath='{.status.loadBalancer.ingress[0].ip}')"

curl -X POST "$TRIDENT_HOST/v1/api-keys" \
  -H "X-Admin-Key: $ADMIN_KEY" \
  -H "Content-Type: application/json" \
  -d '{"label": "my-app", "network": "mainnet"}'
```

## Secrets management {#secrets}

Every deployment (and the migration hook Job — see
[Database migrations](#migrations) above) reads `DATABASE_URL`, `REDIS_URL`,
and `ADMIN_API_KEY` from a single Kubernetes Secret named by
`global.existingSecret` (default `trident-secrets`) via `secretKeyRef` —
never from `values.yaml`, and never `COPY`'d into an image layer (see
[crates/api/Dockerfile](../crates/api/Dockerfile),
[crates/indexer/Dockerfile](../crates/indexer/Dockerfile),
[services/api/Dockerfile](../services/api/Dockerfile), and
[database/Dockerfile](../database/Dockerfile): each only copies the compiled
binary (or, for `database/Dockerfile`, the `sqlx` CLI and the migration
`.sql` files) out of its builder stage — no `.env` file, no secret, is ever
part of an image layer). How that one Secret gets *populated* is a separate
choice, with three supported options:

### Option 1 — `kubectl create secret` (quick start / dev)

What the Quick Start above uses. Simplest option, but the plaintext value
passes through your shell history and the `kubectl` process — fine for a
local kind cluster, not recommended for production.

### Option 2 — external-secrets operator (recommended for production)

Syncs the Secret from a real secrets backend (Vault, AWS Secrets Manager, GCP
Secret Manager, Azure Key Vault, ...) on a refresh interval, so no plaintext
value is ever typed into `kubectl` or committed anywhere.

1. Install the [external-secrets operator](https://external-secrets.io/latest/introduction/getting-started/) into the cluster (once, cluster-wide).
2. Create a `SecretStore` or `ClusterSecretStore` pointing at your backend — see the
   [external-secrets provider docs](https://external-secrets.io/latest/provider/aws-secrets-manager/)
   for backend-specific examples. Example for AWS Secrets Manager:

   ```yaml
   apiVersion: external-secrets.io/v1beta1
   kind: ClusterSecretStore
   metadata:
     name: trident-secret-store
   spec:
     provider:
       aws:
         service: SecretsManager
         region: us-east-1
         auth:
           jwt:
             serviceAccountRef:
               name: trident-external-secrets
   ```

3. Enable the chart's `ExternalSecret` and point it at that store:

   ```bash
   helm upgrade trident ./helm/trident \
     --set global.externalSecret.enabled=true \
     --set global.externalSecret.secretStoreRef.name=trident-secret-store \
     --set global.externalSecret.secretStoreRef.kind=ClusterSecretStore
   ```

   By default this expects a single backend secret at `trident/prod` with
   `DATABASE_URL`/`REDIS_URL`/`ADMIN_API_KEY` keys — override
   `global.externalSecret.data[].remoteRef` per key if your backend layout
   differs (see `helm/trident/values.yaml`).

The operator owns and continuously syncs a Secret named
`global.existingSecret` — every other deployment keeps reading it exactly the
same way, so there's no chart change needed anywhere else.

### Option 3 — Secrets Store CSI Driver

An alternative to the external-secrets operator: mount the backend secret as
a volume via the [Secrets Store CSI Driver](https://secrets-store-csi-driver.sigs.k8s.io/)
and its provider for your backend (e.g.
[aws-secrets-store-csi-driver-provider](https://github.com/aws/secrets-store-csi-driver-provider-aws),
[secrets-store-csi-driver-provider-gcp](https://github.com/GoogleCloudPlatform/secrets-store-csi-driver-provider-gcp),
[secrets-store-csi-driver-provider-azure](https://github.com/Azure/secrets-store-csi-driver-provider-azure)).
Not templated directly in this chart — the CSI driver/provider combination is
cluster- and backend-specific — but the driver's `secretObjects` field can
sync the mounted secret into a native Kubernetes Secret with the same name
(`global.existingSecret`) and keys this chart expects, so no other chart
changes are needed either. Example `SecretProviderClass`:

```yaml
apiVersion: secrets-store.csi.x-k8s.io/v1
kind: SecretProviderClass
metadata:
  name: trident-secrets-csi
spec:
  provider: aws  # or gcp / azure — matches your installed CSI provider
  parameters:
    objects: |
      - objectName: "trident/prod/DATABASE_URL"
        objectType: "secretsmanager"
      - objectName: "trident/prod/REDIS_URL"
        objectType: "secretsmanager"
      - objectName: "trident/prod/ADMIN_API_KEY"
        objectType: "secretsmanager"
  secretObjects:
    - secretName: trident-secrets   # global.existingSecret
      type: Opaque
      data:
        - objectName: "trident/prod/DATABASE_URL"
          key: DATABASE_URL
        - objectName: "trident/prod/REDIS_URL"
          key: REDIS_URL
        - objectName: "trident/prod/ADMIN_API_KEY"
          key: ADMIN_API_KEY
```

Then mount the CSI volume on at least one pod referencing this
`SecretProviderClass` (a single mount is enough to trigger the sync — the
resulting `trident-secrets` Secret is then available cluster-wide via
`secretKeyRef` exactly as with the other two options).

### Verifying no secret ends up in an image layer

```bash
docker history --no-trunc ghcr.io/telocel-labs/trident-go-api:latest | grep -i -E "DATABASE_URL|REDIS_URL|ADMIN_API_KEY|secret"
```

Should print nothing. Each Dockerfile's runtime stage only ever `COPY
--from=builder` the compiled binary — no `ENV`, no `ARG`, no `COPY` of a
secret or `.env` file appears in any layer.

### Verifying no secret is ever logged

The Go API, gRPC API, and indexer all read `DATABASE_URL`/`REDIS_URL` only to
open a connection at startup — none of the three log the connection string
itself (only connection *success/failure*, without the credential-bearing
URL). `ADMIN_API_KEY` is compared, never logged. If you add a log line near
any of these, redact the credential portion — don't log the raw env var value.

### Rotating secrets

1. Update the value in your backend (Vault/Secrets Manager/etc., or `kubectl create secret --dry-run=client -o yaml | kubectl apply -f -` for the manual path).
2. **external-secrets**: happens automatically on the next `refreshInterval` tick (default `1h` in this chart) — no manual step. To force it immediately: `kubectl annotate externalsecret trident-secrets force-sync=$(date +%s) --overwrite`.
3. **CSI**: re-mount (pod restart) picks up the new value; `secretObjects` sync depends on your provider's rotation reconciler — check its docs for a reconciliation interval.
4. **Manual `kubectl create secret`**: re-run the command with the new value, or `kubectl create secret generic trident-secrets --from-literal=... --dry-run=client -o yaml | kubectl apply -f -`.
5. **After any of the above**, roll the consuming pods so they pick up the new value — none of the three services currently hot-reload env vars:
   ```bash
   kubectl rollout restart deployment/trident-go-api deployment/trident-grpc-api deployment/trident-indexer
   ```
   (A future improvement would be [Reloader](https://github.com/stakater/Reloader) to automate this step.)

## Health checks

The Go API exposes two endpoints for Kubernetes' liveness and readiness probes, split per issue #243 so a dependency outage never causes a restart loop that can't fix it:

- **Liveness** — `GET /v1/health` (`failureThreshold: 3`): cheap, no dependency calls, just confirms the process is up. Restarts the container after 3 consecutive failures. Never fails because Postgres/Redis/the gRPC backend is down — restarting the pod doesn't fix an external dependency, so liveness must not conflate "the process is stuck" with "a dependency is unreachable."
- **Readiness** — `GET /v1/ready` (`failureThreshold: 3`): checks Postgres, Redis, and the gRPC backend concurrently (3s timeout per dependency). Returns 503 only when Postgres or the gRPC backend fails, and only after 3 consecutive failures removes the pod from the Service. A Redis failure is reported as `degraded` with a 200 because the Redis-backed middlewares (rate limit, cache, idempotency) fail open, so a Redis outage does not take every pod out of rotation.

## Upgrading

```bash
helm upgrade trident ./helm/trident --reuse-values \
  --set goApi.image.tag=v0.2.0 \
  --set indexer.image.tag=v0.2.0 \
  --set grpcApi.image.tag=v0.2.0
```

## Uninstalling

```bash
helm uninstall trident --namespace trident
kubectl delete namespace trident
# Retain the secret if you plan to reinstall:
# kubectl -n trident delete secret trident-secrets
```
