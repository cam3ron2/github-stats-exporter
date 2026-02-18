# Kustomize secret inputs

The nonprod and prod overlays now generate Kubernetes Secrets via `secretGenerator`.

Before running `kustomize build` or `kubectl apply -k`, copy the example files to the
expected filenames and replace values with real secrets.

## Nonprod

```bash
cp deploy/kustomize/overlays/nonprod/secrets/github/org-a.pem.example \
  deploy/kustomize/overlays/nonprod/secrets/github/org-a.pem
cp deploy/kustomize/overlays/nonprod/secrets/rabbitmq/username.example \
  deploy/kustomize/overlays/nonprod/secrets/rabbitmq/username
cp deploy/kustomize/overlays/nonprod/secrets/rabbitmq/password.example \
  deploy/kustomize/overlays/nonprod/secrets/rabbitmq/password
cp deploy/kustomize/overlays/nonprod/secrets/rabbitmq/erlang-cookie.example \
  deploy/kustomize/overlays/nonprod/secrets/rabbitmq/erlang-cookie
```

## Prod

```bash
cp deploy/kustomize/overlays/prod/secrets/github/org-a.pem.example \
  deploy/kustomize/overlays/prod/secrets/github/org-a.pem
cp deploy/kustomize/overlays/prod/secrets/github/org-b.pem.example \
  deploy/kustomize/overlays/prod/secrets/github/org-b.pem
cp deploy/kustomize/overlays/prod/secrets/rabbitmq/username.example \
  deploy/kustomize/overlays/prod/secrets/rabbitmq/username
cp deploy/kustomize/overlays/prod/secrets/rabbitmq/password.example \
  deploy/kustomize/overlays/prod/secrets/rabbitmq/password
cp deploy/kustomize/overlays/prod/secrets/rabbitmq/erlang-cookie.example \
  deploy/kustomize/overlays/prod/secrets/rabbitmq/erlang-cookie
```

Real secret files are intentionally ignored by git.

If you change RabbitMQ `username` or `password`, also update `amqp.url` in:

- `deploy/kustomize/overlays/nonprod/patch-config.yaml`
- `deploy/kustomize/overlays/prod/patch-config.yaml`
