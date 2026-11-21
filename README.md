# model-registry

Owns team and administrator models, model pricing, the LiteLLM management
API, Flow LiteLLM teams and spend, LLM snapshots and per-task LiteLLM virtual
keys.

## Interfaces

- NATS request/reply on `model-registry.ui.*`, `model-registry.service.*` and
  `model-registry.executor.<executor_id>.check`, queue group `model-registry`.
  Request and reply JSON Schemas live in [schemas/](schemas/).
- Events and audit entries go out through a transactional outbox on
  `model-registry.event.<kind>` and `model-registry.audit.<action>`.
- The durable consumer `model-registry` on `ALL_EVENT`, declared in
  [schemas/consumers.json](schemas/consumers.json) and created by the
  deployment.
- HTTP on one listener: `GET /v1/livez`, `GET /v1/readyz` and the Executor
  edge `GET /v1/tasks/{task_id}/litellm-key` with the claim token as the
  bearer.

## Running

```sh
model-registry --config /etc/flowai/config.yaml
```

[config.example.yaml](config.example.yaml) lists every key the service reads.
The database schema must exist or the role must be allowed to create it;
migrations create objects only in that schema.

## Tests

Set `FLOWAI_CONTRACT_FIXTURE_DIR` to the absolute path of a temporary fixture
directory. It must contain the Gitea-derived declarations at
`qa-e2e/scripts/shared-test-inputs.json` and the pinned publisher schema files
at their declared paths. The qualification workflow stages these inputs
from the selected source commit and the supplied publisher commits.

```sh
go test -mod=readonly -count=1 ./...
go test -mod=readonly -count=1 -tags=integration ./test
```

The integration suite starts and removes its own PostgreSQL 16 fixture. The
LiteLLM adapter tests use a fake management API. The Gitea full-flow tests
exercise the deployed LiteLLM routes for both Executors.
