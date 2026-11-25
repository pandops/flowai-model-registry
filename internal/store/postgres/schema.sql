CREATE TABLE IF NOT EXISTS counters (
  team_id bigint NOT NULL,
  kind text NOT NULL,
  value bigint NOT NULL,
  PRIMARY KEY (team_id, kind)
);
CREATE SEQUENCE IF NOT EXISTS admin_model_seq;
CREATE SEQUENCE IF NOT EXISTS publisher_seq;
CREATE TABLE IF NOT EXISTS models (
  key bigserial PRIMARY KEY,
  creator_team_id bigint,
  model_id bigint NOT NULL,
  created_by text NOT NULL,
  name text NOT NULL,
  alias text NOT NULL UNIQUE,
  litellm_model_id text NOT NULL,
  availability text NOT NULL,
  archived_at timestamptz,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS models_creator_id ON models ((COALESCE(creator_team_id, -1)), model_id);
CREATE TABLE IF NOT EXISTS model_owners (
  model_key bigint NOT NULL REFERENCES models (key),
  team_id bigint NOT NULL,
  PRIMARY KEY (model_key, team_id)
);
CREATE INDEX IF NOT EXISTS model_owners_team ON model_owners (team_id);
CREATE TABLE IF NOT EXISTS operations (
  model_key bigint PRIMARY KEY REFERENCES models (key),
  kind text NOT NULL,
  team_id bigint NOT NULL,
  operator_id bigint NOT NULL,
  request_id bigint NOT NULL,
  created_at timestamptz NOT NULL
);
CREATE TABLE IF NOT EXISTS compensations (
  id bigserial PRIMARY KEY,
  team_id bigint NOT NULL,
  model_id bigint NOT NULL,
  alias text NOT NULL,
  litellm_model_id text NOT NULL,
  credential_name text NOT NULL,
  created_at timestamptz NOT NULL
);
CREATE TABLE IF NOT EXISTS flow_teams (
  team_id bigint NOT NULL,
  flow_id bigint NOT NULL,
  litellm_team_id text NOT NULL,
  flow_revision bigint NOT NULL,
  flow_limit jsonb,
  budget_reset_at timestamptz,
  updated_at timestamptz NOT NULL,
  PRIMARY KEY (team_id, flow_id)
);
CREATE TABLE IF NOT EXISTS spend_observations (
  team_id bigint NOT NULL,
  flow_id bigint NOT NULL,
  succeeded boolean NOT NULL,
  spend_usd text,
  budget_reset_at timestamptz,
  observed_at timestamptz NOT NULL,
  last_success_spend_usd text,
  last_success_reset_at timestamptz,
  last_success_at timestamptz,
  PRIMARY KEY (team_id, flow_id)
);
CREATE TABLE IF NOT EXISTS executors (
  executor_id bigint PRIMARY KEY,
  team_id bigint NOT NULL,
  removed boolean NOT NULL
);
CREATE TABLE IF NOT EXISTS snapshots (
  team_id bigint NOT NULL,
  task_id bigint NOT NULL,
  attempt bigint NOT NULL,
  claim_seq bigint NOT NULL,
  executor_id bigint NOT NULL,
  flow_id bigint NOT NULL,
  model_alias text NOT NULL,
  model_key bigint NOT NULL REFERENCES models (key),
  per_task_limit jsonb,
  created_at timestamptz NOT NULL,
  ended_at timestamptz,
  PRIMARY KEY (team_id, task_id, attempt, claim_seq)
);
CREATE INDEX IF NOT EXISTS snapshots_model ON snapshots (model_key) WHERE ended_at IS NULL;
CREATE TABLE IF NOT EXISTS task_keys (
  team_id bigint NOT NULL,
  task_id bigint NOT NULL,
  attempt bigint NOT NULL,
  claim_seq bigint NOT NULL,
  token text NOT NULL,
  ciphertext bytea NOT NULL,
  expires_at timestamptz NOT NULL,
  extended_at timestamptz NOT NULL,
  revoke_cause text,
  revoke_requested_at timestamptz,
  revoked_at timestamptz,
  PRIMARY KEY (team_id, task_id, attempt, claim_seq),
  FOREIGN KEY (team_id, task_id, attempt, claim_seq) REFERENCES snapshots (team_id, task_id, attempt, claim_seq)
);
CREATE TABLE IF NOT EXISTS orphans (
  id bigserial PRIMARY KEY,
  team_id bigint NOT NULL,
  task_id bigint NOT NULL,
  attempt bigint NOT NULL,
  claim_seq bigint NOT NULL,
  token text NOT NULL,
  created_at timestamptz NOT NULL
);
CREATE TABLE IF NOT EXISTS consumed (
  publisher text NOT NULL,
  seq bigint NOT NULL,
  PRIMARY KEY (publisher, seq)
);
CREATE TABLE IF NOT EXISTS outbox (
  seq bigint PRIMARY KEY,
  subject text NOT NULL,
  team text NOT NULL,
  request_id bigint,
  payload bytea NOT NULL,
  created_at timestamptz NOT NULL,
  sent_at timestamptz
);
CREATE INDEX IF NOT EXISTS outbox_unsent ON outbox (seq) WHERE sent_at IS NULL;
