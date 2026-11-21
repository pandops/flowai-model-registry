package test

import (
	"encoding/json"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// message-broker: message payloads follow publisher-owned JSON Schemas.
// Every harness also validates every message model-registry publishes and
// every reply it sends against svc/model-registry/schemas/.
func TestV0048MessageBrokerPayloadsFollowPublisherSchemas(t *testing.T) {
	foreign := []string{schemaExecutorRegistered, schemaExecutorRemoved, schemaExecutorTypeReq, schemaExecutorTypeReply,
		schemaDockerEvent, schemaDockerCheckReq, schemaK8sEvent, schemaK8sCheckReq,
		schemaTMClaimCurrentReq, schemaTMClaimCurrentRep, schemaTMReleased, schemaTMCancelled, schemaTMArchived,
		schemaFRLLMSettingsReq, schemaFRLLMSettingsRep, schemaFRAliasUsageReq, schemaFRAliasUsageRep,
		schemaFRRetireReq, schemaFRRestoreReq} // retire-alias replies 204: it has no reply schema

	t.Run("Publisher message violates its own schema", func(t *testing.T) {
		c := ownSchemas(t)
		s, err := c.Compile(ownSchemaBase + "event.task-key-revoked.schema.json")
		if err != nil {
			t.Fatal(err)
		}
		bad := `{"publisher":"model-registry","publisher_seq":1,"team_id":3,"occurred_at":"2026-10-14T09:30:00.000Z","kind":"task-key-revoked","attempt":1,"claim_seq":1,"cause":"finished"}`
		inst, _ := jsonschema.UnmarshalJSON(strings.NewReader(bad))
		if s.Validate(inst) == nil {
			t.Fatal("a task-key-revoked event without task_id passes its schema")
		}
		withKey := strings.Replace(bad, `"attempt"`, `"task_id":1010,"key":"sk-x","attempt"`, 1)
		inst, _ = jsonschema.UnmarshalJSON(strings.NewReader(withKey))
		if s.Validate(inst) == nil {
			t.Fatal("an event with an extra key field passes its schema")
		}
		// Every own schema file compiles and names the file in its $id.
		files, _ := filepath.Glob(filepath.Join(serviceDir(t), "schemas", "*.schema.json"))
		for _, f := range files {
			if _, err := c.Compile(ownSchemaBase + filepath.Base(f)); err != nil {
				t.Fatalf("%s: %v", filepath.Base(f), err)
			}
		}
	})

	t.Run("Consumer contract test reads the publisher schema", func(t *testing.T) {
		for _, f := range foreign {
			if !foreignSchemaExists(t, f) {
				t.Fatalf("publisher schema %s is missing", f)
			}
			assertSharedInput(t, f, "model-registry")
		}
		// No test imports another service's package.
		files, err := filepath.Glob(filepath.Join(serviceDir(t), "test", "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range files {
			file, err := parser.ParseFile(token.NewFileSet(), f, nil, parser.ImportsOnly)
			if err != nil {
				t.Fatal(err)
			}
			for _, spec := range file.Imports {
				path := strings.Trim(spec.Path.Value, "\"`")
				if strings.HasPrefix(path, "github.com/flowai/platform/") ||
					(strings.HasPrefix(path, "github.com/pandops/flowai-") &&
						!strings.HasPrefix(path, "github.com/pandops/flowai-model-registry/")) {
					t.Fatalf("%s imports %s", filepath.Base(f), path)
				}
			}
		}
	})

	t.Run("Schema change selects its consumers", func(t *testing.T) {
		raw, err := os.ReadFile(filepath.Join(contractFixtureDir(t), "qa-e2e", "scripts", "shared-test-inputs.json"))
		if err != nil {
			t.Fatal(err)
		}
		var doc struct {
			Schemas map[string][]string `json:"schemas"`
		}
		_ = json.Unmarshal(raw, &doc)
		for path, owners := range doc.Schemas {
			if strings.HasPrefix(path, "svc/model-registry/schemas/") && (len(owners) == 0 || owners[0] != "model-registry") {
				t.Fatalf("%s lists %v, publisher model-registry must come first", path, owners)
			}
		}
		for _, f := range []string{"svc/model-registry/schemas/service.validate-models.reply.schema.json", "svc/model-registry/schemas/executor.check.reply.schema.json"} {
			if len(doc.Schemas[f]) < 2 {
				t.Fatalf("%s declares no consumer: %v", f, doc.Schemas[f])
			}
		}
	})
}
