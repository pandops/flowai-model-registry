package test

import (
	"strings"
	"testing"
)

// message-broker: subjects model-registry publishes and requests carry no
// team token and all end in ".pub".
func TestV0048MessageBrokerNamesSubjectsFromImmutableIdentifiers(t *testing.T) {
	t.Run("Team appears as a subject token only in the task queue and Source intake", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "m1", nil)
		h.createModel(3, "m2", nil)
		k := claim(3, 1010, 1, 1)
		h.readyClaim(7, k, 21, []string{"3/m1", "3/m2"}, nil, flowLimit("10", "week"))
		h.mustCheckOK(7, k, "3/m1")
		h.issueKey(7, k)
		h.archive(3, "m1")
		h.unarchive(3, "m1")
		h.endClaim(k, "task_terminal")
		h.executorEvent(7, "finished", k, nil)
		h.mustReconcile()
		h.relay()
		var subjects []string
		for _, m := range h.broker.Published() {
			subjects = append(subjects, m.Subject)
		}
		for _, m := range h.broker.Requests() {
			if !strings.HasPrefix(m.Subject, "model-registry.") {
				subjects = append(subjects, m.Subject)
			}
		}
		if len(subjects) == 0 {
			t.Fatal("model-registry sent nothing")
		}
		for _, s := range subjects {
			if strings.HasPrefix(s, "flowai.") || !strings.HasSuffix(s, ".pub") {
				t.Fatalf("subject %q", s)
			}
			for _, tok := range strings.Split(s, ".") {
				if tok == "3" {
					t.Fatalf("subject %q carries the team", s)
				}
			}
		}
	})
}
