package test

import (
	"testing"

	"github.com/pandops/flowai-model-registry/internal/store"
)

// service-platform: changes are published through the outbox.
// "Synchronous publications without an outbox" is not applicable:
// model-registry publishes only through its outbox.
func TestV0048ServicePlatformPublishesChangesThroughAnOutbox(t *testing.T) {
	t.Run("Rolled-back change publishes nothing", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "m1", nil)
		h.relay()
		h.broker.ResetRecords()
		h.store.FailCommit(store.TxModelUpdate, 1)
		h.setPaused(3, "m1", true)
		h.relay()
		for _, m := range h.broker.Published() {
			if m.Subject == "model-registry.event.model-availability-changed.pub" || bytesContain(m.Data, `"outcome":"succeeded"`) {
				t.Fatalf("rolled-back update published %s %s", m.Subject, m.Data)
			}
		}
	})
}
