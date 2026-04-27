package common

import "testing"

func TestDRWAEventIdentifiers_NonEmpty(t *testing.T) {
	for _, id := range DRWAEventIdentifiers {
		if id == "" {
			t.Fatalf("DRWA event identifier must not be empty")
		}
	}
}

func TestDRWAEventIdentifiers_NoDuplicates(t *testing.T) {
	seen := make(map[string]struct{}, len(DRWAEventIdentifiers))
	for _, id := range DRWAEventIdentifiers {
		if _, exists := seen[id]; exists {
			t.Fatalf("duplicate DRWA event identifier: %q", id)
		}
		seen[id] = struct{}{}
	}
}

func TestIsDRWAIdentifier(t *testing.T) {
	if !IsDRWAIdentifier(DrwaTokenPolicyEvent) {
		t.Fatalf("expected %q to be a DRWA identifier", DrwaTokenPolicyEvent)
	}
	if IsDRWAIdentifier("nonDrwaEvent") {
		t.Fatalf("unexpected DRWA identifier match for non DRWA event")
	}
}

func TestIsDRWAIdentifier_AllCanonicalEvents(t *testing.T) {
	for _, identifier := range DRWAEventIdentifiers {
		if !IsDRWAIdentifier(identifier) {
			t.Fatalf("expected %q to be recognized as a DRWA identifier", identifier)
		}
	}
}
