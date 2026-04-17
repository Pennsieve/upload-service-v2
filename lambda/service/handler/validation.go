package handler

import "regexp"

// uuidRE matches the canonical 8-4-4-4-12 hex UUID format used for manifest
// and upload identifiers throughout the platform. Validation guards against
// callers embedding arbitrary strings into contexts that interpret special
// characters — notably the STS session policy constructed in
// storage_credentials.go, where a JSON-escaping payload could widen the
// policy scope.
var uuidRE = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func isValidUUID(s string) bool {
	return uuidRE.MatchString(s)
}
