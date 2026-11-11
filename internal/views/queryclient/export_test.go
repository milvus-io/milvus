package queryclient

// Expose the collector only to external tests so the end-to-end reduction test
// can import the Proxy pipeline without creating a production dependency cycle.
var NewLegacyQueryCollectorForTest = newLegacyQueryCollector
