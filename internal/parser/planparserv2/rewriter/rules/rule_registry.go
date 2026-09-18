package rules

// NewDefaultRules returns the ordered rules enabled by the supplied config.
func NewDefaultRules(optimizeEnabled bool) []Rule {
	// Term ordering is an execution invariant, not an optional optimization.
	rules := []Rule{sortTermValuesRule{}}
	if !optimizeEnabled {
		return rules
	}
	return append(rules,
		canonicalizeTermRule{},
		canonicalizeValueRule{},
		simplifyUnaryRule{},
		simplifyLogicalRule{},
	)
}

var orLogicalRules = []logicalPartsRule{
	orArrayContainsRule{},
	orEqualsToInRule{},
	orTextMatchRule{},
	orRangeRule{},
	orBinaryRangeRule{},
	orInNotEqualRule{},
	orInUnionRule{},
	orInEqualRule{},
}

var andLogicalRules = []logicalPartsRule{
	andArrayContainsRule{},
	andRangeRule{},
	andBinaryRangeRule{},
	andInIntersectionRule{},
	andInNotEqualRule{},
	andInRangeRule{},
	andInEqualRule{},
	andNotEqualsToNotInRule{},
}
