package planner

type UnaryOperator int

const (
	UnaryOperatorUnknown           UnaryOperator = iota
	UnaryOperatorExclamationSymbol               // '!'
	UnaryOperatorTilde                           // '~'
	UnaryOperatorPositive                        // '+'
	UnaryOperatorNegative                        // '-'
	UnaryOperatorNot                             // 'not'
)

func (o UnaryOperator) String() string {
	switch o {
	case UnaryOperatorExclamationSymbol:
		return "!"
	case UnaryOperatorTilde:
		return "~"
	case UnaryOperatorPositive:
		return "+"
	case UnaryOperatorNegative:
		return "-"
	case UnaryOperatorNot:
		return "not"
	default:
		return "UNKNOWN"
	}
}

type ComparisonOperator int

const (
	ComparisonOperatorUnknown      ComparisonOperator = 0
	ComparisonOperatorEqual        ComparisonOperator = 1                                                       // '='
	ComparisonOperatorGreaterThan  ComparisonOperator = 2                                                       // '>'
	ComparisonOperatorLessThan     ComparisonOperator = 4                                                       // '<'
	ComparisonOperatorNotEqual     ComparisonOperator = 8                                                       // '<>', '!='
	ComparisonOperatorLessEqual                       = ComparisonOperatorLessThan | ComparisonOperatorEqual    // '<='
	ComparisonOperatorGreaterEqual                    = ComparisonOperatorGreaterThan | ComparisonOperatorEqual // '>='
)

func (o ComparisonOperator) String() string {
	switch o {
	case ComparisonOperatorEqual:
		return "="
	case ComparisonOperatorGreaterThan:
		return ">"
	case ComparisonOperatorLessThan:
		return "<"
	case ComparisonOperatorNotEqual:
		return "!="
	case ComparisonOperatorLessEqual:
		return "<="
	case ComparisonOperatorGreaterEqual:
		return ">="
	default:
		return "UNKNOWN"
	}
}

type LogicalOperator int

const (
	LogicalOperatorUnknown LogicalOperator = iota
	LogicalOperatorAnd
	LogicalOperatorOr
)

func (o LogicalOperator) String() string {
	switch o {
	case LogicalOperatorAnd:
		return "AND"
	case LogicalOperatorOr:
		return "OR"
	default:
		return "UNKNOWN"
	}
}

type IsOperator int

const (
	IsOperatorUnknown IsOperator = iota
	IsOperatorIs
	IsOperatorIsNot
)

func (o IsOperator) String() string {
	switch o {
	case IsOperatorIs:
		return "IS"
	case IsOperatorIsNot:
		return "IS NOT"
	default:
		return "UNKNOWN"
	}
}

type InOperator int

const (
	InOperatorUnknown InOperator = iota
	InOperatorIn
	InOperatorNotIn
)

func (o InOperator) String() string {
	switch o {
	case InOperatorIn:
		return "In"
	case InOperatorNotIn:
		return "NOT IN"
	default:
		return "UNKNOWN"
	}
}
