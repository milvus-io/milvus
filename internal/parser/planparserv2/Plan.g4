grammar Plan;

expr:
	IntegerConstant											                     # Integer
	| FloatingConstant										                     # Floating
	| BooleanConstant										                     # Boolean
	| StringLiteral											                     # String
	| Identifier											                     # Identifier
	| JSONIdentifier                                                             # JSONIdentifier
	| '(' expr ')'											                     # Parens
	| expr LIKE StringLiteral                                                    # Like
	| expr POW expr											                     # Power
	| op = (ADD | SUB | BNOT | NOT) expr					                     # Unary
//	| '(' typeName ')' expr									                     # Cast
	| expr op = (MUL | DIV | MOD) expr						                     # MulDivMod
	| expr op = (ADD | SUB) expr							                     # AddSub
	| expr op = (SHL | SHR) expr							                     # Shift
	| expr op = (IN | NIN) ('[' expr (',' expr)* ','? ']')                       # Term
	| expr op = (IN | NIN) EmptyTerm                                             # EmptyTerm
	| JSONContains'('expr',' expr')'                                             # JSONContains
	| expr op1 = (LT | LE) (Identifier | JSONIdentifier) op2 = (LT | LE) expr	 # Range
	| expr op1 = (GT | GE) (Identifier | JSONIdentifier) op2 = (GT | GE) expr    # ReverseRange
	| expr op = (LT | LE | GT | GE) expr					                     # Relational
	| expr op = (EQ | NE) expr								                     # Equality
	| expr BAND expr										                     # BitAnd
	| expr BXOR expr										                     # BitXor
	| expr BOR expr											                     # BitOr
	| expr AND expr											                     # LogicalAnd
	| expr OR expr											                     # LogicalOr
	| EXISTS expr                                                                # Exists;

// typeName: ty = (BOOL | INT8 | INT16 | INT32 | INT64 | FLOAT | DOUBLE);

// BOOL: 'bool';
// INT8: 'int8';
// INT16: 'int16';
// INT32: 'int32';
// INT64: 'int64';
// FLOAT: 'float';
// DOUBLE: 'double';

LT: '<';
LE: '<=';
GT: '>';
GE: '>=';
EQ: '==';
NE: '!=';

LIKE: 'like' | 'LIKE';
EXISTS: 'exists' | 'EXISTS';

ADD: '+';
SUB: '-';
MUL: '*';
DIV: '/';
MOD: '%';
POW: '**';
SHL: '<<';
SHR: '>>';
BAND: '&';
BOR: '|';
BXOR: '^';

AND: '&&' | 'and';
OR: '||' | 'or';

BNOT: '~';
NOT: '!' | 'not';

IN: 'in';
NIN: 'not in';
EmptyTerm: '[' (Whitespace | Newline)* ']';

JSONContains: 'json_contains' | 'JSON_CONTAINS';

BooleanConstant: 'true' | 'True' | 'TRUE' | 'false' | 'False' | 'FALSE';

IntegerConstant:
	DecimalConstant
	| OctalConstant
	| HexadecimalConstant
	| BinaryConstant;

FloatingConstant:
	DecimalFloatingConstant
	| HexadecimalFloatingConstant;

Identifier: Nondigit (Nondigit | Digit)* | '$meta';

StringLiteral: EncodingPrefix? ('"' DoubleSCharSequence? '"' | '\'' SingleSCharSequence? '\'');
JSONIdentifier: Identifier('[' (StringLiteral | DecimalConstant) ']')+;

fragment EncodingPrefix: 'u8' | 'u' | 'U' | 'L';

fragment DoubleSCharSequence: DoubleSChar+;
fragment SingleSCharSequence: SingleSChar+;

fragment DoubleSChar: ~["\\\r\n] | EscapeSequence | '\\\n' | '\\\r\n';
fragment SingleSChar: ~['\\\r\n] | EscapeSequence | '\\\n' | '\\\r\n';
fragment Nondigit: [a-zA-Z_];
fragment Digit: [0-9];
fragment BinaryConstant: '0' [bB] [0-1]+;
fragment DecimalConstant: NonzeroDigit Digit* | '0';
fragment OctalConstant: '0' OctalDigit*;
fragment HexadecimalConstant: '0' [xX] HexadecimalDigitSequence;
fragment NonzeroDigit: [1-9];
fragment OctalDigit: [0-7];
fragment HexadecimalDigit: [0-9a-fA-F];
fragment HexQuad:
	HexadecimalDigit HexadecimalDigit HexadecimalDigit HexadecimalDigit;
fragment UniversalCharacterName:
	'\\u' HexQuad
	| '\\U' HexQuad HexQuad;
fragment DecimalFloatingConstant:
	FractionalConstant ExponentPart?
	| DigitSequence ExponentPart;
fragment HexadecimalFloatingConstant:
	'0' [xX] (
		HexadecimalFractionalConstant
		| HexadecimalDigitSequence
	) BinaryExponentPart;
fragment FractionalConstant:
	DigitSequence? '.' DigitSequence
	| DigitSequence '.';
fragment ExponentPart: [eE] [+-]? DigitSequence;
fragment DigitSequence: Digit+;
fragment HexadecimalFractionalConstant:
	HexadecimalDigitSequence? '.' HexadecimalDigitSequence
	| HexadecimalDigitSequence '.';
fragment HexadecimalDigitSequence: HexadecimalDigit+;
fragment BinaryExponentPart: [pP] [+-]? DigitSequence;
fragment EscapeSequence:
	'\\' ['"?abfnrtv\\]
	| '\\' OctalDigit OctalDigit? OctalDigit?
	| '\\x' HexadecimalDigitSequence
	| UniversalCharacterName;

Whitespace: [ \t]+ -> skip;

Newline: ( '\r' '\n'? | '\n') -> skip;
