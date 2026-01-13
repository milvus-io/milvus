grammar Plan;

expr:
  Identifier (op1=(ADD | SUB) INTERVAL interval_string=StringLiteral)? op2=(LT | LE | GT | GE | EQ | NE) ISO compare_string=StringLiteral # TimestamptzCompareForward
	| ISO compare_string=StringLiteral op2=(LT | LE | GT | GE | EQ | NE) Identifier (op1=(ADD | SUB) INTERVAL interval_string=StringLiteral)? # TimestamptzCompareReverse
	| IntegerConstant											                                            # Integer
	| FloatingConstant										                                                # Floating
	| BooleanConstant										                                                # Boolean
	| StringLiteral											                                                # String
	| (Identifier|Meta)           			      							                                # Identifier
	| JSONIdentifier                                                                                        # JSONIdentifier
	| StructSubFieldIdentifier                                                                              # StructSubField
	| LBRACE Identifier RBRACE                                                                              # TemplateVariable
	| '(' expr ')'											                                                # Parens
	| '[' expr (',' expr)* ','? ']'                                                                         # Array
	| EmptyArray                                                                                            # EmptyArray
	| EXISTS expr                                                                                           # Exists
	| expr LIKE StringLiteral                                                                               # Like
	| TEXTMATCH'('Identifier',' StringLiteral (',' textMatchOption)? ')'                                    # TextMatch
	| PHRASEMATCH'('Identifier',' StringLiteral (',' expr)? ')'       			                            # PhraseMatch
	| RANDOMSAMPLE'(' expr ')'						     						                            # RandomSample
	| ElementFilter'('Identifier',' expr')'                                	                                # ElementFilter
	| op=(MATCH_ALL | MATCH_ANY) '(' Identifier ',' expr ')'                                                 # MatchSimple
	| op=(MATCH_LEAST | MATCH_MOST | MATCH_EXACT) '(' Identifier ',' expr ',' THRESHOLD ASSIGN IntegerConstant ')'  # MatchThreshold
	| expr POW expr											                                                # Power
	| op = (ADD | SUB | BNOT | NOT) expr					                                                # Unary
//	| '(' typeName ')' expr									                                                # Cast
	| expr op = (MUL | DIV | MOD) expr						                                                # MulDivMod
	| expr op = (ADD | SUB) expr							                                                # AddSub
	| expr op = (SHL | SHR) expr							                                                # Shift
	| expr op = NOT? IN expr                                                                                # Term
	| (JSONContains | ArrayContains)'('expr',' expr')'                                                      # JSONContains
	| (JSONContainsAll | ArrayContainsAll)'('expr',' expr')'                                                # JSONContainsAll
	| (JSONContainsAny | ArrayContainsAny)'('expr',' expr')'                                                # JSONContainsAny
	| op=(STEuqals | STTouches | STOverlaps | STCrosses | STContains | STIntersects | STWithin) '(' Identifier ',' StringLiteral ')'  # SpatialBinary
	| STDWithin'('Identifier','StringLiteral',' expr')'                                                     # STDWithin
	| STIsValid'('Identifier')'                                  			 	                            # STIsValid
	| ArrayLength'('(Identifier | JSONIdentifier)')'                                                        # ArrayLength
	| Identifier '(' ( expr (',' expr )* ','? )? ')'                                                        # Call
	| expr op1 = (LT | LE) (Identifier | JSONIdentifier | StructSubFieldIdentifier) op2 = (LT | LE) expr	# Range
	| expr op1 = (GT | GE) (Identifier | JSONIdentifier | StructSubFieldIdentifier) op2 = (GT | GE) expr    # ReverseRange
	| expr op = (LT | LE | GT | GE) expr					                                                # Relational
	| expr op = (EQ | NE) expr								                                                # Equality
	| expr BAND expr										                                                # BitAnd
	| expr BXOR expr										                                                # BitXor
	| expr BOR expr											                                                # BitOr
	| expr AND expr											                                                # LogicalAnd
	| expr OR expr											                                                # LogicalOr
	| (Identifier | JSONIdentifier) ISNULL                                                                  # IsNull
	| (Identifier | JSONIdentifier) ISNOTNULL                                                               # IsNotNull;

textMatchOption:
	MINIMUM_SHOULD_MATCH ASSIGN IntegerConstant;

LBRACE: '{';
RBRACE: '}';

LT: '<';
LE: '<=';
GT: '>';
GE: '>=';
EQ: '==';
NE: '!=';

// Case-insensitive keywords using fragments
LIKE: L I K E;
EXISTS: E X I S T S;
TEXTMATCH: T E X T '_' M A T C H;
PHRASEMATCH: P H R A S E '_' M A T C H;
RANDOMSAMPLE: R A N D O M '_' S A M P L E;
MATCH_ALL: M A T C H '_' A L L;
MATCH_ANY: M A T C H '_' A N Y;
MATCH_LEAST: M A T C H '_' L E A S T;
MATCH_MOST: M A T C H '_' M O S T;
MATCH_EXACT: M A T C H '_' E X A C T;
INTERVAL: I N T E R V A L;
ISO: I S O;
MINIMUM_SHOULD_MATCH: M I N I M U M '_' S H O U L D '_' M A T C H;
THRESHOLD: T H R E S H O L D;
ASSIGN: '=';

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

AND: '&&' | A N D;
OR: '||' | O R;

ISNULL: I S ' ' N U L L;
ISNOTNULL: I S ' ' N O T ' ' N U L L;

BNOT: '~';
NOT: '!' | N O T;

IN: I N;
EmptyArray: '[' (Whitespace | Newline)* ']';

JSONContains: J S O N '_' C O N T A I N S;
JSONContainsAll: J S O N '_' C O N T A I N S '_' A L L;
JSONContainsAny: J S O N '_' C O N T A I N S '_' A N Y;

ArrayContains: A R R A Y '_' C O N T A I N S;
ArrayContainsAll: A R R A Y '_' C O N T A I N S '_' A L L;
ArrayContainsAny: A R R A Y '_' C O N T A I N S '_' A N Y;
ArrayLength: A R R A Y '_' L E N G T H;
ElementFilter: E L E M E N T '_' F I L T E R;

STEuqals: S T '_' E Q U A L S;
STTouches: S T '_' T O U C H E S;
STOverlaps: S T '_' O V E R L A P S;
STCrosses: S T '_' C R O S S E S;
STContains: S T '_' C O N T A I N S;
STIntersects: S T '_' I N T E R S E C T S;
STWithin: S T '_' W I T H I N;
STDWithin: S T '_' D W I T H I N;
STIsValid: S T '_' I S V A L I D;

BooleanConstant: T R U E | F A L S E;

IntegerConstant:
	DecimalConstant
	| OctalConstant
	| HexadecimalConstant
	| BinaryConstant;

FloatingConstant:
	DecimalFloatingConstant
	| HexadecimalFloatingConstant;

Identifier: Nondigit (Nondigit | Digit)*;
Meta: '$meta';

StringLiteral: EncodingPrefix? ('"' DoubleSCharSequence? '"' | '\'' SingleSCharSequence? '\'');
JSONIdentifier: (Identifier | Meta)('[' (StringLiteral | DecimalConstant) ']')+;
StructSubFieldIdentifier: '$[' Identifier ']';

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

// Case-insensitive letter fragments
fragment A: [aA];
fragment B: [bB];
fragment C: [cC];
fragment D: [dD];
fragment E: [eE];
fragment F: [fF];
fragment G: [gG];
fragment H: [hH];
fragment I: [iI];
fragment J: [jJ];
fragment K: [kK];
fragment L: [lL];
fragment M: [mM];
fragment N: [nN];
fragment O: [oO];
fragment P: [pP];
fragment Q: [qQ];
fragment R: [rR];
fragment S: [sS];
fragment T: [tT];
fragment U: [uU];
fragment V: [vV];
fragment W: [wW];
fragment X: [xX];
fragment Y: [yY];
fragment Z: [zZ];
