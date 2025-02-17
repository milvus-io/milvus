grammar Plan;

// -----------------------------------------------------------------------------
// Parser Rules
// -----------------------------------------------------------------------------

// Top-level expression.
expr
    : logicalOrExpr
    ;

// Logical OR.
logicalOrExpr
    : logicalAndExpr ( OR logicalAndExpr )*
    ;

// Logical AND.
logicalAndExpr
    : equalityExpr ( AND equalityExpr )*
    ;

// Equality expressions (==, !=).
equalityExpr
    : comparisonExpr ( (EQ | NE) comparisonExpr )*
    ;

// Comparison expressions.
// Optimized to support chain comparisons (e.g. 1 < a < 2) without ambiguity,
// and to separately handle LIKE and IN operators.
comparisonExpr
    : additiveExpr ( compOp additiveExpr )*      // Chain comparisons: e.g. 1 < a < 2
    | additiveExpr LIKE StringLiteral              // LIKE operator
    | additiveExpr (NOT? IN) additiveExpr          // Membership operator: IN
    ;

// Comparison operators.
compOp
    : LT | LE | GT | GE
    ;

// Addition and subtraction.
additiveExpr
    : multiplicativeExpr ( (ADD | SUB) multiplicativeExpr )*
    ;

// Multiplication, division, and modulo.
multiplicativeExpr
    : powerExpr ( (MUL | DIV | MOD) powerExpr )*
    ;

// Exponentiation (right-associative).
powerExpr
    : unaryExpr ( POW powerExpr )?
    ;

// Unary expressions: prefix operators and the EXISTS keyword.
unaryExpr
    : (ADD | SUB | BNOT | NOT) unaryExpr
    | EXISTS unaryExpr
    | postfixExpr
    ;

// Postfix expressions: function calls and postfix null-checks.
postfixExpr
    : primaryExpr ( postfixOp )?
    ;

// Postfix operations: function calls, IS NULL, IS NOT NULL.
postfixOp
    : '(' argumentList? ')'     # FunctionCall
    | ISNULL                    # IsNull
    | ISNOTNULL                 # IsNotNull
    ;

// Function call argument list (allows an optional trailing comma).
argumentList
    : expr (',' expr)* ','?
    ;

// Primary expressions: literals, identifiers, parenthesized expressions,
// array literals, template variables, and built-in function calls.
primaryExpr
    : IntegerConstant                         # Integer
    | FloatingConstant                        # Floating
    | BooleanConstant                         # Boolean
    | StringLiteral                           # String
    | Identifier                              # Identifier
    | Meta                                    # Meta
    | JSONIdentifier                          # JSONIdentifier
    | LBRACE Identifier RBRACE                # TemplateVariable
    | '(' expr ')'                            # Parens
    | '[' expr (',' expr)* ','? ']'             # Array
    | EmptyArray                              # EmptyArray
      // Built-in function calls:
    | TEXTMATCH '(' Identifier ',' StringLiteral ')'
                                                # TextMatch
    | PHRASEMATCH '(' Identifier ',' StringLiteral (',' expr)? ')'
                                                # PhraseMatch
    | (JSONContains | ArrayContains) '(' expr ',' expr ')' # JSONContains
    | (JSONContainsAll | ArrayContainsAll) '(' expr ',' expr ')' # JSONContainsAll
    | (JSONContainsAny | ArrayContainsAny) '(' expr ',' expr ')' # JSONContainsAny
    | ArrayLength '(' (Identifier | JSONIdentifier) ')' # ArrayLength
    ;

// -----------------------------------------------------------------------------
// Lexer Rules
// -----------------------------------------------------------------------------

LBRACE: '{';
RBRACE: '}';

LT: '<';
LE: '<=';
GT: '>';
GE: '>=';

EQ: '==';
NE: '!=';

LIKE: 'like' | 'LIKE';
EXISTS: 'exists' | 'EXISTS';
TEXTMATCH: 'text_match' | 'TEXT_MATCH';
PHRASEMATCH: 'phrase_match' | 'PHRASE_MATCH';

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

AND: '&&' | 'and' | 'AND';
OR: '||' | 'or' | 'OR';

ISNULL: 'is null' | 'IS NULL';
ISNOTNULL: 'is not null' | 'IS NOT NULL';

BNOT: '~';
NOT: '!' | 'not' | 'NOT';

IN: 'in' | 'IN';

EmptyArray: '[' (Whitespace | Newline)* ']';

JSONContains: 'json_contains' | 'JSON_CONTAINS';
JSONContainsAll: 'json_contains_all' | 'JSON_CONTAINS_ALL';
JSONContainsAny: 'json_contains_any' | 'JSON_CONTAINS_ANY';

ArrayContains: 'array_contains' | 'ARRAY_CONTAINS';
ArrayContainsAll: 'array_contains_all' | 'ARRAY_CONTAINS_ALL';
ArrayContainsAny: 'array_contains_any' | 'ARRAY_CONTAINS_ANY';
ArrayLength: 'array_length' | 'ARRAY_LENGTH';

BooleanConstant: 'true' | 'True' | 'TRUE' | 'false' | 'False' | 'FALSE';

IntegerConstant
    : DecimalConstant
    | OctalConstant
    | HexadecimalConstant
    | BinaryConstant
    ;

FloatingConstant
    : DecimalFloatingConstant
    | HexadecimalFloatingConstant
    ;

Identifier: Nondigit (Nondigit | Digit)*;
Meta: '$meta';

StringLiteral: EncodingPrefix? ( '"' DoubleSCharSequence? '"' | '\'' SingleSCharSequence? '\'' );
JSONIdentifier: (Identifier | Meta) ('[' (StringLiteral | DecimalConstant) ']')+;

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
fragment HexQuad: HexadecimalDigit HexadecimalDigit HexadecimalDigit HexadecimalDigit;
fragment UniversalCharacterName: '\\u' HexQuad | '\\U' HexQuad HexQuad;

fragment DecimalFloatingConstant:
    FractionalConstant ExponentPart?
    | DigitSequence ExponentPart
    ;

fragment HexadecimalFloatingConstant:
    '0' [xX] ( HexadecimalFractionalConstant | HexadecimalDigitSequence ) BinaryExponentPart
    ;

fragment FractionalConstant:
    DigitSequence? '.' DigitSequence
    | DigitSequence '.'
    ;

fragment ExponentPart: [eE] [+-]? DigitSequence;
fragment DigitSequence: Digit+;
fragment HexadecimalFractionalConstant:
    HexadecimalDigitSequence? '.' HexadecimalDigitSequence
    | HexadecimalDigitSequence '.'
    ;
fragment HexadecimalDigitSequence: HexadecimalDigit+;
fragment BinaryExponentPart: [pP] [+-]? DigitSequence;
fragment EscapeSequence:
    '\\' ['"?abfnrtv\\]
    | '\\' OctalDigit OctalDigit? OctalDigit?
    | '\\x' HexadecimalDigitSequence
    | UniversalCharacterName;

Whitespace: [ \t]+ -> skip;
Newline: ( '\r' '\n'? | '\n' ) -> skip;