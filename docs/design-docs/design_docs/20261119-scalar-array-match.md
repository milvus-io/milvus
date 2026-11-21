# MATCH filters on scalar ARRAY fields

The five row-returning quantifiers accept a scalar array. For example,
`MATCH_LEAST(scores, $ >= 80, threshold=2)` selects rows with at least two
qualifying scores. Each slot binds its scalar value to `$`. StructArray
predicates keep binding a child with `$[child]`.

A predicate uses the scalar operations supported on the same StructArray child
type, including equality, membership, numeric arithmetic, string patterns, and
element IS NULL / IS NOT NULL. It can combine predicates with AND, OR and NOT.
The target is a single-level scalar array. Nested collection operations,
correlated row references, text and phrase search, and element-returning
element_filter are outside this change.

Count slots whose predicate is true. Duplicates count independently. ALL
requires every slot to be true, so it accepts an empty list. ANY requires
at least one hit. LEAST, MOST and EXACT compare the count with the threshold;
LEAST takes a positive threshold and the other two accept zero. A null parent
produces UNKNOWN for all five quantifiers.

The plan binds the target by field ID. StructArray writers send the parent ID
and its name so older QueryNodes can read the plan. A reader uses the name
only when the ID is absent, including plans from flattened legacy schemas.
Scalar ARRAY writes only its field ID. A future JSON target will likewise
bind by root field ID, with its path and missing-value semantics separate. Growing and
sealed segments use the row-to-element offsets and evaluate the predicate
against the raw array column. An ordinary scalar index returns row offsets and
cannot supply these element hits.
