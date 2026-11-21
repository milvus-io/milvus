# Nullable elements in scalar ARRAY fields

A single-level ARRAY of a supported scalar type can opt in with
`element_nullable=true` independently of field-level `nullable`. A null row,
an empty list and a list containing a null are distinct. Capacity counts
logical slots, including nulls. Existing schemas keep their original format.

The insert payload uses the same element bitmap and compact typed values as an
element-nullable StructArray child. The proxy materializes dense scalar slots
for storage. V2/V3 use the existing Arrow list representation with independent
list and child validity. V1 and growing-source flush retain their native-list
restrictions. CDC carries the field flag and element validity in existing
schema and insert messages.

Partial upsert preserves an omitted array. PATH_REPLACE takes a one-slot
operand, including `[null]`, and addresses the logical index in a present row.
A null parent or an index outside the list is an input error. ARRAY_APPEND,
ARRAY_REMOVE and scalar indexes retain their existing restrictions on arrays
with element validity.
