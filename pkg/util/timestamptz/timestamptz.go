package timestamptz

import (
	"bytes"
	"strings"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

// Define max/min offset boundaries in seconds for validation, exported for external checks if necessary.
const (
	MaxOffsetSeconds = 14 * 3600  // +14:00
	MinOffsetSeconds = -12 * 3600 // -12:00
)

// NaiveTzLayouts is a list of common timestamp formats that lack timezone information.
var NaiveTzLayouts = []string{
	"2006-01-02T15:04:05.999999999",
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02 15:04:05",
}

// Define layouts at the package level for better performance and clarity.
var extraAbsoluteLayouts = []string{
	"2006-01-02 15:04:05Z07:00", // Case 3 (Z), Case 7/8 (+08:00)
	"2006-01-02 15:04:05Z07",    // PostgreSQL also supports short offsets like +08
}

// validateOffset ensures the absolute time offset is within Milvus/PG valid ranges.
func validateOffset(t time.Time) (time.Time, error) {
	// Parsing succeeded (TZ-aware string). Now, perform the strict offset validation.
	// If the string contains an explicit offset (like +99:00), t.Zone() will reflect it.
	_, offsetSeconds := t.Zone()
	if offsetSeconds > MaxOffsetSeconds || offsetSeconds < MinOffsetSeconds {
		offsetHours := offsetSeconds / 3600
		return time.Time{}, merr.WrapErrParameterInvalidMsg("UTC offset hour %d is out of the valid range [%d, %d]",
			offsetHours, MinOffsetSeconds/3600, MaxOffsetSeconds/3600)
	}
	return t, nil
}

// ParseTimeTz is the internal core function for parsing TZ-aware or naive timestamps.
// It includes strict validation for the UTC offset range.
func ParseTimeTz(inputStr string, defaultTimezoneStr string) (time.Time, error) {
	// 1. Primary parsing: Attempt to parse a TZ-aware string (RFC3339Nano)
	t, err := time.Parse(time.RFC3339Nano, inputStr)

	if err == nil {
		return validateOffset(t)
	}

	// 2. PostgreSQL-style Absolute Time: Space separator + Offset or Z
	// Adding layouts to catch "YYYY-MM-DD HH:MM:SS-07:00" or "YYYY-MM-DD HH:MM:SSZ"
	for _, layout := range extraAbsoluteLayouts {
		if t, err = time.Parse(layout, inputStr); err == nil {
			return validateOffset(t)
		}
	}

	// 3. Specific fix for Case: "2024-12-31 22:00:00Z"
	// Handle Absolute Time with a space separator instead of 'T'.
	// Since it contains 'Z', it is parsed as UTC directly.
	if t, err = time.Parse("2006-01-02 15:04:05Z", inputStr); err == nil {
		return t, nil
	}

	loc, err := time.LoadLocation(defaultTimezoneStr)
	if err != nil {
		return time.Time{}, merr.WrapErrParameterInvalidErr(err, "invalid default timezone string '%s'", defaultTimezoneStr)
	}

	// 4. Fallback parsing: Attempt to parse a naive string using NaiveTzLayouts
	var parsed bool
	for _, layout := range NaiveTzLayouts {
		// For naive strings, time.ParseInLocation assigns the default location (loc).
		parsedTime, parseErr := time.ParseInLocation(layout, inputStr, loc)
		if parseErr == nil {
			t = parsedTime
			parsed = true
			break
		}
	}

	if !parsed {
		return time.Time{}, merr.WrapErrParameterInvalidMsg("invalid timestamp string: '%s'. Does not match any known format", inputStr)
	}

	// No offset validation needed here: The time was assigned the safe defaultTimezoneStr (loc),
	// which is already validated via time.LoadLocation.

	return t, nil
}

// ValidateTimestampTz checks if the timestamp string is valid (TZ-aware or naive + default TZ).
func ValidateTimestampTz(inputStr string, defaultTimezoneStr string) error {
	_, err := ParseTimeTz(inputStr, defaultTimezoneStr)
	return err
}

// ValidateAndNormalizeTimestampTz validates the timestamp and normalizes it to a TZ-aware RFC3339Nano string.
func ValidateAndNormalizeTimestampTz(inputStr string, defaultTimezoneStr string) (string, error) {
	t, err := ParseTimeTz(inputStr, defaultTimezoneStr)
	if err != nil {
		return "", err
	}
	// Normalization: Format the time object to include the timezone offset.
	return t.Format(time.RFC3339Nano), nil
}

// ValidateAndReturnUnixMicroTz validates the timestamp and returns its Unix microsecond (int64) representation.
func ValidateAndReturnUnixMicroTz(inputStr string, defaultTimezoneStr string) (int64, error) {
	t, err := ParseTimeTz(inputStr, defaultTimezoneStr)
	if err != nil {
		return 0, err
	}
	// UnixMicro() returns the number of microseconds since UTC 1970-01-01T00:00:00Z.
	return t.UnixMicro(), nil
}

// CompareUnixMicroTz compares two timestamp strings at Unix microsecond precision.
// If both strings are valid and represent the same microsecond moment in time, it returns true.
// Note: It assumes the input strings are guaranteed to be valid as per the requirement.
// If not, it will return an error indicating the invalid input.
func CompareUnixMicroTz(ts1 string, ts2 string, defaultTimezoneStr string) (bool, error) {
	// 1. Parse the first timestamp
	t1, err := ParseTimeTz(ts1, defaultTimezoneStr)
	if err != nil {
		return false, merr.WrapErrServiceInternalErr(err, "error parsing first timestamp '%s'", ts1)
	}

	// 2. Parse the second timestamp
	t2, err := ParseTimeTz(ts2, defaultTimezoneStr)
	if err != nil {
		return false, merr.WrapErrServiceInternalErr(err, "error parsing second timestamp '%s'", ts2)
	}

	// 3. Compare their Unix Microsecond values (int64)
	// This automatically compares them based on the UTC epoch, regardless of their original location representation.
	return t1.UnixMicro() == t2.UnixMicro(), nil
}

// ConvertUnixMicroToTimezoneString converts a Unix microsecond timestamp (UTC epoch)
// into a TZ-aware string formatted as RFC3339Nano, adjusted to the target timezone.
func ConvertUnixMicroToTimezoneString(ts int64, targetTimezoneStr string) (string, error) {
	loc, err := time.LoadLocation(targetTimezoneStr)
	if err != nil {
		return "", merr.WrapErrParameterInvalidErr(err, "invalid target timezone string '%s'", targetTimezoneStr)
	}

	// 1. Convert Unix Microsecond (UTC) to a time.Time object (still in UTC).
	t := time.UnixMicro(ts).UTC()

	// 2. Adjust the time object to the target location.
	localTime := t.In(loc)

	// 3. Format the result.
	return localTime.Format(time.RFC3339Nano), nil
}

// formatTimeMicroWithoutTrailingZeros is an optimized function to format a time.Time
// object. It first truncates the time to microsecond precision (6 digits) and then
// removes all trailing zeros from the fractional seconds part.
//
// Example 1: 2025-03-20T10:30:00.123456000Z -> 2025-03-20T10:30:00.123456Z
// Example 2: 2025-03-20T10:30:00.123000000Z -> 2025-03-20T10:30:00.123Z
// Example 3: 2025-03-20T10:30:00.000000000Z -> 2025-03-20T10:30:00Z
func FormatTimeMicroWithoutTrailingZeros(t time.Time) string {
	// 1. Truncate to Microsecond (6 digits max) to ensure we don't exceed the required precision.
	tMicro := t.Truncate(time.Microsecond)

	// 2. Format the time using the standard high precision format (RFC3339Nano).
	// This results in exactly 9 fractional digits, padded with trailing zeros if necessary.
	s := tMicro.Format(time.RFC3339Nano)

	// 3. Locate the key delimiters ('.' and the Timezone marker 'Z' or '+/-').
	dotIndex := strings.LastIndexByte(s, '.')

	// Find the Timezone marker index (Z, +, or -)
	tzIndex := len(s) - 1
	for ; tzIndex >= 0; tzIndex-- {
		if s[tzIndex] == 'Z' || s[tzIndex] == '+' || s[tzIndex] == '-' {
			break
		}
	}

	// If the format is unexpected, return the original string.
	if dotIndex == -1 || tzIndex == -1 {
		return s
	}

	// 4. Extract and efficiently trim the fractional part using bytes.TrimRight.

	// Slice the fractional part (e.g., "123456000")
	fractionalPart := s[dotIndex+1 : tzIndex]

	// Use bytes.TrimRight for efficient removal of trailing '0' characters.
	trimmedBytes := bytes.TrimRight([]byte(fractionalPart), "0")

	// 5. Reconstruct the final string based on the trimming result.

	// Case A: The fractional part was entirely zeros (e.g., .000000000)
	if len(trimmedBytes) == 0 {
		// Remove the '.' and the fractional part, keep the Timezone marker.
		// Result: "2025-03-20T10:30:00Z"
		return s[:dotIndex] + s[tzIndex:]
	}

	// Case B: Fractional part remains (e.g., .123, .123456)
	// Recombine: [Time Body] + "." + [Trimmed Fraction] + [Timezone Marker]
	// The dot (s[:dotIndex+1]) must be retained here.
	return s[:dotIndex+1] + string(trimmedBytes) + s[tzIndex:]
}

// IsTimezoneValid checks if a given string is a valid, recognized timezone name
// (e.g., "Asia/Shanghai" or "UTC").
// It utilizes Go's time.LoadLocation function.
func IsTimezoneValid(tz string) bool {
	if tz == "" {
		return false
	}
	_, err := time.LoadLocation(tz)
	return err == nil
}

// CheckAndRewriteTimestampTzDefaultValue processes the collection schema to validate
// and rewrite default values for TIMESTAMPTZ fields.
//
// Background:
//  1. TIMESTAMPTZ default values are initially stored as user-provided ISO 8601 strings
//     (in ValueField.GetStringData()).
//  2. Milvus stores TIMESTAMPTZ data internally as UTC microseconds (int64).
//
// Logic:
// The function iterates through all fields of type DataType_Timestamptz. For each field
// with a default value:
//  1. It retrieves the collection's default timezone if no offset is present in the string.
//  2. It calls ValidateAndReturnUnixMicroTz to validate the string (including the UTC
//     offset range check) and convert it to the absolute UTC microsecond (int64) value.
//  3. It rewrites the ValueField, setting the LongData field with the calculated int64
//     value, thereby replacing the initial string representation.
func CheckAndRewriteTimestampTzDefaultValue(schema *schemapb.CollectionSchema) error {
	// 1. Get the collection-level default timezone.
	// Assuming common.TimezoneKey and common.DefaultTimezone are defined constants.
	timezone, exist := funcutil.TryGetAttrByKeyFromRepeatedKV(common.TimezoneKey, schema.GetProperties())
	if !exist {
		timezone = common.DefaultTimezone
	}

	for _, fieldSchema := range schema.GetFields() {
		// Only process TIMESTAMPTZ fields.
		if fieldSchema.GetDataType() != schemapb.DataType_Timestamptz {
			continue
		}

		defaultValue := fieldSchema.GetDefaultValue()
		if defaultValue == nil {
			continue
		}

		// 2. Read the default value as a string (the input format).
		// We expect the default value to be set in string_data initially.
		stringTz := defaultValue.GetStringData()
		if stringTz == "" {
			// Skip or handle empty string default values if necessary.
			continue
		}

		// 3. Validate the string and convert it to UTC microsecond (int64).
		// This also performs the critical UTC offset range validation.
		utcMicro, err := ValidateAndReturnUnixMicroTz(stringTz, timezone)
		if err != nil {
			// If validation fails (e.g., invalid format or illegal offset), return error immediately.
			return err
		}

		// 4. Rewrite the default value to store the UTC microsecond (int64).
		// By setting ValueField_LongData, the oneof field in the protobuf structure
		// automatically switches from string_data to timestamptz_data(int64).
		defaultValue.Data = &schemapb.ValueField_TimestamptzData{
			TimestamptzData: utcMicro,
		}

		// The original string_data field is now cleared due to the oneof nature,
		// and the default value is correctly represented as an int64 microsecond value.
	}
	return nil
}

// CheckAndRewriteTimestampTzDefaultValueForFieldSchema processes a single FieldSchema
// to validate and rewrite the default value specifically for TIMESTAMPTZ fields.
//
// The function ensures the default value (initially a string) is correctly converted
// and stored internally as an absolute UTC microsecond (int64) value.
//
// Parameters:
//
//	fieldSchema: The specific FieldSchema object to be processed.
//	collectionTimezone: The collection-level default timezone string (e.g., "UTC", "Asia/Shanghai")
//	                    used to parse timestamps without an explicit offset.
//
// Returns:
//
//	error: An error if validation fails (e.g., invalid timestamp format or illegal offset range), otherwise nil.
func CheckAndRewriteTimestampTzDefaultValueForFieldSchema(
	fieldSchema *schemapb.FieldSchema,
	collectionTimezone string,
) error {
	defaultValue := fieldSchema.GetDefaultValue()
	if defaultValue == nil {
		return nil
	}

	// 2. Read the default value as a string (the initial user-provided format).
	// The default value is expected to be stored in string_data initially.
	stringTz := defaultValue.GetStringData()
	if stringTz == "" {
		// Skip or handle empty string default values if necessary.
		return nil
	}

	// 3. Validate the string and convert it to UTC microsecond (int64).
	// The validation function also applies the collectionTimezone if no offset is present
	// in the input stringTz, and performs offset range checks.
	utcMicro, err := ValidateAndReturnUnixMicroTz(stringTz, collectionTimezone)
	if err != nil {
		// If validation fails (e.g., invalid format or illegal offset), return error immediately.
		return err
	}

	// 4. Rewrite the default value to store the absolute UTC microsecond (int64).
	// By setting ValueField_LongData, the oneof field in the protobuf structure
	// automatically switches the internal representation from string_data to timestamptz_data(int64).
	defaultValue.Data = &schemapb.ValueField_TimestamptzData{
		TimestamptzData: utcMicro,
	}
	fieldSchema.DefaultValue = defaultValue
	return nil
}

// RewriteTimestampTzDefaultValueToString converts the default_value of TIMESTAMPTZ fields
// in the DescribeCollectionResponse from the internal int64 (UTC microsecond) format
// back to a human-readable, timezone-aware string (RFC3339Nano).
//
// This is necessary because TIMESTAMPTZ default values are stored internally as int64
// after validation but must be returned to the user as a string, respecting the
// collection's default timezone for display purposes if no explicit offset was stored.
func RewriteTimestampTzDefaultValueToString(schema *schemapb.CollectionSchema) error {
	if schema == nil {
		return nil
	}

	// 1. Determine the target timezone for display.
	// This is typically stored in the collection properties.
	timezone, exist := funcutil.TryGetAttrByKeyFromRepeatedKV(common.TimezoneKey, schema.GetProperties())
	if !exist {
		timezone = common.DefaultTimezone // Fallback to a default, like "UTC"
	}

	// 2. Iterate through all fields in the schema.
	for _, fieldSchema := range schema.GetFields() {
		// Only process TIMESTAMPTZ fields.
		if fieldSchema.GetDataType() != schemapb.DataType_Timestamptz {
			continue
		}

		defaultValue := fieldSchema.GetDefaultValue()
		if defaultValue == nil {
			continue
		}

		// 3. Check if the default value is stored in the internal int64 (LongData) format.
		// If it's not LongData, we assume it's either unset or already a string (which shouldn't happen
		// if the creation flow worked correctly).
		utcMicro, ok := defaultValue.GetData().(*schemapb.ValueField_TimestamptzData)
		if !ok {
			continue // Skip if not stored as LongData (int64)
		}

		ts := utcMicro.TimestamptzData

		// 4. Convert the int64 microsecond value back to a timezone-aware string.
		tzString, err := ConvertUnixMicroToTimezoneString(ts, timezone)
		if err != nil {
			// In a real system, you might log the error and use the raw int64 as a fallback string,
			// but here we'll set a placeholder string to avoid crashing.
			return merr.WrapErrServiceInternalErr(err, "error converting timestamp")
		}

		// 5. Rewrite the default value field in the response schema.
		// The protobuf oneof structure ensures setting one field clears the others.
		fieldSchema.GetDefaultValue().Data = &schemapb.ValueField_StringData{
			StringData: tzString,
		}
	}
	return nil
}
