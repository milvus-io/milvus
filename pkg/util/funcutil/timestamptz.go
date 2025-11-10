package funcutil

import (
	"bytes"
	"fmt"
	"strings"
	"time"
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

// ParseTimeTz is the internal core function for parsing TZ-aware or naive timestamps.
// It includes strict validation for the UTC offset range.
func ParseTimeTz(inputStr string, defaultTimezoneStr string) (time.Time, error) {
	// 1. Primary parsing: Attempt to parse a TZ-aware string (RFC3339Nano)
	t, err := time.Parse(time.RFC3339Nano, inputStr)

	if err == nil {
		// Parsing succeeded (TZ-aware string). Now, perform the strict offset validation.

		// If the string contains an explicit offset (like +99:00), t.Zone() will reflect it.
		_, offsetSeconds := t.Zone()

		if offsetSeconds > MaxOffsetSeconds || offsetSeconds < MinOffsetSeconds {
			offsetHours := offsetSeconds / 3600
			return time.Time{}, fmt.Errorf("UTC offset hour %d is out of the valid range [%d, %d]", offsetHours, MinOffsetSeconds/3600, MaxOffsetSeconds/3600)
		}

		return t, nil
	}

	loc, err := time.LoadLocation(defaultTimezoneStr)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid default timezone string '%s': %w", defaultTimezoneStr, err)
	}

	// 2. Fallback parsing: Attempt to parse a naive string using NaiveTzLayouts
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
		return time.Time{}, fmt.Errorf("invalid timestamp string: '%s'. Does not match any known format", inputStr)
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
		return false, fmt.Errorf("error parsing first timestamp '%s': %w", ts1, err)
	}

	// 2. Parse the second timestamp
	t2, err := ParseTimeTz(ts2, defaultTimezoneStr)
	if err != nil {
		return false, fmt.Errorf("error parsing second timestamp '%s': %w", ts2, err)
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
		return "", fmt.Errorf("invalid target timezone string '%s': %w", targetTimezoneStr, err)
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
