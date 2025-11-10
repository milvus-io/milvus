package funcutil

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestValidateAndNormalizeTimestampTz tests the function for string validation and normalization.
func TestValidateAndNormalizeTimestampTz(t *testing.T) {
	testCases := []struct {
		name             string
		inputStr         string
		defaultTZ        string
		expectedOutput   string
		expectError      bool
		errorContainsMsg string
	}{
		{
			name:           "Case 1: TZ-aware (UTC)",
			inputStr:       "2024-10-23T15:30:00Z",
			defaultTZ:      "Asia/Tokyo",
			expectedOutput: "2024-10-23T15:30:00Z",
			expectError:    false,
		},
		{
			name:           "Case 2: TZ-aware (Offset)",
			inputStr:       "2024-10-23T15:30:00+05:30",
			defaultTZ:      "UTC",
			expectedOutput: "2024-10-23T15:30:00+05:30",
			expectError:    false,
		},
		{
			name:           "Case 3: Naive (Apply Default TZ Shanghai)",
			inputStr:       "2024-10-23 15:30:00",
			defaultTZ:      "Asia/Shanghai", // Shanghai is UTC+08:00
			expectedOutput: "2024-10-23T15:30:00+08:00",
			expectError:    false,
		},
		{
			name:           "Case 4: Naive (Apply Default TZ LA)",
			inputStr:       "2024-10-23 15:30:00.123456",
			defaultTZ:      "America/Los_Angeles", // LA is UTC-07:00 (PDT for Oct)
			expectedOutput: "2024-10-23T15:30:00.123456-07:00",
			expectError:    false,
		},
		{
			name:             "Case 5: Invalid Format",
			inputStr:         "23-10-2024 15:30",
			defaultTZ:        "UTC",
			expectedOutput:   "",
			expectError:      true,
			errorContainsMsg: "invalid timestamp string",
		},
		{
			name:             "Case 6: Invalid Default Timezone",
			inputStr:         "2024-10-23T15:30:00Z",
			defaultTZ:        "Invalid/TZ",
			expectedOutput:   "",
			expectError:      true,
			errorContainsMsg: "invalid default timezone string",
		},
		{
			name:             "Case 7: Offset Too High (+15:00)",
			inputStr:         "2024-10-23T15:30:00+15:00",
			defaultTZ:        "UTC",
			expectedOutput:   "",
			expectError:      true,
			errorContainsMsg: "UTC offset hour 15 is out of the valid range",
		},
		{
			name:             "Case 8: Offset Too Low (-13:00)",
			inputStr:         "2024-10-23T15:30:00-13:00",
			defaultTZ:        "UTC",
			expectedOutput:   "",
			expectError:      true,
			errorContainsMsg: "UTC offset hour -13 is out of the valid range",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := ValidateAndNormalizeTimestampTz(tc.inputStr, tc.defaultTZ)

			if tc.expectError {
				assert.Error(t, err)
				if err != nil {
					assert.Contains(t, err.Error(), tc.errorContainsMsg)
				}
			} else {
				assert.NoError(t, err)
				// Truncate expected output to microsecond for safe comparison
				// as time.Time can contain up to nanoseconds, but we test to microsecond precision.
				tParsed, _ := time.Parse(time.RFC3339Nano, tc.expectedOutput)
				expectedFormatted := tParsed.Truncate(time.Microsecond).Format(time.RFC3339Nano)

				assert.Equal(t, expectedFormatted, result)
			}
		})
	}
}

// TestValidateAndReturnUnixMicroTz tests the function for Unix microsecond conversion.
func TestValidateAndReturnUnixMicroTz(t *testing.T) {
	// Base time for comparison: 2024-10-23T15:30:00.123456Z (UTC)
	// UnixMicro should be the same regardless of input format/timezone if moment is the same.
	expectedMicro := int64(1729697400123456)

	testCases := []struct {
		name          string
		inputStr      string
		defaultTZ     string
		expectedMicro int64
		expectError   bool
	}{
		{
			name:          "Case 1: TZ-aware (UTC)",
			inputStr:      "2024-10-23T15:30:00.123456Z",
			defaultTZ:     "Asia/Tokyo",
			expectedMicro: expectedMicro,
			expectError:   false,
		},
		{
			name:          "Case 2: TZ-aware (Offset)",
			inputStr:      "2024-10-23T23:30:00.123456+08:00", // +8h is 15:30 UTC
			defaultTZ:     "UTC",
			expectedMicro: expectedMicro,
			expectError:   false,
		},
		{
			name:          "Case 3: Naive (Apply Default TZ Shanghai)",
			inputStr:      "2024-10-23 23:30:00.123456", // 23:30 Shanghai (UTC+08:00) is 15:30 UTC
			defaultTZ:     "Asia/Shanghai",
			expectedMicro: expectedMicro,
			expectError:   false,
		},
		{
			name:          "Case 4: Naive (Apply Default TZ LA)",
			inputStr:      "2024-10-23 08:30:00.123456", // 08:30 LA (PDT, UTC-07:00) is 15:30 UTC
			defaultTZ:     "America/Los_Angeles",
			expectedMicro: expectedMicro,
			expectError:   false,
		},
		{
			name:          "Case 5: Invalid Offset",
			inputStr:      "2024-10-23T15:30:00+15:00",
			defaultTZ:     "UTC",
			expectedMicro: 0,
			expectError:   true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := ValidateAndReturnUnixMicroTz(tc.inputStr, tc.defaultTZ)

			if tc.expectError {
				assert.Error(t, err)
				assert.Equal(t, int64(0), result)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedMicro, result)
			}
		})
	}
}

// TestCompareUnixMicroTz tests the comparison function.
func TestCompareUnixMicroTz(t *testing.T) {
	// Base time for comparison: 2025-10-23T15:30:00Z (Using the current date for realism)

	// These strings all represent the moment 2025-10-23T15:30:00.000000Z
	sameTime1 := "2025-10-23T15:30:00Z"
	sameTime2 := "2025-10-23T23:30:00+08:00" // Shanghai: 23:30 local -> 15:30 UTC
	sameTime3 := "2025-10-23 15:30:00"       // Naive time

	// A slightly different moment (1 microsecond later)
	differentTime := "2025-10-23T15:30:00.000001Z"

	testCases := []struct {
		name        string
		ts1         string
		ts2         string
		defaultTZ   string
		expectedCmp bool
		expectError bool
	}{
		{
			name:        "Case 1: Same moment, different explicit TZ format",
			ts1:         sameTime1, // 15:30 UTC
			ts2:         sameTime2, // 15:30 UTC
			defaultTZ:   "UTC",
			expectedCmp: true,
			expectError: false,
		},
		{
			name:        "Case 2: Same moment, one naive, UTC default TZ needed",
			ts1:         sameTime1, // 15:30 UTC
			ts2:         sameTime3, // 15:30 assigned UTC -> 15:30 UTC
			defaultTZ:   "UTC",
			expectedCmp: true,
			expectError: false,
		},
		{
			// FIX FOR DST: On 2025-10-23, New York is in EDT (UTC-04:00).
			// To match 15:30 UTC, the naive time (ts2) must be 15:30 + 04:00 = 19:30 local time.
			name:        "Case 3: Same moment, DST-aware comparison for naive string",
			ts1:         "2025-10-23T19:30:00-04:00", // 19:30 EDT -> 15:30 UTC
			ts2:         "2025-10-23 19:30:00",       // 19:30 assigned New York (EDT) -> 15:30 UTC
			defaultTZ:   "America/New_York",
			expectedCmp: true,
			expectError: false,
		},
		{
			name:        "Case 4: Different moment (1 microsecond difference)",
			ts1:         sameTime1,
			ts2:         differentTime,
			defaultTZ:   "UTC",
			expectedCmp: false,
			expectError: false,
		},
		// Revised Case 5
		{
			name:        "Case 5: Different naive times under the same default TZ",
			ts1:         "2025-10-23 10:00:00", // 10:00 assigned LA -> 03:00 UTC
			ts2:         "2025-10-23 11:00:00", // 11:00 assigned LA -> 04:00 UTC
			defaultTZ:   "America/Los_Angeles",
			expectedCmp: false, // 03:00 UTC != 04:00 UTC
			expectError: false,
		},
		// New Case 5B: Identical naive strings MUST be equal.
		{
			name:        "Case 5B: Identical naive strings must be equal",
			ts1:         "2025-10-23 10:00:00", // 10:00 assigned LA -> 03:00 UTC
			ts2:         "2025-10-23 10:00:00", // 10:00 assigned LA -> 03:00 UTC
			defaultTZ:   "America/Los_Angeles",
			expectedCmp: true,
			expectError: false,
		},
		{
			name:        "Case 6: Invalid TS1 (Offset Too High)",
			ts1:         "2025-10-23T15:30:00+15:00", // Should fail offset check
			ts2:         sameTime1,
			defaultTZ:   "UTC",
			expectedCmp: false,
			expectError: true,
		},
		{
			name:        "Case 7: Invalid TS2 (Bad Format)",
			ts1:         sameTime1,
			ts2:         "not a timestamp",
			defaultTZ:   "UTC",
			expectedCmp: false,
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := CompareUnixMicroTz(tc.ts1, tc.ts2, tc.defaultTZ)

			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedCmp, result)
			}
		})
	}
}

// TestValidateTimestampTz tests the simple validation function.
func TestValidateTimestampTz(t *testing.T) {
	testCases := []struct {
		name        string
		inputStr    string
		defaultTZ   string
		expectError bool
	}{
		{
			name:        "Case 1: Valid TZ-aware",
			inputStr:    "2024-10-23T15:30:00Z",
			defaultTZ:   "UTC",
			expectError: false,
		},
		{
			name:        "Case 2: Valid Naive",
			inputStr:    "2024-10-23 15:30:00",
			defaultTZ:   "Asia/Shanghai",
			expectError: false,
		},
		{
			name:        "Case 3: Invalid Format",
			inputStr:    "Invalid",
			defaultTZ:   "UTC",
			expectError: true,
		},
		{
			name:        "Case 4: Invalid Offset",
			inputStr:    "2024-10-23T15:30:00+15:00",
			defaultTZ:   "UTC",
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateTimestampTz(tc.inputStr, tc.defaultTZ)

			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestConvertUnixMicroToTimezoneString tests the conversion of Unix Microsecond
// to a TZ-aware RFC3339Nano string.
func TestConvertUnixMicroToTimezoneString2(t *testing.T) {
	// Base time for all tests: 2025-03-20T10:30:00.123456Z (UTC)
	// This date is chosen to test standard time outside of DST changes.
	baseTimeUTC := time.Date(2025, time.March, 20, 10, 30, 0, 123456000, time.UTC)
	baseUnixMicro := baseTimeUTC.UnixMicro() // 1710921000123456 (Actual value calculated by Go)

	// DST test date: 2025-10-23T15:30:00.000000Z (UTC)
	// New York is in EDT (UTC-04:00) on this date.
	dstTimeUTC := time.Date(2025, time.October, 23, 15, 30, 0, 0, time.UTC)
	dstUnixMicro := dstTimeUTC.UnixMicro() // 1729697400000000

	testCases := []struct {
		name             string
		unixMicro        int64
		targetTZ         string
		expectedOutput   string
		expectError      bool
		errorContainsMsg string
	}{
		// --- Basic Functionality & Precision ---
		{
			name:           "Case 1: Standard conversion to UTC",
			unixMicro:      baseUnixMicro,
			targetTZ:       "UTC",
			expectedOutput: "2025-03-20T10:30:00.123456Z",
			expectError:    false,
		},
		{
			name:           "Case 2: Conversion to Asia/Shanghai (+08:00)",
			unixMicro:      baseUnixMicro,
			targetTZ:       "Asia/Shanghai", // UTC+08:00
			expectedOutput: "2025-03-20T18:30:00.123456+08:00",
			expectError:    false,
		},
		{
			name:      "Case 3: Conversion to America/Los_Angeles (PDT on Mar 20)",
			unixMicro: baseUnixMicro,
			targetTZ:  "America/Los_Angeles",
			// Corrected Output: 10:30 UTC - 7 hours (PDT) = 03:30 local time
			expectedOutput: "2025-03-20T03:30:00.123456-07:00",
			expectError:    false,
		},

		// --- DST Handling (2025-10-23) ---
		{
			name:           "Case 4: DST active (America/New_York) - UTC-04:00",
			unixMicro:      dstUnixMicro,
			targetTZ:       "America/New_York",          // EDT = UTC-04:00 on this date
			expectedOutput: "2025-10-23T11:30:00-04:00", // 15:30 UTC -> 11:30 local time
			expectError:    false,
		},

		// --- Error Handling ---
		{
			name:             "Case 5: Invalid Timezone String",
			unixMicro:        baseUnixMicro,
			targetTZ:         "Invalid/TZ_Name",
			expectedOutput:   "",
			expectError:      true,
			errorContainsMsg: "invalid target timezone string",
		},
		{
			name:           "Case 6: UnixMicro 0 (Epoch)",
			unixMicro:      0,
			targetTZ:       "UTC",
			expectedOutput: "1970-01-01T00:00:00Z",
			expectError:    false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := ConvertUnixMicroToTimezoneString(tc.unixMicro, tc.targetTZ)

			if tc.expectError {
				assert.Error(t, err)
				if err != nil {
					assert.Contains(t, err.Error(), tc.errorContainsMsg)
				}
			} else {
				assert.NoError(t, err)

				// Assert the resulting string can be parsed back and retains microsecond precision
				tParsed, parseErr := time.Parse(time.RFC3339Nano, result)
				assert.NoError(t, parseErr)

				// Standardize the expected output to ensure nanosecond precision padding is correct
				expectedFormatted := tc.expectedOutput

				assert.Equal(t, expectedFormatted, result)

				// Ensure the microsecond value is maintained (conversion consistency check)
				assert.Equal(t, tc.unixMicro, tParsed.UnixMicro())
			}
		})
	}
}

// TestFormatTimeMicroWithoutTrailingZerosREVISED tests the custom formatting function
// to ensure it correctly truncates to microsecond and removes trailing zeros.
func TestFormatTimeMicroWithoutTrailingZeros(t *testing.T) {
	testCases := []struct {
		name           string
		nanoSeconds    int    // Nanoseconds part
		tzOffsetHours  int    // TZ Offset in hours
		expectedOutput string // Expected final string after cleaning
	}{
		{
			name:           "Case 1: Full Microsecond Precision (No trailing zeros to remove)",
			nanoSeconds:    123456000, // .123456
			tzOffsetHours:  8,
			expectedOutput: "2025-10-23T00:00:00.123456+08:00",
		},
		{
			name:           "Case 2: Millisecond Precision (Remove 6 trailing zeros)",
			nanoSeconds:    123000000, // .123
			tzOffsetHours:  8,
			expectedOutput: "2025-10-23T00:00:00.123+08:00",
		},
		{
			name:           "Case 3: Second Precision (Remove all 9 fractional zeros)",
			nanoSeconds:    0, // .000000
			tzOffsetHours:  8,
			expectedOutput: "2025-10-23T00:00:00+08:00", // Note: The dot is removed.
		},
		{
			name:           "Case 4: Single-digit precision (Remove 8 trailing zeros)",
			nanoSeconds:    100000000, // .1
			tzOffsetHours:  0,
			expectedOutput: "2025-10-23T00:00:00.1Z",
		},
		{
			name:           "Case 5: Precision beyond microsecond (Truncate first, then remove zeros)",
			nanoSeconds:    123456789, // .123456789 -> truncates to .123456
			tzOffsetHours:  -7,
			expectedOutput: "2025-10-23T00:00:00.123456-07:00",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a time.Time object for a fixed date/time
			tz := time.FixedZone("TEST_TZ", tc.tzOffsetHours*3600)
			tInput := time.Date(2025, time.October, 23, 0, 0, 0, tc.nanoSeconds, tz)

			result := FormatTimeMicroWithoutTrailingZeros(tInput)

			assert.Equal(t, tc.expectedOutput, result)

			// Verification: Ensure round-trip parsing still works and yields the correct microsecond value
			tParsed, parseErr := time.Parse(time.RFC3339Nano, result)
			assert.NoError(t, parseErr)
			// Microsecond precision is preserved
			assert.Equal(t, tInput.Truncate(time.Microsecond).UnixMicro(), tParsed.UnixMicro())
		})
	}
}

// funcutil/time_test.go (Updated TestConvertUnixMicroToTimezoneString)

// TestConvertUnixMicroToTimezoneString tests the conversion of Unix Microsecond
// to a TZ-aware RFC3339Nano string, now including the trailing zero removal logic.
func TestConvertUnixMicroToTimezoneString(t *testing.T) {
	// Base time 1: 2025-03-20T10:30:00.123456Z (UTC). Nanoseconds: 123456000
	baseTimeUTC := time.Date(2025, time.March, 20, 10, 30, 0, 123456000, time.UTC)
	baseUnixMicro := baseTimeUTC.UnixMicro() // 1710921000123456

	// Base time 2: 2025-10-23T15:30:00.000000Z (UTC). Nanoseconds: 0
	dstTimeUTC := time.Date(2025, time.October, 23, 15, 30, 0, 0, time.UTC)
	dstUnixMicro := dstTimeUTC.UnixMicro() // 1729697400000000

	testCases := []struct {
		name             string
		unixMicro        int64
		targetTZ         string
		expectedOutput   string // NOW CLEANED OF TRAILING ZEROS
		expectError      bool
		errorContainsMsg string
	}{
		// --- Basic Functionality & Precision (Using baseUnixMicro - ends in .123456) ---
		{
			name:           "Case 1: Standard conversion to UTC (Microsecond precision)",
			unixMicro:      baseUnixMicro,
			targetTZ:       "UTC",
			expectedOutput: "2025-03-20T10:30:00.123456Z", // Removed "000"
			expectError:    false,
		},
		{
			name:           "Case 2: Conversion to Asia/Shanghai (+08:00)",
			unixMicro:      baseUnixMicro,
			targetTZ:       "Asia/Shanghai",                    // UTC+08:00
			expectedOutput: "2025-03-20T18:30:00.123456+08:00", // Removed "000"
			expectError:    false,
		},
		{
			// Fix from previous round: 10:30 UTC - 7 hours (PDT) = 03:30 local time.
			name:           "Case 3: Conversion to America/Los_Angeles (PDT on Mar 20)",
			unixMicro:      baseUnixMicro,
			targetTZ:       "America/Los_Angeles",              // PDT = UTC-07:00
			expectedOutput: "2025-03-20T03:30:00.123456-07:00", // Removed "000"
			expectError:    false,
		},

		// --- DST Handling (Using dstUnixMicro - ends in .000000) ---
		{
			name:           "Case 4: DST active (America/New_York) - UTC-04:00 (Second precision)",
			unixMicro:      dstUnixMicro,
			targetTZ:       "America/New_York",          // EDT = UTC-04:00 on this date
			expectedOutput: "2025-10-23T11:30:00-04:00", // Removed all fractional zeros and the dot
			expectError:    false,
		},

		// --- Custom Test Case: Only Millisecond precision (must remove 3 trailing zeros) ---
		{
			name: "Case 5: Millisecond precision input",
			// Use the calculated value for 2025-03-20T10:30:00.123Z.
			// If you were previously using baseUnixMicro, calculate the new value:
			// (baseUnixMicro / 1000000) * 1000000 + 123000
			unixMicro:      (baseUnixMicro / 1000000 * 1000000) + 123000,
			targetTZ:       "UTC",
			expectedOutput: "2025-03-20T10:30:00.123Z",
			expectError:    false,
		},

		// --- Error Handling ---
		{
			name:             "Case 6: Invalid Timezone String",
			unixMicro:        baseUnixMicro,
			targetTZ:         "Invalid/TZ_Name",
			expectedOutput:   "",
			expectError:      true,
			errorContainsMsg: "invalid target timezone string",
		},
		{
			name:           "Case 7: UnixMicro 0 (Epoch) - Second precision",
			unixMicro:      0,
			targetTZ:       "UTC",
			expectedOutput: "1970-01-01T00:00:00Z", // Removed all fractional zeros and the dot
			expectError:    false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := ConvertUnixMicroToTimezoneString(tc.unixMicro, tc.targetTZ)

			if tc.expectError {
				assert.Error(t, err)
				if err != nil {
					assert.Contains(t, err.Error(), tc.errorContainsMsg)
				}
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedOutput, result)

				// Verify round-trip consistency: the resulting string must parse back
				// to the original microsecond value (since no significant digits were lost).
				tParsed, parseErr := time.Parse(time.RFC3339Nano, result)
				assert.NoError(t, parseErr)
				assert.Equal(t, tc.unixMicro, tParsed.UnixMicro())
			}
		})
	}
}

// TestIsTimezoneValid tests the IsTimezoneValid function.
func TestIsTimezoneValid(t *testing.T) {
	// Common valid IANA timezone names
	validTimezones := []string{
		"UTC",
		"Local",
		"Asia/Shanghai",
		"America/New_York",
		"Europe/London",
		"Australia/Sydney",
		"Etc/GMT+10",
	}

	// Common invalid or malformed timezone names
	invalidTimezones := []string{
		"",
		"GMT+8",
		"CST",
		"PST",
		"Invalid/Zone",
		"Asia/Beijingg",
		"99:00",
		time.FixedZone("MyZone", 3600).String(), // Custom location names (like "MyZone") are not recognized by time.LoadLocation.
	}

	// Test valid timezones
	for _, tz := range validTimezones {
		t.Run("Valid_"+tz, func(t *testing.T) {
			if !IsTimezoneValid(tz) {
				t.Errorf("IsTimezoneValid(\"%s\") expected true, but got false", tz)
			}
		})
	}

	// Test invalid timezones
	for _, tz := range invalidTimezones {
		t.Run("Invalid_"+tz, func(t *testing.T) {
			if IsTimezoneValid(tz) {
				t.Errorf("IsTimezoneValid(\"%s\") expected false, but got true", tz)
			}
		})
	}
}
