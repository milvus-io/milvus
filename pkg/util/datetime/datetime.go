// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datetime

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	// SecondsPerDay is the length of a calendar day in seconds. DATE packing
	// uses Unix epoch days, which ignore leap seconds.
	SecondsPerDay = int64(24 * 60 * 60)
	// MicrosPerSecond is 1e6.
	MicrosPerSecond = int64(1_000_000)
	// MaxTimeMicros is 24:00:00.000000, the PostgreSQL inclusive upper bound.
	MaxTimeMicros = int64(24) * 60 * 60 * MicrosPerSecond
)

var dateLayout = "2006-01-02"

func parseTwoDigits(s string) (int, bool) {
	if len(s) != 2 || s[0] < '0' || s[0] > '9' || s[1] < '0' || s[1] > '9' {
		return 0, false
	}
	return int(s[0]-'0')*10 + int(s[1]-'0'), true
}

// ParseDate parses an ISO-8601 calendar date (YYYY-MM-DD) into days since
// 1970-01-01 UTC. Time-of-day and timezone suffixes are rejected.
func ParseDate(input string) (int32, error) {
	s := strings.TrimSpace(input)
	if s == "" {
		return 0, merr.WrapErrParameterInvalidMsg("invalid DATE literal: empty string")
	}
	if len(s) != len(dateLayout) {
		return 0, merr.WrapErrParameterInvalidMsg("invalid DATE literal '%s': expected YYYY-MM-DD", s)
	}
	t, err := time.Parse(dateLayout, s)
	if err != nil {
		return 0, merr.WrapErrParameterInvalidMsg("invalid DATE literal '%s'", s)
	}
	if t.UTC().Format(dateLayout) != s {
		return 0, merr.WrapErrParameterInvalidMsg("invalid DATE literal '%s'", s)
	}
	days := t.Unix() / SecondsPerDay
	if days < math.MinInt32 || days > math.MaxInt32 {
		return 0, merr.WrapErrParameterInvalidMsg("DATE literal '%s' is out of int32 day range", s)
	}
	return int32(days), nil
}

// FormatDate renders packed days since epoch as YYYY-MM-DD.
func FormatDate(days int32) string {
	return time.Unix(int64(days)*SecondsPerDay, 0).UTC().Format(dateLayout)
}

// ParseTime parses an ISO-8601 time-of-day without a timezone into microseconds
// since midnight. 24:00:00 is accepted. Timezone suffixes are rejected.
func ParseTime(input string) (int64, error) {
	s := strings.TrimSpace(input)
	if s == "" {
		return 0, merr.WrapErrParameterInvalidMsg("invalid TIME literal: empty string")
	}
	if hasTimeZoneSuffix(s) {
		return 0, merr.WrapErrParameterInvalidMsg("invalid TIME literal '%s': timezone is not allowed", s)
	}
	if micros, ok := parseMidnightWrap(s); ok {
		return micros, nil
	}
	parts := strings.Split(s, ":")
	if len(parts) != 3 {
		return 0, merr.WrapErrParameterInvalidMsg("invalid TIME literal '%s': expected HH:MM:SS[.ffffff]", s)
	}
	hour, ok := parseTwoDigits(parts[0])
	if !ok {
		return 0, merr.WrapErrParameterInvalidMsg("invalid TIME literal '%s': expected HH:MM:SS[.ffffff]", s)
	}
	minute, ok := parseTwoDigits(parts[1])
	if !ok {
		return 0, merr.WrapErrParameterInvalidMsg("invalid TIME literal '%s': expected HH:MM:SS[.ffffff]", s)
	}
	secPart := parts[2]
	frac := int64(0)
	if i := strings.IndexByte(secPart, '.'); i >= 0 {
		secStr, fracStr := secPart[:i], secPart[i+1:]
		sec, ok := parseTwoDigits(secStr)
		if !ok || minute > 59 || sec > 59 || hour > 23 {
			return 0, merr.WrapErrParameterInvalidMsg("invalid TIME literal '%s': expected HH:MM:SS[.ffffff]", s)
		}
		if len(fracStr) == 0 || len(fracStr) > 6 {
			return 0, merr.WrapErrParameterInvalidMsg("invalid TIME literal '%s': fraction must be 1 to 6 digits", s)
		}
		for _, r := range fracStr {
			if r < '0' || r > '9' {
				return 0, merr.WrapErrParameterInvalidMsg("invalid TIME literal '%s': expected HH:MM:SS[.ffffff]", s)
			}
		}
		padded := fracStr + strings.Repeat("0", 6-len(fracStr))
		frac, _ = strconv.ParseInt(padded, 10, 64)
		micros := int64(hour)*3600*MicrosPerSecond + int64(minute)*60*MicrosPerSecond + int64(sec)*MicrosPerSecond + frac
		return micros, nil
	}
	sec, ok := parseTwoDigits(secPart)
	if !ok || minute > 59 || sec > 59 || hour > 23 {
		return 0, merr.WrapErrParameterInvalidMsg("invalid TIME literal '%s': expected HH:MM:SS[.ffffff]", s)
	}
	return int64(hour)*3600*MicrosPerSecond + int64(minute)*60*MicrosPerSecond + int64(sec)*MicrosPerSecond, nil
}

// FormatTime renders microseconds since midnight as HH:MM:SS[.ffffff] with
// trailing fractional zeros dropped.
func FormatTime(micros int64) string {
	if micros < 0 {
		micros = 0
	}
	if micros > MaxTimeMicros {
		micros = MaxTimeMicros
	}
	hours := micros / (3600 * MicrosPerSecond)
	remain := micros % (3600 * MicrosPerSecond)
	minutes := remain / (60 * MicrosPerSecond)
	remain = remain % (60 * MicrosPerSecond)
	seconds := remain / MicrosPerSecond
	frac := remain % MicrosPerSecond
	if frac == 0 {
		return fmt.Sprintf("%02d:%02d:%02d", hours, minutes, seconds)
	}
	fracStr := strings.TrimRight(fmt.Sprintf("%06d", frac), "0")
	return fmt.Sprintf("%02d:%02d:%02d.%s", hours, minutes, seconds, fracStr)
}

func parseMidnightWrap(s string) (int64, bool) {
	if !strings.HasPrefix(s, "24:00:00") {
		return 0, false
	}
	rest := s[len("24:00:00"):]
	if rest == "" {
		return MaxTimeMicros, true
	}
	if rest[0] != '.' {
		return 0, false
	}
	frac := rest[1:]
	if frac == "" || len(frac) > 6 {
		return 0, false
	}
	for _, r := range frac {
		if r != '0' {
			return 0, false
		}
	}
	return MaxTimeMicros, true
}

func hasTimeZoneSuffix(s string) bool {
	if strings.ContainsAny(s, "TtZz ") {
		return true
	}
	// Offset like +08:00 or -05 after the seconds field. Colons in HH:MM:SS
	// are not offsets; scan from the first digit group after seconds.
	for i := 8; i < len(s); i++ {
		if s[i] == '+' || s[i] == '-' {
			return true
		}
	}
	return false
}

// ValidateDateDays checks that packed days are a finite int32 (always true)
// and exists so call sites can share one error wrapper.
func ValidateDateDays(days int64) (int32, error) {
	if days < math.MinInt32 || days > math.MaxInt32 {
		return 0, merr.WrapErrParameterInvalidMsg("DATE days %d is out of int32 range", days)
	}
	return int32(days), nil
}

// ValidateTimeMicros checks TIME packed values.
func ValidateTimeMicros(micros int64) error {
	if micros < 0 || micros > MaxTimeMicros {
		return merr.WrapErrParameterInvalidMsg("TIME microseconds %d is out of range [0, %d]", micros, MaxTimeMicros)
	}
	return nil
}
