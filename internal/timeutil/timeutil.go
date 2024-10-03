package timeutil

import (
	"fmt"
	"time"
)

var TimeFuncs = map[string]func(time.Time) time.Time{
	"startOfHour":     StartOfHour,
	"endOfHour":       EndOfHour,
	"startOfDay":      StartOfDay,
	"endOfDay":        EndOfDay,
	"startOfWeek":     StartOfWeek,
	"endOfWeek":       EndOfWeek,
	"startOfMonth":    StartOfMonth,
	"endOfMonth":      EndOfMonth,
	"startOfCalendar": StartOfCalendar,
	"endOfCalendar":   EndOfCalendar,
	"startOfYear":     StartOfYear,
	"endOfYear":       EndOfYear,
}

var startTime = map[string]func(time.Time) time.Time{
	time.RFC3339:  func(t time.Time) time.Time { return t },
	time.DateTime: func(t time.Time) time.Time { return t },
	"2006-01-02":  StartOfDay,
	"2006-01":     StartOfMonth,
	"2006":        StartOfYear,
}

var endTime = map[string]func(time.Time) time.Time{
	time.RFC3339:  func(t time.Time) time.Time { return t },
	time.DateTime: func(t time.Time) time.Time { return t },
	"2006-01-02":  EndOfDay,
	"2006-01":     EndOfMonth,
	"2006":        EndOfYear,
}

func parse(loc *time.Location, s string, m map[string]func(time.Time) time.Time) (time.Time, error) {
	for key, val := range m {
		t, err := time.ParseInLocation(key, s, loc)
		if err == nil {
			return val(t), nil
		}
	}

	return time.Time{}, fmt.Errorf("unsupported format")
}

func ParseStartInLocation(s string, loc *time.Location) (time.Time, error) {
	return parse(loc, s, startTime)
}

func ParseEndInLocation(s string, loc *time.Location) (time.Time, error) {
	return parse(loc, s, endTime)
}

func ParseStart(s string) (time.Time, error) {
	return ParseStartInLocation(s, time.Local)
}

func ParseEnd(s string) (time.Time, error) {
	return ParseEndInLocation(s, time.Local)
}

func ResolveTime(what string, t time.Time) (time.Time, error) {
	fn, ok := TimeFuncs[what]
	if !ok {
		return time.Time{}, fmt.Errorf("invalid date specifier %q", what)
	}

	return fn(t), nil
}

func IsDateSpecified(what string) bool {
	_, ok := TimeFuncs[what]

	return ok
}

func StartOfHour(now time.Time) time.Time {
	return time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), 0, 0, 0, now.Location())
}

func EndOfHour(now time.Time) time.Time {
	return time.Date(now.Year(), now.Month(), now.Day(), now.Hour()+1, 0, 0, -1, now.Location())
}

func StartOfDay(now time.Time) time.Time {
	return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
}

func EndOfDay(now time.Time) time.Time {
	return time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, -1, now.Location())
}

func StartOfWeek(now time.Time) time.Time {
	weekday := time.Duration(now.Weekday())
	if weekday == 0 {
		weekday = 7
	}

	year, month, day := now.Date()
	currentZeroDay := time.Date(year, month, day, 0, 0, 0, 0, time.Local)

	return currentZeroDay.Add(-1 * (weekday - 1) * 24 * time.Hour)
}

func EndOfWeek(now time.Time) time.Time {
	weekday := time.Duration(now.Weekday())
	if weekday == 7 {
		weekday = 0
	}

	year, month, day := now.Date()
	currentZeroDay := time.Date(year, month, day, 0, 0, 0, 0, time.Local)

	return currentZeroDay.Add(-1 * (weekday - 1) * 24 * time.Hour)
}

func StartOfMonth(now time.Time) time.Time {
	year, month, _ := now.Date()

	return time.Date(year, month, 1, 0, 0, 0, 0, now.Location())
}

func EndOfMonth(now time.Time) time.Time {
	year, month, _ := now.Date()

	return time.Date(year, month+1, 0, 0, 0, 0, -1, now.Location())
}

func StartOfCalendar(now time.Time) time.Time {
	return StartOfWeek(StartOfMonth(now))
}

func EndOfCalendar(now time.Time) time.Time {
	return EndOfWeek(EndOfMonth(now))
}

func IsSameDay(a, b time.Time) bool {
	ay, am, ad := a.Date()
	by, bm, bd := b.Date()

	return ay == by && am == bm && ad == bd
}

func StartOfYear(now time.Time) time.Time {
	return time.Date(now.Year(), time.January, 1, 0, 0, 0, 0, now.Location())
}

func EndOfYear(now time.Time) time.Time {
	return time.Date(now.Year()+1, time.January, 1, 0, 0, 0, -1, now.Location())
}
