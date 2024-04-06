package lib

import "time"

const (
	YYYY_MM_DD_HH_MM_SS = "2006-01-02 15:04:05"
)

func TimeNow() time.Time {
	return time.Now()
}

func ParseTimeSecondFormat(timeStr string) (time.Time, error) {
	return time.ParseInLocation(YYYY_MM_DD_HH_MM_SS, timeStr, time.Local)
}

func TimeSecondFormat(t time.Time) string {
	return t.Format(YYYY_MM_DD_HH_MM_SS)
}
