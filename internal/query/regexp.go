package query

import "regexp"

// matchString wraps regexp.MatchString.
func matchString(pattern, s string) (bool, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return false, err
	}
	return re.MatchString(s), nil
}
