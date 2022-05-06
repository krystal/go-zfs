package zfs

import (
	"bytes"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
)

const allProperty = "all"

// parseTabular parses a tab-delimited []byte slice into a slice of string
// slices. ZFS output is tab-delimited when the -H flag is used, and each line
// is split on TAB.
//
// For example if "zpool get -H size,capacity,altroot" returned the following:
//
//  zfs-local-test	size	336M	-
//  zfs-local-test	capacity	9%	-
//  zfs-local-test	altroot	-	default
//  zfs-other-test	size	336M	-
//  zfs-other-test	capacity	0%	-
//  zfs-other-test	altroot	-	default
//
// The return value would be:
//
//  [][]string{
//      {"zfs-local-test", "size", "336M", "-"},
//      {"zfs-local-test", "capacity", "9%", "-"},
//      {"zfs-local-test", "altroot", "-", "default"},
//      {"zfs-other-test", "size", "336M", "-"},
//      {"zfs-other-test", "capacity", "0%", "-"},
//      {"zfs-other-test", "altroot", "-", "default"},
//  }
//
// The retuned slice is dubbed "records", and should be passed to newProperties
// to create a Properties map.
func parseTabular(data []byte) [][]string {
	records := [][]string{}

	lines := bytes.Split(data, []byte{10})
	for _, line := range lines {
		records = append(records, strings.Split(string(line), "\t"))
	}

	return records
}

// Property represents a single ZFS property.
type Property struct {
	// Name is the name of the ZFS pool/dataset that the property belongs to.
	Name string

	// Property is the name of the property itself.
	Property string

	// Value is the value of the property.
	Value string

	// Source is the source of the property.
	Source string
}

// Properties is a collection of ZFS properties, that includes typed accessor
// helper methods.
type Properties map[string]Property

// newProperties accepts a "records" slice of string slices, typically as
// returned by parseTabular, and returns a Properties map.
//
// For example, if given the following "records" [][]string value:
//
//  [][]string{
//      {"zfs-local-test", "size", "336M", "-"},
//      {"zfs-local-test", "capacity", "9%", "-"},
//      {"zfs-local-test", "altroot", "-", "default"},
//      {"zfs-other-test", "size", "336M", "-"},
//      {"zfs-other-test", "capacity", "0%", "-"},
//      {"zfs-other-test", "altroot", "-", "default"},
//  }
//
// The resulting Properties map would be:
//
//  map[string]Properties{
//      "zfs-local-test": map[string]Property{
//          "size": Property{
//              Name:     "zfs-local-test",
//              Property: "size",
//              Value:    "336M",
//              Source:   "-",
//          },
//          "capacity": Property{
//              Name:     "zfs-local-test",
//              Property: "capacity",
//              Value:    "9%",
//              Source:   "-",
//          },
//          "altroot": Property{
//              Name:     "zfs-local-test",
//              Property: "altroot",
//              Value:    "-",
//              Source:   "default",
//          },
//      },
//      "zfs-other-test": map[string]Property{
//          "size": Property{
//              Name:     "zfs-other-test",
//              Property: "size",
//              Value:    "336M",
//              Source:   "-",
//          },
//          "capacity": Property{
//              Name:     "zfs-other-test",
//              Property: "capacity",
//              Value:    "0%",
//              Source:   "-",
//          },
//          "altroot": Property{
//              Name:     "zfs-other-test",
//              Property: "altroot",
//              Value:    "-",
//              Source:   "default",
//          },
//      },
//  }
func newProperties(records [][]string) map[string]Properties {
	r := map[string]Properties{}
	for _, record := range records {
		if len(record) == 4 && record[0] != "" {
			if _, ok := r[record[0]]; !ok {
				r[record[0]] = map[string]Property{}
			}

			r[record[0]][record[1]] = Property{
				Name:     record[0],
				Property: record[1],
				Value:    record[2],
				Source:   record[3],
			}
		}
	}

	return r
}

// String returns the value of the given property.
//
// The second return value indicates if the property is present and could
// successfully be parsed.
func (p Properties) String(property string) (string, bool) {
	if prop, ok := p[property]; ok && prop.Value != "-" {
		return prop.Value, true
	}

	return "", false
}

// Bytes returns the value of the given property as number of bytes.
//
// The second return value indicates if the property is present and could
// successfully be parsed.
func (p Properties) Bytes(property string) (uint64, bool) {
	if prop, ok := p[property]; ok && prop.Value != "-" {
		if r, err := p.parseSize(prop.Value); err == nil {
			return r, true
		}
	}

	return 0, false
}

// Percent returns the value of the given property as a uint64. It will strip
// any trailing "%" before parsing it as a uint64, ensuring it can handle
// percent-based values like "1%" and "42%".
//
// The second return value indicates if the property is present and could
// successfully be parsed.
func (p Properties) Percent(property string) (uint64, bool) {
	if prop, ok := p[property]; ok && prop.Value != "-" {
		v := strings.TrimSuffix(prop.Value, "%")
		if r, err := strconv.ParseUint(v, 10, 64); err == nil {
			return r, true
		}
	}

	return 0, false
}

// Ratio returns the value of the given property as a float64. It will strip any
// trailing "x" before parsing it as a float64, ensuring it can handle
// ratio-based values like "1x" and "0.5x".
//
// The second return value indicates if the property is present and could
// successfully be parsed.
func (p Properties) Ratio(property string) (float64, bool) {
	if prop, ok := p[property]; ok && prop.Value != "-" {
		v := strings.TrimSuffix(prop.Value, "x")
		if r, err := strconv.ParseFloat(v, 64); err == nil {
			return r, true
		}
	}

	return 0, false
}

// Bool returns the value of the given property as a bool. Only "on" and
// "enabled" are considered true, all other value return false.
//
// The second return value indicates if the property is present and could
// successfully be parsed.
func (p Properties) Bool(property string) (bool, bool) {
	if prop, ok := p[property]; ok && prop.Value != "" && prop.Value != "-" {
		return p.parseBool(prop.Value), true
	}

	return false, false
}

// Time returns the value of the given property as a time.Time. It can handle
// both unix timestamp values from ZFS (-p flag) and human readable time values.
//
// The second return value indicates if the property is present and could
// successfully be parsed.
func (p Properties) Time(property string) (time.Time, bool) {
	if prop, ok := p[property]; ok && prop.Value != "" && prop.Value != "-" {
		if v, err := p.parseTime(prop.Value); err == nil {
			return v, true
		}
	}

	return time.Time{}, false
}

// Uint64 returns the value of the given property as a uint64.
//
// The second return value indicates if the property is present and could
// successfully be parsed.
func (p Properties) Uint64(property string) (uint64, bool) {
	if prop, ok := p[property]; ok && prop.Value != "" && prop.Value != "-" {
		if r, err := strconv.ParseUint(prop.Value, 10, 64); err == nil {
			return r, true
		}
	}

	return 0, false
}

func propertyMapFlags(flag string, properties map[string]string) []string {
	props := []string{}
	for key, prop := range properties {
		props = append(props, fmt.Sprintf("%s=%s", key, prop))
	}
	sort.Strings(props)

	r := []string{}
	for _, prop := range props {
		r = append(r, flag, prop)
	}

	return r
}

var zfsIECSizeRegexp = regexp.MustCompile(`^([0-9]+)\s*([a-zA-Z])$`)

func (p Properties) parseSize(size string) (uint64, error) {
	s := strings.TrimSpace(size)
	if zfsIECSizeRegexp.MatchString(s) {
		s += "iB"
	}

	return humanize.ParseBytes(s)
}

func (p Properties) parseBool(str string) bool {
	switch str {
	case "on", "On", "ON", "enabled", "Enabled", "ENABLED":
		return true
	}

	return false
}

func (p Properties) parseTime(str string) (time.Time, error) {
	str = strings.TrimSpace(str)

	v, err := strconv.ParseInt(str, 10, 64)
	if err == nil {
		return time.Unix(v, 0).UTC(), nil
	}

	t, err := time.Parse("Mon Jan _2 15:04 2006", str)

	return t.UTC(), err
}
