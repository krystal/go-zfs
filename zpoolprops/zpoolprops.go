// Package zpoolprops is a helper package that privides a list of string
// constants for zpool properties.
//
// Based on the zpoolprops manpage available here:
// https://openzfs.github.io/openzfs-docs/man/7/zpoolprops.7.html
//
// Description
//
// Each pool has several properties associated with it. Some properties are
// read-only statistics while others are configurable and change the behavior of
// the pool.
package zpoolprops

import "fmt"

// The following are read-only properties.
const (
	Allocated     = "allocated"
	Capacity      = "capacity"
	ExpandSize    = "expandsize"
	Fragmentation = "fragmentation"
	Free          = "free"
	Freeing       = "freeing"
	Leaked        = "leaked"
	Health        = "health"
	GUID          = "guid"
	LoadGUID      = "load_guid"
	Size          = "size"
)

// The following properties can be set at creation time and import time.
const (
	AltRoot = "altroot"
)

// The following properties can be set only at import time.
const (
	ReadOnly = "readonly"
)

// The following properties can be set at creation time and import time, and
// later changed with the zpool set command.
//nolint:godot
const (
	// ashift=ashift
	Ashift = "ashift"

	// autoexpand=on|off
	AutoExpand = "autoexpand"

	// autoreplace=on|off
	AutoReplace = "autoreplace"

	// autotrim=on|off
	AutoTrim = "autotrim"

	// bootfs=(unset)|pool[/dataset]
	Bootfs = "bootfs"

	// cachefile=path|none
	Cachefile = "cachefile"

	// comment=text
	Comment = "comment"

	// compatibility=off|legacy|file[,file]...
	Compatibility = "compatibility"

	// dedupditto=number
	DedupDitto = "dedupditto"

	// delegation=on|off
	Delegation = "delegation"

	// failmode=wait|continue|panic
	FailMode = "failmode"

	// listsnapshots=on|off
	ListSnapshots = "listsnapshots"

	// multihost=on|off
	MultiHost = "multihost"

	// version=version
	Version = "version"
)

//nolint:godot,revive
// feature@feature_name=enabled
func Feature(feature_name string) string {
	return fmt.Sprintf("feature@%s", feature_name)
}
