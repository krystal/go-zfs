// Package zfsprops is a helper package that privides a list of string
// constants for zfs native properties.
//
// Based on the zfsprops manpage available here:
// https://openzfs.github.io/openzfs-docs/man/7/zfsprops.7.html
//
// Description
//
// Properties are divided into two types, native properties and user-defined (or
// “user”) properties. Native properties either export internal statistics or
// control ZFS behavior. In addition, native properties are either editable or
// read-only. User properties have no effect on ZFS behavior, but you can use
// them to annotate datasets in a way that is meaningful in your environment.
//
// Native Properties
//
// Every dataset has a set of properties that export statistics about the
// dataset as well as control various behaviors. Properties are inherited from
// the parent unless overridden by the child. Some properties apply only to
// certain types of datasets (file systems, volumes, or snapshots).
//
// The values of numeric properties can be specified using human-readable
// suffixes (for example, k, KB, M, Gb, and so forth, up to Z for zettabyte).
// The following are all valid (and equal) specifications: 1536M, 1.5g, 1.50GB.
//
// The values of non-numeric properties are case sensitive and must be
// lowercase, except for mountpoint, sharenfs, and sharesmb.
//
//
// User Properties
//
// In addition to the standard native properties, ZFS supports arbitrary user
// properties. User properties have no effect on ZFS behavior, but applications
// or administrators can use them to annotate datasets (file systems, volumes,
// and snapshots).
//
// User property names must contain a colon (“:”) character to distinguish them
// from native properties. They may contain lowercase letters, numbers, and the
// following punctuation characters: colon (“:”), dash (“-”), period (“.”), and
// underscore (“_”). The expected convention is that the property name is
// divided into two portions such as module:property, but this namespace is not
// enforced by ZFS. User property names can be at most 256 characters, and
// cannot begin with a dash (“-”).
//
// When making programmatic use of user properties, it is strongly suggested to
// use a reversed DNS domain name for the module component of property names to
// reduce the chance that two independently-developed packages use the same
// property name for different purposes.
//
// The values of user properties are arbitrary strings, are always inherited,
// and are never validated. All of the commands that operate on properties (zfs
// list, zfs get, zfs set, and so forth) can be used to manipulate both native
// properties and user properties. Use the zfs inherit command to clear a user
// property. If the property is not defined in any parent dataset, it is removed
// entirely. Property values are limited to 8192 bytes.
package zfsprops

import "fmt"

// The following native properties consist of read-only statistics about the
// dataset. These properties can be neither set, nor inherited.
const (
	Available            = "available"
	CompressRatio        = "compressratio"
	CreateTxGroup        = "createtxg"
	Creation             = "creation"
	Clones               = "clones"
	DeferDestroy         = "defer_destroy"
	EncryptionRoot       = "encryptionroot"
	FilesystemCount      = "filesystem_count"
	KeyStatus            = "keystatus"
	GUID                 = "guid"
	LogicalReferenced    = "logicalreferenced"
	LogicalUsed          = "logicalused"
	Mounted              = "mounted"
	ObjsetID             = "objsetid"
	Origin               = "origin"
	ReceiveResumeToken   = "receive_resume_token"
	RedactSnaps          = "redact_snaps"
	Referenced           = "referenced"
	RefCompressRatio     = "refcompressratio"
	SnapshotCount        = "snapshot_count"
	Type                 = "type"
	Used                 = "used"
	UsedByChildren       = "usedbychildren"
	UsedByDataset        = "usedbydataset"
	UsedByRefReservation = "usedbyrefreservation"
	UsedBySnapshots      = "usedbysnapshots"
	VolBlockSize         = "volblocksize"
	Written              = "written"
)

// The following native properties can be used to change the behavior of a ZFS
// dataset.
//nolint:godot
const (
	// aclinherit=discard|noallow|restricted|passthrough|passthrough-x
	ACLInherit = "aclinherit"

	// aclmode=discard|groupmask|passthrough|restricted
	ACLMode = "aclmode"

	// acltype=off|nfsv4|posix
	ACLType = "acltype"

	// atime=on|off
	Atime = "atime"

	// canmount=on|off|noauto
	CanMount = "canmount"

	// checksum=on|off|fletcher2|fletcher4|sha256|noparity|sha512|skein|edonr
	Checksum = "checksum"

	//nolint:lll
	// compression=on|off|gzip|gzip-N|lz4|lzjb|zle|zstd|zstd-N|zstd-fast|zstd-fast-N
	Compression = "compression"

	// context=none|SELinux-User:SELinux-Role:SELinux-Type:Sensitivity-Level
	Context = "context"

	// fscontext=none|SELinux-User:SELinux-Role:SELinux-Type:Sensitivity-Level
	FSContext = "fscontext"

	// defcontext=none|SELinux-User:SELinux-Role:SELinux-Type:Sensitivity-Level
	DefContext = "defcontext"

	// rootcontext=none|SELinux-User:SELinux-Role:SELinux-Type:Sensitivity-Level
	RootContext = "rootcontext"

	// copies=1|2|3
	Copies = "copies"

	// devices=on|off
	Devices = "devices"

	//nolint:lll
	// dedup=off|on|verify|sha256[,verify]|sha512[,verify]|skein[,verify]|edonr,verify
	Dedup = "dedup"

	// dnodesize=legacy|auto|1k|2k|4k|8k|16k
	DNodeSize = "dnodesize"

	//nolint:lll
	// encryption=off|on|aes-128-ccm|aes-192-ccm|aes-256-ccm|aes-128-gcm|aes-192-gcm|aes-256-gcm
	Encryption = "encryption"

	// keyformat=raw|hex|passphrase
	KeyFormat = "keyformat"

	//nolint:lll
	// keylocation=prompt|file:///absolute/file/path|https://address|http://address
	KeyLocation = "keylocation"

	// pbkdf2iters=iterations
	PBKDF2Iterations = "pbkdf2iters"

	// exec=on|off
	Exec = "exec"

	// filesystem_limit=count|none
	FilesystemLimit = "filesystem_limit"

	// special_small_blocks=size
	SpecialSmallBlocks = "special_small_blocks"

	// mountpoint=path|none|legacy
	Mountpoint = "mountpoint"

	// nbmand=on|off
	Nbmand = "nbmand"

	// overlay=on|off
	Overlay = "overlay"

	// primarycache=all|none|metadata
	PrimaryCache = "primarycache"

	// quota=size|none
	Quota = "quota"

	// snapshot_limit=count|none
	SnapshotLimit = "snapshot_limit"

	// readonly=on|off
	ReadOnly = "readonly"

	// recordsize=size
	RecordSize = "recordsize"

	// redundant_metadata=all|most
	RedundantMetadata = "redundant_metadata"

	// refquota=size|none
	RefQuota = "refquota"

	// refreservation=size|none|auto
	RefReservation = "refreservation"

	// relatime=on|off
	RelAtime = "relatime"

	// reservation=size|none
	Reservation = "reservation"

	// secondarycache=all|none|metadata
	SecondaryCache = "secondarycache"

	// setuid=on|off
	SetUID = "setuid"

	// sharesmb=on|off|opts
	ShareSMB = "sharesmb"

	// sharenfs=on|off|opts
	ShareNFS = "sharenfs"

	// logbias=latency|throughput
	LogBias = "logbias"

	// snapdev=hidden|visible
	SnapDev = "snapdev"

	// snapdir=hidden|visible
	SnapDir = "snapdir"

	// sync=standard|always|disabled
	Sync = "sync"

	// version=N|current
	Version = "version"

	// volsize=size
	VolSize = "volsize"

	// volmode=default|full|geom|dev|none
	VolMode = "volmode"

	// vscan=on|off
	VScan = "vscan"

	// xattr=on|off|sa
	XAttr = "xattr"

	// jailed=off|on
	Jailed = "jailed"

	// zoned=on|off
	Zoned = "zoned"
)

// The following three properties cannot be changed after the file system is
// created, and therefore, should be set when the file system is created. If the
// properties are not set with the zfs create or zpool create commands, these
// properties are inherited from the parent dataset. If the parent dataset lacks
// these properties due to having been created prior to these features being
// supported, the new file system will have the default values for these
// properties.
//nolint:godot
const (
	// casesensitivity=sensitive|insensitive|mixed
	CaseSensitivity = "casesensitivity"

	// normalization=none|formC|formD|formKC|formKD
	Normalization = "normalization"

	// utf8only=on|off
	UTF8Only = "utf8only"
)

//nolint:godot
// userquota@user=size|none
func UserQuota(user string) string {
	return fmt.Sprintf("userquota@%s", user)
}

//nolint:godot
// userobjquota@user=size|none
func UserObjQuota(user string) string {
	return fmt.Sprintf("userobjquota@%s", user)
}

//nolint:godot
// groupquota@group=size|none
func GroupQuota(group string) string {
	return fmt.Sprintf("groupquota@%s", group)
}

//nolint:godot
// groupobjquota@group=size|none
func GroupObjQuota(group string) string {
	return fmt.Sprintf("groupobjquota@%s", group)
}

//nolint:godot
// projectquota@project=size|none
func ProjectQuota(project string) string {
	return fmt.Sprintf("projectquota@%s", project)
}

//nolint:godot
// projectobjquota@project=size|none
func ProjectObjQuota(project string) string {
	return fmt.Sprintf("projectobjquota@%s", project)
}

// User returns a function which can be used to create user properties that
// following the recommended convention of "module:property".
//
// Any value given to the returned function will be prefixed with "<module>:".
func User(module string) func(property string) string {
	return func(name string) string {
		return fmt.Sprintf("%s:%s", module, name)
	}
}
