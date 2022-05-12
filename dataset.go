package zfs

import (
	"strings"
	"time"

	"github.com/krystal/go-zfs/zfsprops"
)

type DatasetType string

const (
	AllTypes       DatasetType = "all"
	BookmarkType   DatasetType = "bookmark"
	FilesystemType DatasetType = "filesystem"
	SnapshotType   DatasetType = "snapshot"
	VolumeType     DatasetType = "volume"
)

// JoinTypes combines the given dataset types into a DatasetType value which can
// be used to query for datasets of multiple types.
func JoinTypes(types ...DatasetType) DatasetType {
	s := make([]string, len(types))
	for i, t := range types {
		s[i] = string(t)
	}

	return DatasetType(strings.Join(s, ","))
}

type Dataset struct {
	Properties
	Name string
}

func NewDataset(name string, properties Properties) *Dataset {
	props := Properties{}

	for _, prop := range properties {
		if prop.Name == name {
			props[prop.Property] = prop
		}
	}

	return &Dataset{
		Name:       name,
		Properties: props,
	}
}

// Atime return the value of the "atime" property as a bool.
//
// The second return value indicates if the property is present in the Dataset
// instance.
func (p *Dataset) Atime() (bool, bool) {
	return p.Bool(zfsprops.Atime)
}

// CanMount return the value of the "canmount" property as a bool.
//
// The second return value indicates if the property is present in the Dataset
// instance.
func (p *Dataset) CanMount() (bool, bool) {
	return p.Bool(zfsprops.CanMount)
}

// Devices return the value of the "devices" property as a bool.
//
// The second return value indicates if the property is present in the Dataset
// instance.
func (p *Dataset) Devices() (bool, bool) {
	return p.Bool(zfsprops.Devices)
}

// Exec returns the value of the "exec" property as a bool.
//
// The second return value indicates if the property is present in the Dataset
// instance.
func (p *Dataset) Exec() (bool, bool) {
	return p.Bool(zfsprops.Exec)
}

// ReadOnly returns the value of the "readonly" property as a bool.
//
// The second return value indicates if the property is present in the Dataset
// instance.
func (p *Dataset) ReadOnly() (bool, bool) {
	return p.Bool(zfsprops.ReadOnly)
}

// RelAtime returns the value of the "relatime" property as a bool.
//
// The second return value indicates if the property is present in the Dataset
// instance.
func (p *Dataset) RelAtime() (bool, bool) {
	return p.Bool(zfsprops.RelAtime)
}

// SetUID sets the value of the "setuid" property as a bool.
//
// The second return value indicates if the property is present in the Dataset
// instance.
func (p *Dataset) SetUID() (bool, bool) {
	return p.Bool(zfsprops.SetUID)
}

// Available returns the value of the "free" property as number of bytes.
//
// The second return value indicates if the property is present in the Dataset
// instance.
func (p *Dataset) Available() (uint64, bool) {
	return p.Bytes(zfsprops.Available)
}

// Quota returns the value of the "quota" property as number of bytes.
//
// The second return value indicates if the property is present in the Dataset
// instance.
func (p *Dataset) Quota() (uint64, bool) {
	return p.Bytes(zfsprops.Quota)
}

// RefQuota returns the value of the "refquota" property as number of bytes.
//
// The second return value indicates if the property is present in the Dataset
// instance.
func (p *Dataset) RefQuota() (uint64, bool) {
	return p.Bytes(zfsprops.RefQuota)
}

// RefReservation returns the value of the "refreservation" property as number
// of bytes.
//
// The second return value indicates if the property is present in the Dataset
// instance.
func (p *Dataset) RefReservation() (uint64, bool) {
	return p.Bytes(zfsprops.RefReservation)
}

// Reservation returns the value of the "reservation" property as number of
// bytes.
//
// The second return value indicates if the property is present in the Dataset
// instance.
func (p *Dataset) Reservation() (uint64, bool) {
	return p.Bytes(zfsprops.Reservation)
}

// VolSize returns the value of the "volsize" property as number of bytes.
//
// The second return value indicates if the property is present in the Dataset
// instance.
func (p *Dataset) VolSize() (uint64, bool) {
	return p.Bytes(zfsprops.VolSize)
}

// LogicalUsed returns the value of the "logicalused" property as number of
// bytes.
//
// The second return value indicates if the property is present in the Dataset
// instance.
func (p *Dataset) LogicalUsed() (uint64, bool) {
	return p.Bytes(zfsprops.LogicalUsed)
}

// LogicalReferenced returns the value of the "logicalreferenced" property as
// number of bytes.
//
// The second return value indicates if the property is present in the Dataset
// instance.
func (p *Dataset) LogicalReferenced() (uint64, bool) {
	return p.Bytes(zfsprops.LogicalReferenced)
}

// Used returns the value of the "used" property as number of bytes.
//
// The second return value indicates if the property is present in the Dataset
// instance.
func (p *Dataset) Used() (uint64, bool) {
	return p.Bytes(zfsprops.Used)
}

// UsedByChildren returns the value of the "usedbychildren" property as number
// of bytes.
//
// The second return value indicates if the property is present in the Dataset
// instance.
func (p *Dataset) UsedByChildren() (uint64, bool) {
	return p.Bytes(zfsprops.UsedByChildren)
}

// UsedByDataset returns the value of the "usedbydataset" property as number of
// bytes.
//
// The second return value indicates if the property is present in the Dataset
// instance.
func (p *Dataset) UsedByDataset() (uint64, bool) {
	return p.Bytes(zfsprops.UsedByDataset)
}

// UsedBySnapshots returns the value of the "usedbysnapshots" property as number
// of bytes.
//
// The second return value indicates if the property is present in the Dataset
// instance.
func (p *Dataset) UsedBySnapshots() (uint64, bool) {
	return p.Bytes(zfsprops.UsedBySnapshots)
}

// UsedByRefreservation returns the value of the "usedbyrefreservation" property
// as number of bytes.
//
// The second return value indicates if the property is present in the Dataset
// instance.
func (p *Dataset) UsedByRefReservation() (uint64, bool) {
	return p.Bytes(zfsprops.UsedByRefReservation)
}

// CompressRatio returns the value of the "compressratio" property as a float64.
//
// The second return value indicates if the property is present in the Dataset
// instance.
func (p *Dataset) CompressRatio() (float64, bool) {
	return p.Ratio(zfsprops.CompressRatio)
}

// RefCompressRatio returns the value of the "refcompressratio" property as a
// float64.
//
// The second return value indicates if the property is present in the Dataset
// instance.
func (p *Dataset) RefCompressRatio() (float64, bool) {
	return p.Ratio(zfsprops.RefCompressRatio)
}

// Checksum returns the value of the "checksum" property.
//
// The second return value indicates if the property is present in the Dataset
// instance.
func (p *Dataset) Checksum() (string, bool) {
	return p.String(zfsprops.Checksum)
}

// Compression returns the value of the "compression" property.
//
// The second return value indicates if the property is present in the Dataset
// instance.
func (p *Dataset) Compression() (string, bool) {
	return p.String(zfsprops.Compression)
}

// Mountpoint returns the value of the "mountpoint" property.
//
// The second return value indicates if the property is present in the Dataset
// instance. If the raw mountpoint value is "none", an empty string will be
// returned instead of "none".
func (p *Dataset) Mountpoint() (string, bool) {
	v, ok := p.String(zfsprops.Mountpoint)
	if v == "none" {
		v = ""
	}

	return v, ok
}

// Sync returns the value of the "sync" property.
//
// The second return value indicates if the property is present in the Dataset
// instance.
func (p *Dataset) Sync() (string, bool) {
	return p.String(zfsprops.Sync)
}

// Creation returns the value of the "creation" property as a time.Time.
//
// The second return value indicates if the property is present in the Dataset
// instance.
func (p *Dataset) Creation() (time.Time, bool) {
	return p.Time(zfsprops.Creation)
}

// Copies returns the value of the "copies" property as a uint64.
//
// The second return value indicates if the property is present in the Dataset
// instance.
func (p *Dataset) Copies() (uint64, bool) {
	return p.Uint64(zfsprops.Copies)
}

// Type returns the value of the "type" property as a DatasetType.
//
// The second return value indicates if the property is present in the Dataset
// instance.
func (p *Dataset) Type() (DatasetType, bool) {
	if v, ok := p.String(zfsprops.Type); ok {
		return DatasetType(v), true
	}

	return "", false
}
