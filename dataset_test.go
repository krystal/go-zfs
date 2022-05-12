package zfs

import (
	"testing"
	"time"

	"github.com/krystal/go-zfs/zfsprops"
	"github.com/stretchr/testify/assert"
)

func TestNewDataset(t *testing.T) {
	type args struct {
		name       string
		properties Properties
	}
	tests := []struct {
		name string
		args args
		want *Dataset
	}{
		{
			name: "empty",
			args: args{},
			want: &Dataset{Properties: Properties{}},
		},
		{
			name: "empty properties",
			args: args{
				name: "tank/my-dataset",
			},
			want: &Dataset{
				Name:       "tank/my-dataset",
				Properties: Properties{},
			},
		},
		{
			name: "name and properties",
			args: args{
				name: "tank/my-dataset",
				properties: Properties{
					(zfsprops.ReadOnly): {
						Name:     "tank/my-dataset",
						Property: "readonly",
						Value:    "on",
						Source:   "-",
					},
				},
			},
			want: &Dataset{
				Name: "tank/my-dataset",
				Properties: Properties{
					"readonly": {
						Name:     "tank/my-dataset",
						Property: "readonly",
						Value:    "on",
						Source:   "-",
					},
				},
			},
		},
		{
			name: "properties with wrong name",
			args: args{
				name: "tank/my-dataset",
				properties: Properties{
					(zfsprops.ReadOnly): {
						Name:     "tank/my-dataset",
						Property: "readonly",
						Value:    "on",
						Source:   "-",
					},
					(zfsprops.Quota): {
						Name:     "tank/my-dataset/sub-dataset",
						Property: "quota",
						Value:    "2G",
						Source:   "-",
					},
				},
			},
			want: &Dataset{
				Name: "tank/my-dataset",
				Properties: Properties{
					"readonly": {
						Name:     "tank/my-dataset",
						Property: "readonly",
						Value:    "on",
						Source:   "-",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewDataset(tt.args.name, tt.args.properties)

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestDataset_Checksum(t *testing.T) {
	type fields struct {
		Properties Properties
	}
	tests := []struct {
		name   string
		fields fields
		want   string
		wantOk bool
	}{
		{
			name: "not set",
			fields: fields{
				Properties: Properties{},
			},
			want:   "",
			wantOk: false,
		},
		{
			name: "empty",
			fields: fields{
				Properties: Properties{
					"checksum": {
						Name:     "tank/my-dataset",
						Property: "checksum",
						Value:    "",
						Source:   "-",
					},
				},
			},
			want:   "",
			wantOk: true,
		},
		{
			name: "blank",
			fields: fields{
				Properties: Properties{
					"checksum": {
						Name:     "tank/my-dataset",
						Property: "checksum",
						Value:    "-",
						Source:   "-",
					},
				},
			},
			want:   "",
			wantOk: false,
		},
		{
			name: "on",
			fields: fields{
				Properties: Properties{
					"checksum": {
						Name:     "tank/my-dataset",
						Property: "checksum",
						Value:    "on",
						Source:   "-",
					},
				},
			},
			want:   "on",
			wantOk: true,
		},
		{
			name: "off",
			fields: fields{
				Properties: Properties{
					"checksum": {
						Name:     "tank/my-dataset",
						Property: "checksum",
						Value:    "off",
						Source:   "-",
					},
				},
			},
			want:   "off",
			wantOk: true,
		},
		{
			name: "fletcher2",
			fields: fields{
				Properties: Properties{
					"checksum": {
						Name:     "tank/my-dataset",
						Property: "checksum",
						Value:    "fletcher2",
						Source:   "-",
					},
				},
			},
			want:   "fletcher2",
			wantOk: true,
		},
		{
			name: "fletcher4",
			fields: fields{
				Properties: Properties{
					"checksum": {
						Name:     "tank/my-dataset",
						Property: "checksum",
						Value:    "fletcher4",
						Source:   "-",
					},
				},
			},
			want:   "fletcher4",
			wantOk: true,
		},
		{
			name: "sha256",
			fields: fields{
				Properties: Properties{
					"checksum": {
						Name:     "tank/my-dataset",
						Property: "checksum",
						Value:    "sha256",
						Source:   "-",
					},
				},
			},
			want:   "sha256",
			wantOk: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &Dataset{
				Properties: tt.fields.Properties,
			}

			got, gotOk := d.Checksum()

			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantOk, gotOk)
		})
	}
}

func TestDataset_Compression(t *testing.T) {
	type fields struct {
		Properties Properties
	}
	tests := []struct {
		name   string
		fields fields
		want   string
		wantOk bool
	}{
		{
			name: "not set",
			fields: fields{
				Properties: Properties{},
			},
			want:   "",
			wantOk: false,
		},
		{
			name: "empty",
			fields: fields{
				Properties: Properties{
					"compression": {
						Name:     "tank/my-dataset",
						Property: "compression",
						Value:    "",
						Source:   "-",
					},
				},
			},
			want:   "",
			wantOk: true,
		},
		{
			name: "blank",
			fields: fields{
				Properties: Properties{
					"compression": {
						Name:     "tank/my-dataset",
						Property: "compression",
						Value:    "-",
						Source:   "-",
					},
				},
			},
			want:   "",
			wantOk: false,
		},
		{
			name: "on",
			fields: fields{
				Properties: Properties{
					"compression": {
						Name:     "tank/my-dataset",
						Property: "compression",
						Value:    "on",
						Source:   "-",
					},
				},
			},
			want:   "on",
			wantOk: true,
		},
		{
			name: "off",
			fields: fields{
				Properties: Properties{
					"compression": {
						Name:     "tank/my-dataset",
						Property: "compression",
						Value:    "off",
						Source:   "-",
					},
				},
			},
			want:   "off",
			wantOk: true,
		},
		{
			name: "gzip",
			fields: fields{
				Properties: Properties{
					"compression": {
						Name:     "tank/my-dataset",
						Property: "compression",
						Value:    "gzip",
						Source:   "-",
					},
				},
			},
			want:   "gzip",
			wantOk: true,
		},
		{
			name: "lz4",
			fields: fields{
				Properties: Properties{
					"compression": {
						Name:     "tank/my-dataset",
						Property: "compression",
						Value:    "lz4",
						Source:   "-",
					},
				},
			},
			want:   "lz4",
			wantOk: true,
		},
		{
			name: "zle",
			fields: fields{
				Properties: Properties{
					"compression": {
						Name:     "tank/my-dataset",
						Property: "compression",
						Value:    "zle",
						Source:   "-",
					},
				},
			},
			want:   "zle",
			wantOk: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &Dataset{
				Properties: tt.fields.Properties,
			}

			got, gotOk := d.Compression()

			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantOk, gotOk)
		})
	}
}

func TestDataset_Mountpoint(t *testing.T) {
	type fields struct {
		Properties Properties
	}
	tests := []struct {
		name   string
		fields fields
		want   string
		wantOk bool
	}{
		{
			name: "not set",
			fields: fields{
				Properties: Properties{},
			},
			want:   "",
			wantOk: false,
		},
		{
			name: "empty",
			fields: fields{
				Properties: Properties{
					"mountpoint": {
						Name:     "tank/my-dataset",
						Property: "mountpoint",
						Value:    "",
						Source:   "-",
					},
				},
			},
			want:   "",
			wantOk: true,
		},
		{
			name: "blank",
			fields: fields{
				Properties: Properties{
					"mountpoint": {
						Name:     "tank/my-dataset",
						Property: "mountpoint",
						Value:    "-",
						Source:   "-",
					},
				},
			},
			want:   "",
			wantOk: false,
		},
		{
			name: "none",
			fields: fields{
				Properties: Properties{
					"mountpoint": {
						Name:     "tank/my-dataset",
						Property: "mountpoint",
						Value:    "none",
						Source:   "local",
					},
				},
			},
			want:   "",
			wantOk: true,
		},
		{
			name: "/mnt/my-dataset",
			fields: fields{
				Properties: Properties{
					"mountpoint": {
						Name:     "tank/my-dataset",
						Property: "mountpoint",
						Value:    "/mnt/my-dataset",
						Source:   "default",
					},
				},
			},
			want:   "/mnt/my-dataset",
			wantOk: true,
		},
		{
			name: "/tmp/LEWZUBUyBFFX02kQ/my-dataset",
			fields: fields{
				Properties: Properties{
					"mountpoint": {
						Name:     "tank/my-dataset",
						Property: "mountpoint",
						Value:    "/tmp/LEWZUBUyBFFX02kQ/my-dataset",
						Source:   "local",
					},
				},
			},
			want:   "/tmp/LEWZUBUyBFFX02kQ/my-dataset",
			wantOk: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &Dataset{
				Properties: tt.fields.Properties,
			}

			got, gotOk := d.Mountpoint()

			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantOk, gotOk)
		})
	}
}

func TestDataset_Sync(t *testing.T) {
	type fields struct {
		Properties Properties
	}
	tests := []struct {
		name   string
		fields fields
		want   string
		wantOk bool
	}{
		{
			name: "not set",
			fields: fields{
				Properties: Properties{},
			},
			want:   "",
			wantOk: false,
		},
		{
			name: "empty",
			fields: fields{
				Properties: Properties{
					"sync": {
						Name:     "tank/my-dataset",
						Property: "sync",
						Value:    "",
						Source:   "-",
					},
				},
			},
			want:   "",
			wantOk: true,
		},
		{
			name: "blank",
			fields: fields{
				Properties: Properties{
					"sync": {
						Name:     "tank/my-dataset",
						Property: "sync",
						Value:    "-",
						Source:   "-",
					},
				},
			},
			want:   "",
			wantOk: false,
		},
		{
			name: "standard",
			fields: fields{
				Properties: Properties{
					"sync": {
						Name:     "tank/my-dataset",
						Property: "sync",
						Value:    "standard",
						Source:   "-",
					},
				},
			},
			want:   "standard",
			wantOk: true,
		},
		{
			name: "always",
			fields: fields{
				Properties: Properties{
					"sync": {
						Name:     "tank/my-dataset",
						Property: "sync",
						Value:    "always",
						Source:   "-",
					},
				},
			},
			want:   "always",
			wantOk: true,
		},
		{
			name: "disabled",
			fields: fields{
				Properties: Properties{
					"sync": {
						Name:     "tank/my-dataset",
						Property: "sync",
						Value:    "disabled",
						Source:   "-",
					},
				},
			},
			want:   "disabled",
			wantOk: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &Dataset{
				Properties: tt.fields.Properties,
			}

			got, gotOk := d.Sync()

			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantOk, gotOk)
		})
	}
}

func TestDataset_Type(t *testing.T) {
	type fields struct {
		Properties Properties
	}
	tests := []struct {
		name   string
		fields fields
		want   DatasetType
		wantOk bool
	}{
		{
			name: "not set",
			fields: fields{
				Properties: Properties{},
			},
			want:   "",
			wantOk: false,
		},
		{
			name: "empty",
			fields: fields{
				Properties: Properties{
					"type": {
						Name:     "tank/my-dataset",
						Property: "type",
						Value:    "",
						Source:   "-",
					},
				},
			},
			want:   "",
			wantOk: true,
		},
		{
			name: "blank",
			fields: fields{
				Properties: Properties{
					"type": {
						Name:     "tank/my-dataset",
						Property: "type",
						Value:    "-",
						Source:   "-",
					},
				},
			},
			want:   "",
			wantOk: false,
		},
		{
			name: "bookmark",
			fields: fields{
				Properties: Properties{
					"type": {
						Name:     "tank/my-dataset",
						Property: "type",
						Value:    "bookmark",
						Source:   "-",
					},
				},
			},
			want:   BookmarkType,
			wantOk: true,
		},
		{
			name: "filesystem",
			fields: fields{
				Properties: Properties{
					"type": {
						Name:     "tank/my-dataset",
						Property: "type",
						Value:    "filesystem",
						Source:   "-",
					},
				},
			},
			want:   FilesystemType,
			wantOk: true,
		},
		{
			name: "snapshot",
			fields: fields{
				Properties: Properties{
					"type": {
						Name:     "tank/my-dataset",
						Property: "type",
						Value:    "snapshot",
						Source:   "-",
					},
				},
			},
			want:   SnapshotType,
			wantOk: true,
		},
		{
			name: "volume",
			fields: fields{
				Properties: Properties{
					"type": {
						Name:     "tank/my-dataset",
						Property: "type",
						Value:    "volume",
						Source:   "-",
					},
				},
			},
			want:   VolumeType,
			wantOk: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &Dataset{
				Properties: tt.fields.Properties,
			}

			got, gotOk := d.Type()

			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantOk, gotOk)
		})
	}
}

func TestDataset_Bool(t *testing.T) {
	props := []struct {
		name     string
		property string
		lookup   func(*Dataset) (bool, bool)
	}{
		{
			name:     "Bool",
			property: "atime",
			lookup: func(d *Dataset) (bool, bool) {
				return d.Bool("atime")
			},
		},
		{
			name:     "Atime",
			property: "atime",
			lookup: func(d *Dataset) (bool, bool) {
				return d.Atime()
			},
		},
		{
			name:     "CanMount",
			property: "canmount",
			lookup: func(d *Dataset) (bool, bool) {
				return d.CanMount()
			},
		},
		{
			name:     "RelAtime",
			property: "relatime",
			lookup: func(d *Dataset) (bool, bool) {
				return d.RelAtime()
			},
		},
		{
			name:     "ReadOnly",
			property: "readonly",
			lookup: func(d *Dataset) (bool, bool) {
				return d.ReadOnly()
			},
		},
		{
			name:     "Exec",
			property: "exec",
			lookup: func(d *Dataset) (bool, bool) {
				return d.Exec()
			},
		},
		{
			name:     "SetUID",
			property: "setuid",
			lookup: func(d *Dataset) (bool, bool) {
				return d.SetUID()
			},
		},
		{
			name:     "Devices",
			property: "devices",
			lookup: func(d *Dataset) (bool, bool) {
				return d.Devices()
			},
		},
	}
	tests := []struct {
		name   string
		value  string
		unset  bool
		want   bool
		wantOk bool
	}{
		{
			name:   "not set",
			unset:  true,
			want:   false,
			wantOk: false,
		},
		{
			name:   "empty",
			value:  "",
			want:   false,
			wantOk: false,
		},
		{
			name:   "blank",
			value:  "-",
			want:   false,
			wantOk: false,
		},
		{
			name:   "on",
			value:  "on",
			want:   true,
			wantOk: true,
		},
		{
			name:   "off",
			value:  "off",
			want:   false,
			wantOk: true,
		},
		{
			name:   "enabled",
			value:  "enabled",
			want:   true,
			wantOk: true,
		},
		{
			name:   "disabled",
			value:  "disabled",
			want:   false,
			wantOk: true,
		},
	}
	for _, prop := range props {
		t.Run(prop.name, func(t *testing.T) {
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					d := &Dataset{}

					if !tt.unset {
						d.Properties = Properties{
							(prop.property): {
								Name:     "tank/my-dataset",
								Property: prop.property,
								Value:    tt.value,
								Source:   "-",
							},
						}
					}

					got, gotOk := prop.lookup(d)

					assert.Equal(t, tt.want, got)
					assert.Equal(t, tt.wantOk, gotOk)
				})
			}
		})
	}
}

func TestDataset_Bytes(t *testing.T) {
	props := []struct {
		name     string
		property string
		lookup   func(*Dataset) (uint64, bool)
	}{
		{
			name:     "Bytes",
			property: "available",
			lookup: func(d *Dataset) (uint64, bool) {
				return d.Bytes("available")
			},
		},
		{
			name:     "Available",
			property: "available",
			lookup: func(d *Dataset) (uint64, bool) {
				return d.Available()
			},
		},
		{
			name:     "Quota",
			property: "quota",
			lookup: func(d *Dataset) (uint64, bool) {
				return d.Quota()
			},
		},
		{
			name:     "RefQuota",
			property: "refquota",
			lookup: func(d *Dataset) (uint64, bool) {
				return d.RefQuota()
			},
		},
		{
			name:     "RefReservation",
			property: "refreservation",
			lookup: func(d *Dataset) (uint64, bool) {
				return d.RefReservation()
			},
		},
		{
			name:     "Reservation",
			property: "reservation",
			lookup: func(d *Dataset) (uint64, bool) {
				return d.Reservation()
			},
		},
		{
			name:     "LogicalUsed",
			property: "logicalused",
			lookup: func(d *Dataset) (uint64, bool) {
				return d.LogicalUsed()
			},
		},
		{
			name:     "LogicalReferenced",
			property: "logicalreferenced",
			lookup: func(d *Dataset) (uint64, bool) {
				return d.LogicalReferenced()
			},
		},
		{
			name:     "Used",
			property: "used",
			lookup: func(d *Dataset) (uint64, bool) {
				return d.Used()
			},
		},
		{
			name:     "UsedByChildren",
			property: "usedbychildren",
			lookup: func(d *Dataset) (uint64, bool) {
				return d.UsedByChildren()
			},
		},
		{
			name:     "UsedByDataset",
			property: "usedbydataset",
			lookup: func(d *Dataset) (uint64, bool) {
				return d.UsedByDataset()
			},
		},
		{
			name:     "UsedByRefreservation",
			property: "usedbyrefreservation",
			lookup: func(d *Dataset) (uint64, bool) {
				return d.UsedByRefReservation()
			},
		},
		{
			name:     "UsedBySnapshots",
			property: "usedbysnapshots",
			lookup: func(d *Dataset) (uint64, bool) {
				return d.UsedBySnapshots()
			},
		},
		{
			name:     "VolSize",
			property: "volsize",
			lookup: func(d *Dataset) (uint64, bool) {
				return d.VolSize()
			},
		},
	}
	tests := []struct {
		name   string
		value  string
		unset  bool
		want   uint64
		wantOk bool
	}{
		{
			name:   "not set",
			unset:  true,
			want:   0,
			wantOk: false,
		},
		{
			name:   "empty",
			value:  "",
			want:   0,
			wantOk: false,
		},
		{
			name:   "blank",
			value:  "-",
			want:   0,
			wantOk: false,
		},
		{
			name:   "239",
			value:  "239",
			want:   239, // 239 bytes
			wantOk: true,
		},
		{
			name:   "42K",
			value:  "42K",
			want:   42 * 1024, // 42 KiB
			wantOk: true,
		},
		{
			name:   "383M",
			value:  "383M",
			want:   383 * 1024 * 1024, // 383 MiB
			wantOk: true,
		},
		{
			name:   "84G",
			value:  "84G",
			want:   84 * 1024 * 1024 * 1024, // 84 GiB
			wantOk: true,
		},
		{
			name:   "483T",
			value:  "483T",
			want:   483 * 1024 * 1024 * 1024 * 1024, // 483 TiB
			wantOk: true,
		},
		{
			name:   "1023P",
			value:  "1023P",
			want:   1023 * 1024 * 1024 * 1024 * 1024 * 1024, // 1023 PiB
			wantOk: true,
		},
	}
	for _, prop := range props {
		t.Run(prop.name, func(t *testing.T) {
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					d := &Dataset{}

					if !tt.unset {
						d.Properties = Properties{
							(prop.property): {
								Name:     "tank/my-dataset",
								Property: prop.property,
								Value:    tt.value,
								Source:   "-",
							},
						}
					}

					got, gotOk := prop.lookup(d)

					assert.Equal(t, tt.want, got)
					assert.Equal(t, tt.wantOk, gotOk)
				})
			}
		})
	}
}

func TestDataset_Ratio(t *testing.T) {
	props := []struct {
		name     string
		property string
		lookup   func(*Dataset) (float64, bool)
	}{
		{
			name:     "Ratio",
			property: "compressratio",
			lookup: func(d *Dataset) (float64, bool) {
				return d.Ratio("compressratio")
			},
		},
		{
			name:     "CompressRatio",
			property: "compressratio",
			lookup: func(d *Dataset) (float64, bool) {
				return d.CompressRatio()
			},
		},
		{
			name:     "RefCompressRatio",
			property: "refcompressratio",
			lookup: func(d *Dataset) (float64, bool) {
				return d.RefCompressRatio()
			},
		},
	}
	tests := []struct {
		name   string
		value  string
		unset  bool
		want   float64
		wantOk bool
	}{
		{
			name:   "not set",
			unset:  true,
			want:   0.0,
			wantOk: false,
		},
		{
			name:   "empty",
			value:  "",
			want:   0.0,
			wantOk: false,
		},
		{
			name:   "blank",
			value:  "-",
			want:   0.0,
			wantOk: false,
		},
		{
			name:   "0.01",
			value:  "0.01",
			want:   0.01,
			wantOk: true,
		},
		{
			name:   "0.01x",
			value:  "0.01x",
			want:   0.01,
			wantOk: true,
		},
		{
			name:   "0.42",
			value:  "0.42",
			want:   0.42,
			wantOk: true,
		},
		{
			name:   "0.42x",
			value:  "0.42x",
			want:   0.42,
			wantOk: true,
		},
		{
			name:   "1.00",
			value:  "1.00",
			want:   1.00,
			wantOk: true,
		},
		{
			name:   "1.00x",
			value:  "1.00x",
			want:   1.00,
			wantOk: true,
		},
		{
			name:   "18.47",
			value:  "18.47",
			want:   18.47,
			wantOk: true,
		},
		{
			name:   "18.47x",
			value:  "18.47x",
			want:   18.47,
			wantOk: true,
		},
	}
	for _, prop := range props {
		t.Run(prop.name, func(t *testing.T) {
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					d := &Dataset{}

					if !tt.unset {
						d.Properties = Properties{
							(prop.property): {
								Name:     "tank/my-dataset",
								Property: prop.property,
								Value:    tt.value,
								Source:   "-",
							},
						}
					}

					got, gotOk := prop.lookup(d)

					assert.Equal(t, tt.want, got)
					assert.Equal(t, tt.wantOk, gotOk)
				})
			}
		})
	}
}

func TestDataset_Time(t *testing.T) {
	props := []struct {
		name     string
		property string
		lookup   func(*Dataset) (time.Time, bool)
	}{
		{
			name:     "Time",
			property: "creation",
			lookup: func(d *Dataset) (time.Time, bool) {
				return d.Time("creation")
			},
		},
		{
			name:     "Creation",
			property: "creation",
			lookup: func(d *Dataset) (time.Time, bool) {
				return d.Creation()
			},
		},
	}
	tests := []struct {
		name   string
		value  string
		unset  bool
		want   time.Time
		wantOk bool
	}{
		{
			name:   "not set",
			unset:  true,
			want:   time.Time{},
			wantOk: false,
		},
		{
			name:   "empty",
			value:  "",
			want:   time.Time{},
			wantOk: false,
		},
		{
			name:   "blank",
			value:  "-",
			want:   time.Time{},
			wantOk: false,
		},
		{
			name:   "timestamp",
			value:  "1651487819",
			want:   time.Date(2022, time.May, 2, 10, 36, 59, 0, time.UTC),
			wantOk: true,
		},
		{
			name:   "human readable",
			value:  "Mon May  2 10:36 2022",
			want:   time.Date(2022, time.May, 2, 10, 36, 0, 0, time.UTC),
			wantOk: true,
		},
	}
	for _, prop := range props {
		t.Run(prop.name, func(t *testing.T) {
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					d := &Dataset{}

					if !tt.unset {
						d.Properties = Properties{
							(prop.property): {
								Name:     "tank/my-dataset",
								Property: prop.property,
								Value:    tt.value,
								Source:   "-",
							},
						}
					}

					got, gotOk := prop.lookup(d)

					assert.Equal(t, tt.want, got)
					assert.Equal(t, tt.wantOk, gotOk)
				})
			}
		})
	}
}

func TestDataset_Uint64(t *testing.T) {
	props := []struct {
		name     string
		property string
		lookup   func(*Dataset) (uint64, bool)
	}{
		{
			name:     "Uint64",
			property: "copies",
			lookup: func(d *Dataset) (uint64, bool) {
				return d.Uint64("copies")
			},
		},
		{
			name:     "Copies",
			property: "copies",
			lookup: func(d *Dataset) (uint64, bool) {
				return d.Copies()
			},
		},
	}
	tests := []struct {
		name   string
		value  string
		unset  bool
		want   uint64
		wantOk bool
	}{
		{
			name:   "not set",
			unset:  true,
			want:   0,
			wantOk: false,
		},
		{
			name:   "empty",
			value:  "",
			want:   0,
			wantOk: false,
		},
		{
			name:   "blank",
			value:  "-",
			want:   0,
			wantOk: false,
		},
		{
			name:   "-1",
			value:  "-1",
			want:   0,
			wantOk: false,
		},
		{
			name:   "1",
			value:  "1",
			want:   1,
			wantOk: true,
		},
		{
			name:   "129",
			value:  "129",
			want:   129,
			wantOk: true,
		},
		{
			name:   "18446744073709551615",
			value:  "18446744073709551615",
			want:   18446744073709551615,
			wantOk: true,
		},
		{
			name:   "18446744073709551616",
			value:  "18446744073709551616",
			want:   0,
			wantOk: false,
		},
	}
	for _, prop := range props {
		t.Run(prop.name, func(t *testing.T) {
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					d := &Dataset{}

					if !tt.unset {
						d.Properties = Properties{
							(prop.property): {
								Name:     "tank/my-dataset",
								Property: prop.property,
								Value:    tt.value,
								Source:   "-",
							},
						}
					}

					got, gotOk := prop.lookup(d)

					assert.Equal(t, tt.want, got)
					assert.Equal(t, tt.wantOk, gotOk)
				})
			}
		})
	}
}
