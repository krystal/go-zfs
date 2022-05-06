package zfs

import (
	"testing"

	"github.com/krystal/go-zfs/zpoolprops"
	"github.com/stretchr/testify/assert"
)

func TestPoolHealths(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		{name: HealthDegraded, want: "DEGRADED"},
		{name: HealthFaulted, want: "FAULTED"},
		{name: HealthOffline, want: "OFFLINE"},
		{name: HealthOnline, want: "ONLINE"},
		{name: HealthRemoved, want: "REMOVED"},
		{name: HealthUnavailable, want: "UNAVAIL"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.name)
		})
	}
}

func TestNewPool(t *testing.T) {
	type args struct {
		name       string
		properties Properties
	}
	tests := []struct {
		name string
		args args
		want *Pool
	}{
		{
			name: "empty",
			args: args{},
			want: &Pool{Properties: Properties{}},
		},
		{
			name: "empty properties",
			args: args{
				name: "my-test-pool",
			},
			want: &Pool{
				Name:       "my-test-pool",
				Properties: Properties{},
			},
		},
		{
			name: "name and properties",
			args: args{
				name: "my-test-pool",
				properties: Properties{
					(zpoolprops.AutoTrim): {
						Name:     "my-test-pool",
						Property: "autotrim",
						Value:    "on",
						Source:   "-",
					},
				},
			},
			want: &Pool{
				Name: "my-test-pool",
				Properties: Properties{
					"autotrim": {
						Name:     "my-test-pool",
						Property: "autotrim",
						Value:    "on",
						Source:   "-",
					},
				},
			},
		},
		{
			name: "properties with wrong name",
			args: args{
				name: "my-test-pool",
				properties: Properties{
					(zpoolprops.AutoTrim): {
						Name:     "my-test-pool",
						Property: "autotrim",
						Value:    "on",
						Source:   "-",
					},
					(zpoolprops.Free): {
						Name:     "my-other-pool",
						Property: "free",
						Value:    "8G",
						Source:   "-",
					},
				},
			},
			want: &Pool{
				Name: "my-test-pool",
				Properties: Properties{
					"autotrim": {
						Name:     "my-test-pool",
						Property: "autotrim",
						Value:    "on",
						Source:   "-",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := newPool(tt.args.name, tt.args.properties)

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestPool_Health(t *testing.T) {
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
					"health": {
						Name:     "my-test-pool",
						Property: "health",
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
					"health": {
						Name:     "my-test-pool",
						Property: "health",
						Value:    "-",
						Source:   "-",
					},
				},
			},
			want:   "",
			wantOk: false,
		},
		{
			name: "degraded health property",
			fields: fields{
				Properties: Properties{
					"health": {
						Name:     "my-test-pool",
						Property: "health",
						Value:    HealthDegraded,
						Source:   "-",
					},
				},
			},
			want:   "DEGRADED",
			wantOk: true,
		},
		{
			name: "faulted health property",
			fields: fields{
				Properties: Properties{
					"health": {
						Name:     "my-test-pool",
						Property: "health",
						Value:    HealthFaulted,
						Source:   "-",
					},
				},
			},
			want:   "FAULTED",
			wantOk: true,
		},
		{
			name: "offline health property",
			fields: fields{
				Properties: Properties{
					"health": {
						Name:     "my-test-pool",
						Property: "health",
						Value:    HealthOffline,
						Source:   "-",
					},
				},
			},
			want:   "OFFLINE",
			wantOk: true,
		},
		{
			name: "online health property",
			fields: fields{
				Properties: Properties{
					"health": {
						Name:     "my-test-pool",
						Property: "health",
						Value:    HealthOnline,
						Source:   "-",
					},
				},
			},
			want:   "ONLINE",
			wantOk: true,
		},
		{
			name: "removed health property",
			fields: fields{
				Properties: Properties{
					"health": {
						Name:     "my-test-pool",
						Property: "health",
						Value:    HealthRemoved,
						Source:   "-",
					},
				},
			},
			want:   "REMOVED",
			wantOk: true,
		},
		{
			name: "unavail health property",
			fields: fields{
				Properties: Properties{
					"health": {
						Name:     "my-test-pool",
						Property: "health",
						Value:    HealthUnavailable,
						Source:   "-",
					},
				},
			},
			want:   "UNAVAIL",
			wantOk: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Pool{
				Properties: tt.fields.Properties,
			}

			got, gotOk := p.Health()

			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantOk, gotOk)
		})
	}
}

func TestPool_Bool(t *testing.T) {
	props := []struct {
		name     string
		property string
		lookup   func(*Pool) (bool, bool)
	}{
		{
			name:     "Bool",
			property: "readonly",
			lookup: func(d *Pool) (bool, bool) {
				return d.Bool("readonly")
			},
		},
		{
			name:     "ReadOnly",
			property: "readonly",
			lookup: func(d *Pool) (bool, bool) {
				return d.ReadOnly()
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
					d := &Pool{}

					if !tt.unset {
						d.Properties = Properties{
							(prop.property): {
								Name:     "tank/my-pool",
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

func TestPool_Bytes(t *testing.T) {
	props := []struct {
		name     string
		property string
		lookup   func(*Pool) (uint64, bool)
	}{
		{
			name:     "Bytes",
			property: "allocated",
			lookup: func(p *Pool) (uint64, bool) {
				return p.Bytes("allocated")
			},
		},
		{
			name:     "Allocated",
			property: "allocated",
			lookup: func(p *Pool) (uint64, bool) {
				return p.Allocated()
			},
		},
		{
			name:     "Free",
			property: "free",
			lookup: func(p *Pool) (uint64, bool) {
				return p.Free()
			},
		},
		{
			name:     "Freeing",
			property: "freeing",
			lookup: func(p *Pool) (uint64, bool) {
				return p.Freeing()
			},
		},
		{
			name:     "Leaked",
			property: "leaked",
			lookup: func(p *Pool) (uint64, bool) {
				return p.Leaked()
			},
		},
		{
			name:     "Size",
			property: "size",
			lookup: func(p *Pool) (uint64, bool) {
				return p.Size()
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
					p := &Pool{}

					if !tt.unset {
						p.Properties = Properties{
							(prop.property): {
								Name:     "tank/my-dataset",
								Property: prop.property,
								Value:    tt.value,
								Source:   "-",
							},
						}
					}

					got, gotOk := prop.lookup(p)

					assert.Equal(t, tt.want, got)
					assert.Equal(t, tt.wantOk, gotOk)
				})
			}
		})
	}
}

func TestPool_Percent(t *testing.T) {
	props := []struct {
		name     string
		property string
		lookup   func(*Pool) (uint64, bool)
	}{
		{
			name:     "Percent",
			property: "capacity",
			lookup: func(d *Pool) (uint64, bool) {
				return d.Percent("capacity")
			},
		},
		{
			name:     "Capacity",
			property: "capacity",
			lookup: func(d *Pool) (uint64, bool) {
				return d.Capacity()
			},
		},
		{
			name:     "Fragmentation",
			property: "fragmentation",
			lookup: func(d *Pool) (uint64, bool) {
				return d.Fragmentation()
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
			name:   "0%",
			value:  "0%",
			want:   0,
			wantOk: true,
		},
		{
			name:   "1%",
			value:  "1%",
			want:   1,
			wantOk: true,
		},
		{
			name:   "99%",
			value:  "99%",
			want:   99,
			wantOk: true,
		},
		{
			name:   "100%",
			value:  "100%",
			want:   100,
			wantOk: true,
		},
		{
			name:   "150%",
			value:  "150%",
			want:   150,
			wantOk: true,
		},
	}
	for _, prop := range props {
		t.Run(prop.name, func(t *testing.T) {
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					d := &Pool{}

					if !tt.unset {
						d.Properties = Properties{
							(prop.property): {
								Name:     "tank/my-pool",
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
