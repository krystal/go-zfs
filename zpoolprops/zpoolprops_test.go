package zpoolprops

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProperties(t *testing.T) {
	tests := []struct {
		prop string
		want string
	}{
		// The following are read-only properties.
		{prop: Allocated, want: "allocated"},
		{prop: Capacity, want: "capacity"},
		{prop: ExpandSize, want: "expandsize"},
		{prop: Fragmentation, want: "fragmentation"},
		{prop: Free, want: "free"},
		{prop: Freeing, want: "freeing"},
		{prop: Leaked, want: "leaked"},
		{prop: Health, want: "health"},
		{prop: GUID, want: "guid"},
		{prop: LoadGUID, want: "load_guid"},
		{prop: Size, want: "size"},

		// The following properties can be set at creation time and import time.
		{prop: AltRoot, want: "altroot"},

		// The following properties can be set only at import time.
		{prop: ReadOnly, want: "readonly"},

		// The following properties can be set at creation time and import time,
		// and later changed with the zpool set command.
		{prop: Ashift, want: "ashift"},
		{prop: AutoExpand, want: "autoexpand"},
		{prop: AutoReplace, want: "autoreplace"},
		{prop: AutoTrim, want: "autotrim"},
		{prop: Bootfs, want: "bootfs"},
		{prop: Cachefile, want: "cachefile"},
		{prop: Comment, want: "comment"},
		{prop: Compatibility, want: "compatibility"},
		{prop: DedupDitto, want: "dedupditto"},
		{prop: Delegation, want: "delegation"},
		{prop: FailMode, want: "failmode"},
		{prop: ListSnapshots, want: "listsnapshots"},
		{prop: MultiHost, want: "multihost"},
		{prop: Version, want: "version"},
	}
	for _, tt := range tests {
		t.Run(tt.prop, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.prop)
		})
	}
}

func TestFeature(t *testing.T) {
	type args struct {
		name string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "empty",
			args: args{name: ""},
			want: "feature@",
		},
		{
			name: "foo",
			args: args{name: "foo"},
			want: "feature@foo",
		},
		{
			name: "bar",
			args: args{name: "bar"},
			want: "feature@bar",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Feature(tt.args.name)

			assert.Equal(t, tt.want, got)
		})
	}
}
