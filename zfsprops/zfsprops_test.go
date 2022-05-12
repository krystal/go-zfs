package zfsprops

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProperties(t *testing.T) {
	tests := []struct {
		prop string
		want string
	}{
		// The following native properties consist of read-only statistics about
		// the dataset. These properties can be neither set, nor inherited.
		{prop: Available, want: "available"},
		{prop: CompressRatio, want: "compressratio"},
		{prop: CreateTxGroup, want: "createtxg"},
		{prop: Creation, want: "creation"},
		{prop: Clones, want: "clones"},
		{prop: DeferDestroy, want: "defer_destroy"},
		{prop: EncryptionRoot, want: "encryptionroot"},
		{prop: FilesystemCount, want: "filesystem_count"},
		{prop: KeyStatus, want: "keystatus"},
		{prop: GUID, want: "guid"},
		{prop: LogicalReferenced, want: "logicalreferenced"},
		{prop: LogicalUsed, want: "logicalused"},
		{prop: Mounted, want: "mounted"},
		{prop: ObjsetID, want: "objsetid"},
		{prop: Origin, want: "origin"},
		{prop: ReceiveResumeToken, want: "receive_resume_token"},
		{prop: RedactSnaps, want: "redact_snaps"},
		{prop: Referenced, want: "referenced"},
		{prop: RefCompressRatio, want: "refcompressratio"},
		{prop: SnapshotCount, want: "snapshot_count"},
		{prop: Type, want: "type"},
		{prop: Used, want: "used"},
		{prop: UsedByChildren, want: "usedbychildren"},
		{prop: UsedByDataset, want: "usedbydataset"},
		{prop: UsedByRefReservation, want: "usedbyrefreservation"},
		{prop: UsedBySnapshots, want: "usedbysnapshots"},
		{prop: VolBlockSize, want: "volblocksize"},
		{prop: Written, want: "written"},

		// The following native properties can be used to change the behavior of
		// a ZFS dataset.
		{prop: ACLInherit, want: "aclinherit"},
		{prop: ACLMode, want: "aclmode"},
		{prop: ACLType, want: "acltype"},
		{prop: Atime, want: "atime"},
		{prop: CanMount, want: "canmount"},
		{prop: Checksum, want: "checksum"},
		{prop: Compression, want: "compression"},
		{prop: Context, want: "context"},
		{prop: FSContext, want: "fscontext"},
		{prop: DefContext, want: "defcontext"},
		{prop: RootContext, want: "rootcontext"},
		{prop: Copies, want: "copies"},
		{prop: Devices, want: "devices"},
		{prop: Dedup, want: "dedup"},
		{prop: DNodeSize, want: "dnodesize"},
		{prop: Encryption, want: "encryption"},
		{prop: KeyFormat, want: "keyformat"},
		{prop: KeyLocation, want: "keylocation"},
		{prop: PBKDF2Iterations, want: "pbkdf2iters"},
		{prop: Exec, want: "exec"},
		{prop: FilesystemLimit, want: "filesystem_limit"},
		{prop: SpecialSmallBlocks, want: "special_small_blocks"},
		{prop: Mountpoint, want: "mountpoint"},
		{prop: Nbmand, want: "nbmand"},
		{prop: Overlay, want: "overlay"},
		{prop: PrimaryCache, want: "primarycache"},
		{prop: Quota, want: "quota"},
		{prop: SnapshotLimit, want: "snapshot_limit"},
		{prop: ReadOnly, want: "readonly"},
		{prop: RecordSize, want: "recordsize"},
		{prop: RedundantMetadata, want: "redundant_metadata"},
		{prop: RefQuota, want: "refquota"},
		{prop: RefReservation, want: "refreservation"},
		{prop: RelAtime, want: "relatime"},
		{prop: Reservation, want: "reservation"},
		{prop: SecondaryCache, want: "secondarycache"},
		{prop: SetUID, want: "setuid"},
		{prop: ShareSMB, want: "sharesmb"},
		{prop: ShareNFS, want: "sharenfs"},
		{prop: LogBias, want: "logbias"},
		{prop: SnapDev, want: "snapdev"},
		{prop: SnapDir, want: "snapdir"},
		{prop: Sync, want: "sync"},
		{prop: Version, want: "version"},
		{prop: VolSize, want: "volsize"},
		{prop: VolMode, want: "volmode"},
		{prop: VScan, want: "vscan"},
		{prop: XAttr, want: "xattr"},
		{prop: Jailed, want: "jailed"},
		{prop: Zoned, want: "zoned"},

		// The following three properties cannot be changed after the file
		// system is created, and therefore, should be set when the file system
		// is created.
		{prop: CaseSensitivity, want: "casesensitivity"},
		{prop: Normalization, want: "normalization"},
		{prop: UTF8Only, want: "utf8only"},
	}
	for _, tt := range tests {
		t.Run(tt.prop, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.prop)
		})
	}
}

func TestUserQuota(t *testing.T) {
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
			want: "userquota@",
		},
		{
			name: "foo",
			args: args{name: "john"},
			want: "userquota@john",
		},
		{
			name: "bar",
			args: args{name: "jane"},
			want: "userquota@jane",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := UserQuota(tt.args.name)

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestUserObjQuota(t *testing.T) {
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
			want: "userobjquota@",
		},
		{
			name: "foo",
			args: args{name: "john"},
			want: "userobjquota@john",
		},
		{
			name: "bar",
			args: args{name: "jane"},
			want: "userobjquota@jane",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := UserObjQuota(tt.args.name)

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestGroupQuota(t *testing.T) {
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
			want: "groupquota@",
		},
		{
			name: "foo",
			args: args{name: "web"},
			want: "groupquota@web",
		},
		{
			name: "bar",
			args: args{name: "admin"},
			want: "groupquota@admin",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GroupQuota(tt.args.name)

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestGroupObjQuota(t *testing.T) {
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
			want: "groupobjquota@",
		},
		{
			name: "foo",
			args: args{name: "web"},
			want: "groupobjquota@web",
		},
		{
			name: "bar",
			args: args{name: "admin"},
			want: "groupobjquota@admin",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GroupObjQuota(tt.args.name)

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestProjectQuota(t *testing.T) {
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
			want: "projectquota@",
		},
		{
			name: "foo",
			args: args{name: "apollo"},
			want: "projectquota@apollo",
		},
		{
			name: "bar",
			args: args{name: "athena"},
			want: "projectquota@athena",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ProjectQuota(tt.args.name)

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestProjectObjQuota(t *testing.T) {
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
			want: "projectobjquota@",
		},
		{
			name: "foo",
			args: args{name: "apollo"},
			want: "projectobjquota@apollo",
		},
		{
			name: "bar",
			args: args{name: "athena"},
			want: "projectobjquota@athena",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ProjectObjQuota(tt.args.name)

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestUser(t *testing.T) {
	tests := []struct {
		name     string
		module   string
		property string
		want     string
	}{
		{
			name:     "empty module and property",
			module:   "",
			property: "",
			want:     ":",
		},
		{
			name:     "empty property",
			module:   "com.example.foobar",
			property: "",
			want:     "com.example.foobar:",
		},
		{
			name:     "empty module",
			module:   "",
			property: "account_owner_uuid",
			want:     ":account_owner_uuid",
		},
		{
			name:     "module and property",
			module:   "com.example.foobar",
			property: "account_owner_uuid",
			want:     "com.example.foobar:account_owner_uuid",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := User(tt.module)

			got1 := f(tt.property)
			got2 := f(tt.property)
			got3 := f("other_property")

			assert.Equal(t, tt.want, got1)
			assert.Equal(t, tt.want, got2)
			assert.Equal(t, tt.module+":other_property", got3)
		})
	}
}
