package zfs

import (
	"context"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	mock_runner "github.com/krystal/go-runner/mock"
	"github.com/krystal/go-zfs/zpoolprops"
	"github.com/romdo/gomockctx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManager_GetPoolProperty(t *testing.T) {
	ioWriter := reflect.TypeOf((*io.Writer)(nil)).Elem()

	type args struct {
		name     string
		property string
	}
	tests := []struct {
		name           string
		args           args
		wantArgs       []string
		stdout         string
		stderr         string
		want           string
		commandErr     error
		wantErr        string
		wantErrTargets []error
	}{
		{
			name: "empty pool name",
			args: args{
				name:     "",
				property: "size",
			},
			wantErr: "zpool; invalid name",
			wantErrTargets: []error{
				Err,
				ErrInvalidName,
				ErrZpool,
			},
		},
		{
			name: "invalid pool name",
			args: args{
				name:     "my-pool/things",
				property: "size",
			},
			wantErr: "zpool; invalid name",
			wantErrTargets: []error{
				Err,
				ErrZpool,
				ErrInvalidName,
			},
		},
		{
			name: "empty property name",
			args: args{
				name:     "my-test-pool",
				property: "",
			},
			wantErr: "zpool; invalid property",
			wantErrTargets: []error{
				Err,
				ErrZpool,
				ErrInvalidProperty,
			},
		},
		{
			name: "empty value",
			args: args{
				name:     "my-test-pool",
				property: "size",
			},
			wantArgs: []string{
				"get", "-Hp", "-o", "value", "size", "my-test-pool",
			},
			want: "",
		},
		{
			name: "size",
			args: args{
				name:     "my-test-pool",
				property: "size",
			},
			wantArgs: []string{
				"get", "-Hp", "-o", "value", "size", "my-test-pool",
			},
			stdout: "352321536\n",
			want:   "352321536",
		},
		{
			name: "feature@async_destroy",
			args: args{
				name:     "my-test-pool",
				property: "feature@async_destroy",
			},
			wantArgs: []string{
				"get", "-Hp", "-o", "value", "feature@async_destroy",
				"my-test-pool",
			},
			stdout: "enabled\n",
			want:   "enabled",
		},
		{
			name: "command error",
			args: args{
				name:     "my-test-pool",
				property: "sizex",
			},
			wantArgs: []string{
				"get", "-Hp", "-o", "value", "sizex", "my-test-pool",
			},
			stderr: `bad property list: invalid property 'sizex'
usage:
	get [-Hp] [-o "all" | field[,...]] <"all" | property[,...]> <pool> ...
`,
			commandErr: errors.New("exit status 1"),
			wantErr: "zpool; exit status 1: " +
				"bad property list: invalid property 'sizex'",
			wantErrTargets: []error{Err, ErrZpool},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := gomockctx.New(context.Background())
			ctrl := gomock.NewController(t)
			r := mock_runner.NewMockRunner(ctrl)
			if len(tt.wantArgs) > 0 {
				r.EXPECT().RunContext(
					gomockctx.Eq(ctx),
					gomock.Nil(),
					gomock.AssignableToTypeOf(ioWriter),
					gomock.AssignableToTypeOf(ioWriter),
					"zpool",
					tt.wantArgs,
				).DoAndReturn(func(
					_ context.Context,
					_ io.Reader,
					stdout io.Writer,
					stderr io.Writer,
					_ string,
					_ ...string,
				) error {
					_, _ = stdout.Write([]byte(tt.stdout))
					_, _ = stderr.Write([]byte(tt.stderr))

					return tt.commandErr
				})
			}

			m := &Manager{Runner: r}

			got, err := m.GetPoolProperty(ctx, tt.args.name, tt.args.property)

			if tt.wantErr != "" {
				assert.EqualError(t, err, tt.wantErr)
				assert.Empty(t, got)
				for _, target := range tt.wantErrTargets {
					assert.ErrorIs(t, err, target)
				}

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestManager_SetPoolProperty(t *testing.T) {
	ioWriter := reflect.TypeOf((*io.Writer)(nil)).Elem()

	type args struct {
		name     string
		property string
		value    string
	}
	tests := []struct {
		name           string
		args           args
		wantArgs       []string
		stderr         string
		commandErr     error
		wantErr        string
		wantErrTargets []error
	}{
		{
			name: "empty pool name",
			args: args{
				name:     "",
				property: "quota",
				value:    "1G",
			},
			wantErr: "zpool; invalid name",
			wantErrTargets: []error{
				Err,
				ErrZpool,
				ErrInvalidName,
			},
		},
		{
			name: "invalid pool name",
			args: args{
				name:     "my-pool/things",
				property: "quota",
				value:    "1G",
			},
			wantErr: "zpool; invalid name",
			wantErrTargets: []error{
				Err,
				ErrZpool,
				ErrInvalidName,
			},
		},
		{
			name: "empty property name",
			args: args{
				name:     "my-test-pool",
				property: "",
				value:    "2G",
			},
			wantErr: "zpool; invalid property: empty property name",
			wantErrTargets: []error{
				Err,
				ErrZpool,
				ErrInvalidProperty,
			},
		},
		{
			name: "all",
			args: args{
				name:     "my-test-pool",
				property: "all",
				value:    "what",
			},
			wantErr: "zpool; invalid property: 'all' is not a valid property",
			wantErrTargets: []error{
				Err,
				ErrZpool,
				ErrInvalidProperty,
			},
		},
		{
			name: "quota",
			args: args{
				name:     "my-test-pool",
				property: "quota",
				value:    "10G",
			},
			wantArgs: []string{"set", "quota=10G", "my-test-pool"},
		},
		{
			name: "feature@async_destroy",
			args: args{
				name:     "my-test-pool",
				property: "feature@async_destroy",
				value:    "disabled",
			},
			wantArgs: []string{
				"set", "feature@async_destroy=disabled", "my-test-pool",
			},
		},
		{
			name: "command error",
			args: args{
				name:     "my-test-pool",
				property: "listsnapshots",
				value:    "whatnow",
			},
			wantArgs: []string{"set", "listsnapshots=whatnow", "my-test-pool"},
			//nolint:lll
			stderr: `cannot set property for 'zfs-local-test': 'listsnapshots' must be one of 'on | off'
usage:
	set <property=value> <pool>
`,
			commandErr: errors.New("exit status 1"),
			wantErr: "zpool; exit status 1: cannot set property for " +
				"'zfs-local-test': 'listsnapshots' must be one of 'on | off'",
			wantErrTargets: []error{Err, ErrZpool},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := gomockctx.New(context.Background())
			ctrl := gomock.NewController(t)
			r := mock_runner.NewMockRunner(ctrl)
			if len(tt.wantArgs) > 0 {
				r.EXPECT().RunContext(
					gomockctx.Eq(ctx),
					gomock.Nil(),
					gomock.AssignableToTypeOf(ioWriter),
					gomock.AssignableToTypeOf(ioWriter),
					"zpool",
					tt.wantArgs,
				).DoAndReturn(func(
					_ context.Context,
					_ io.Reader,
					_ io.Writer,
					stderr io.Writer,
					_ string,
					_ ...string,
				) error {
					_, _ = stderr.Write([]byte(tt.stderr))

					return tt.commandErr
				})
			}

			m := &Manager{Runner: r}

			err := m.SetPoolProperty(
				ctx,
				tt.args.name,
				tt.args.property,
				tt.args.value,
			)

			if tt.wantErr != "" {
				assert.EqualError(t, err, tt.wantErr)
				for _, target := range tt.wantErrTargets {
					assert.ErrorIs(t, err, target)
				}

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestManager_SetPoolProperties(t *testing.T) {
	ioWriter := reflect.TypeOf((*io.Writer)(nil)).Elem()

	type args struct {
		name       string
		properties map[string]string
	}
	tests := []struct {
		name           string
		args           args
		wantArgs       []string
		stderr         string
		commandErr     error
		wantErr        string
		wantErrTargets []error
	}{
		{
			name: "empty dataset name",
			args: args{
				name: "",
				properties: map[string]string{
					"quota": "10G",
				},
			},
			wantErr: "zpool; invalid name",
			wantErrTargets: []error{
				Err,
				ErrZpool,
				ErrInvalidName,
			},
		},
		{
			name: "invalid pool name",
			args: args{
				name: "my-pool/things",
				properties: map[string]string{
					"quota": "10G",
				},
			},
			wantErr: "zpool; invalid name",
			wantErrTargets: []error{
				Err,
				ErrZpool,
				ErrInvalidName,
			},
		},
		{
			name: "empty property name",
			args: args{
				name: "my-test-pool",
				properties: map[string]string{
					"":      "what",
					"quota": "10G",
				},
			},
			wantErr: "zpool; invalid property: empty property name",
			wantErrTargets: []error{
				Err,
				ErrZpool,
				ErrInvalidProperty,
			},
		},
		{
			name: "all",
			args: args{
				name: "my-test-pool",
				properties: map[string]string{
					"all":   "what",
					"quota": "10G",
				},
			},
			wantErr: "zpool; invalid property: 'all' is not a valid property",
			wantErrTargets: []error{
				Err,
				ErrZpool,
				ErrInvalidProperty,
			},
		},
		{
			name: "single property",
			args: args{
				name: "my-test-pool",
				properties: map[string]string{
					"quota": "10G",
				},
			},
			wantArgs: []string{"set", "quota=10G", "my-test-pool"},
		},
		{
			name: "multiple properties",
			args: args{
				name: "my-test-pool",
				properties: map[string]string{
					"quota":                 "10G",
					"feature@async_destroy": "disabled",
				},
			},
			wantArgs: []string{
				"set", "feature@async_destroy=disabled", "quota=10G",
				"my-test-pool",
			},
		},
		{
			name: "command error",
			args: args{
				name: "my-test-pool",
				properties: map[string]string{
					"listsnapshots": "whatnow",
				},
			},
			wantArgs: []string{"set", "listsnapshots=whatnow", "my-test-pool"},
			//nolint:lll
			stderr: `cannot set property for 'zfs-local-test': 'listsnapshots' must be one of 'on | off'
usage:
	set <property=value> <pool>
`,
			commandErr: errors.New("exit status 1"),
			wantErr: "zpool; exit status 1: cannot set property for " +
				"'zfs-local-test': 'listsnapshots' must be one of 'on | off'",
			wantErrTargets: []error{Err, ErrZpool},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := gomockctx.New(context.Background())
			ctrl := gomock.NewController(t)
			r := mock_runner.NewMockRunner(ctrl)
			if len(tt.wantArgs) > 0 {
				r.EXPECT().RunContext(
					gomockctx.Eq(ctx),
					gomock.Nil(),
					gomock.AssignableToTypeOf(ioWriter),
					gomock.AssignableToTypeOf(ioWriter),
					"zpool",
					tt.wantArgs,
				).DoAndReturn(func(
					_ context.Context,
					_ io.Reader,
					_ io.Writer,
					stderr io.Writer,
					_ string,
					_ ...string,
				) error {
					_, _ = stderr.Write([]byte(tt.stderr))

					return tt.commandErr
				})
			}

			m := &Manager{Runner: r}

			err := m.SetPoolProperties(ctx, tt.args.name, tt.args.properties)

			if tt.wantErr != "" {
				assert.EqualError(t, err, tt.wantErr)
				for _, target := range tt.wantErrTargets {
					assert.ErrorIs(t, err, target)
				}

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestManager_CreatePool(t *testing.T) {
	ioWriter := reflect.TypeOf((*io.Writer)(nil)).Elem()

	type args struct {
		options *CreatePoolOptions
	}
	tests := []struct {
		name           string
		args           args
		wantArgs       []string
		stderr         string
		commandErr     error
		wantErr        string
		wantErrTargets []error
	}{
		{
			name:    "nil options",
			args:    args{},
			wantErr: "zpool; invalid create options",
			wantErrTargets: []error{
				Err,
				ErrInvalidCreateOptions,
				ErrZpool,
			},
		},
		{
			name: "empty options",
			args: args{
				options: &CreatePoolOptions{},
			},
			wantErr: "zpool; invalid create options; invalid name",
			wantErrTargets: []error{
				Err,
				ErrZpool,
				ErrInvalidName,
				ErrInvalidCreateOptions,
			},
		},
		{
			name: "empty pool name",
			args: args{
				options: &CreatePoolOptions{
					Name:  "",
					Vdevs: []string{"/dev/test-a", "/dev/test-b"},
				},
			},
			wantErr: "zpool; invalid create options; invalid name",
			wantErrTargets: []error{
				Err,
				ErrZpool,
				ErrInvalidName,
			},
		},
		{
			name: "invalid pool name",
			args: args{
				options: &CreatePoolOptions{
					Name:  "my-pool/things",
					Vdevs: []string{"/dev/test-a", "/dev/test-b"},
				},
			},
			wantErr: "zpool; invalid create options; invalid name",
			wantErrTargets: []error{
				Err,
				ErrZpool,
				ErrInvalidName,
			},
		},
		{
			name: "no vdevs",
			args: args{
				options: &CreatePoolOptions{
					Name: "my-test-pool",
				},
			},
			wantErr: "zpool; invalid create options: no vdevs specified",
			wantErrTargets: []error{
				Err,
				ErrZpool,
				ErrInvalidCreateOptions,
			},
		},
		{
			name: "simple",
			args: args{
				options: &CreatePoolOptions{
					Name:  "my-test-pool",
					Vdevs: []string{"/dev/test-a", "/dev/test-b"},
				},
			},
			wantArgs: []string{
				"create", "my-test-pool", "/dev/test-a", "/dev/test-b",
			},
		},
		{
			name: "mirror",
			args: args{
				options: &CreatePoolOptions{
					Name:  "my-test-pool",
					Vdevs: []string{"mirror", "/dev/mirr-a", "/dev/mirr-b"},
				},
			},
			wantArgs: []string{
				"create", "my-test-pool",
				"mirror", "/dev/mirr-a", "/dev/mirr-b",
			},
		},
		{
			name: "mountpoint",
			args: args{
				options: &CreatePoolOptions{
					Name:       "my-test-pool",
					Mountpoint: "/mnt/test",
					Vdevs:      []string{"/dev/test-a", "/dev/test-b"},
				},
			},
			wantArgs: []string{
				"create", "-m", "/mnt/test", "my-test-pool",
				"/dev/test-a", "/dev/test-b",
			},
		},
		{
			name: "mountpoint and root",
			args: args{
				options: &CreatePoolOptions{
					Name:       "my-test-pool",
					Mountpoint: "/data",
					Root:       "/mnt/zfs-inspect",
					Vdevs:      []string{"/dev/test-a", "/dev/test-b"},
				},
			},
			wantArgs: []string{
				"create", "-m", "/data", "-R", "/mnt/zfs-inspect",
				"my-test-pool", "/dev/test-a", "/dev/test-b",
			},
		},
		{
			name: "force",
			args: args{
				options: &CreatePoolOptions{
					Name:  "my-test-pool",
					Force: true,
					Vdevs: []string{"/dev/test-a", "/dev/test-b"},
				},
			},
			wantArgs: []string{
				"create", "-f", "my-test-pool", "/dev/test-a", "/dev/test-b",
			},
		},
		{
			name: "disable features",
			args: args{
				options: &CreatePoolOptions{
					Name:            "my-test-pool",
					DisableFeatures: true,
					Vdevs:           []string{"/dev/test-a", "/dev/test-b"},
				},
			},
			wantArgs: []string{
				"create", "-d", "my-test-pool", "/dev/test-a", "/dev/test-b",
			},
		},
		{
			name: "pool properties",
			args: args{
				options: &CreatePoolOptions{
					Name: "my-test-pool",
					Properties: map[string]string{
						(zpoolprops.AutoTrim): "off",
						(zpoolprops.Ashift):   "12",
					},
					Vdevs: []string{"/dev/test-a", "/dev/test-b"},
				},
			},
			wantArgs: []string{
				"create", "-o", "ashift=12", "-o", "autotrim=off",
				"my-test-pool", "/dev/test-a", "/dev/test-b",
			},
		},
		{
			name: "filesystem properties",
			args: args{
				options: &CreatePoolOptions{
					Name: "my-test-pool",
					FilesystemProperties: map[string]string{
						"canmount":    "off",
						"compression": "lz4",
					},
					Vdevs: []string{"/dev/test-a", "/dev/test-b"},
				},
			},
			wantArgs: []string{
				"create", "-O", "canmount=off", "-O", "compression=lz4",
				"my-test-pool", "/dev/test-a", "/dev/test-b",
			},
		},
		{
			name: "custom args",
			args: args{
				options: &CreatePoolOptions{
					Name:  "my-test-pool",
					Args:  []string{"-t", "other-name", "-n"},
					Vdevs: []string{"/dev/test-a", "/dev/test-b"},
				},
			},
			wantArgs: []string{
				"create", "-t", "other-name", "-n",
				"my-test-pool", "/dev/test-a", "/dev/test-b",
			},
		},
		{
			name: "all options",
			args: args{
				options: &CreatePoolOptions{
					Name: "my-test-pool",
					Properties: map[string]string{
						(zpoolprops.AutoTrim): "off",
						(zpoolprops.Ashift):   "12",
					},
					FilesystemProperties: map[string]string{
						"canmount":    "off",
						"compression": "lz4",
					},
					Mountpoint:      "/data",
					Root:            "/mnt/zfs-inspect",
					Force:           true,
					DisableFeatures: true,
					Args:            []string{"-t", "other-name", "-n"},
					Vdevs:           []string{"/dev/test-a", "/dev/test-b"},
				},
			},
			wantArgs: []string{
				"create",
				"-m", "/data",
				"-R", "/mnt/zfs-inspect",
				"-f",
				"-d",
				"-o", "ashift=12", "-o", "autotrim=off",
				"-O", "canmount=off", "-O", "compression=lz4",
				"-t", "other-name", "-n",
				"my-test-pool",
				"/dev/test-a", "/dev/test-b",
			},
		},
		{
			name: "command error",
			args: args{
				options: &CreatePoolOptions{
					Name:  "my-test-pool",
					Vdevs: []string{"nope", "bye"},
				},
			},
			wantArgs: []string{"create", "my-test-pool", "nope", "bye"},
			stderr: `cannot open 'nope': no such device in /dev
must be a full path or shorthand device name
usage:
	create [-fnd] [-o property=value] ...
`,
			commandErr: errors.New("exit status 1"),
			wantErr: "zpool; exit status 1: cannot open 'nope': " +
				"no such device in /dev: " +
				"must be a full path or shorthand device name",
			wantErrTargets: []error{Err, ErrZpool},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := gomockctx.New(context.Background())
			ctrl := gomock.NewController(t)
			r := mock_runner.NewMockRunner(ctrl)
			if len(tt.wantArgs) > 0 {
				r.EXPECT().RunContext(
					gomockctx.Eq(ctx),
					gomock.Nil(),
					gomock.AssignableToTypeOf(ioWriter),
					gomock.AssignableToTypeOf(ioWriter),
					"zpool",
					tt.wantArgs,
				).DoAndReturn(func(
					_ context.Context,
					_ io.Reader,
					_ io.Writer,
					stderr io.Writer,
					_ string,
					_ ...string,
				) error {
					_, _ = stderr.Write([]byte(tt.stderr))

					return tt.commandErr
				})
			}

			m := &Manager{Runner: r}

			err := m.CreatePool(ctx, tt.args.options)

			if tt.wantErr != "" {
				assert.EqualError(t, err, tt.wantErr)
				for _, target := range tt.wantErrTargets {
					assert.ErrorIs(t, err, target)
				}

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestManager_GetPool(t *testing.T) {
	ioWriter := reflect.TypeOf((*io.Writer)(nil)).Elem()

	type args struct {
		name       string
		properties []string
	}
	tests := []struct {
		name           string
		args           args
		wantArgs       []string
		stdout         string
		stderr         string
		want           *Pool
		commandErr     error
		wantErr        string
		wantErrTargets []error
	}{
		{
			name: "empty pool name",
			args: args{
				name: "",
			},
			wantErr: "zpool; invalid name",
			wantErrTargets: []error{
				Err,
				ErrZpool,
				ErrInvalidName,
			},
		},
		{
			name: "invalid pool name",
			args: args{
				name: "my-pool/things",
			},
			wantErr: "zpool; invalid name",
			wantErrTargets: []error{
				Err,
				ErrZpool,
				ErrInvalidName,
			},
		},
		{
			name: "no properties",
			args: args{
				name: "my-test-pool",
			},
			wantArgs: []string{
				"get", "-Hp", "-o", "name,property,value,source",
				"all", "my-test-pool",
			},
			stdout: "\n",
			want: &Pool{
				Name:       "my-test-pool",
				Properties: Properties{},
			},
		},
		{
			name: "many properties",
			args: args{
				name: "my-test-pool",
			},
			wantArgs: []string{
				"get", "-Hp", "-o", "name,property,value,source",
				"all", "my-test-pool",
			},
			stdout: `my-test-pool	size	352321536	-
my-test-pool	capacity	9	-
my-test-pool	health	ONLINE	-
my-test-pool	guid	3298971372827319759	-
my-test-pool	version	-	default
my-test-pool	autoreplace	off	default
my-test-pool	failmode	wait	default
my-test-pool	dedupratio	1.00x	-
my-test-pool	free	317718528	-
my-test-pool	allocated	34917580	-
my-test-pool	readonly	off	-
my-test-pool	ashift	0	default
my-test-pool	freeing	0	-
my-test-pool	fragmentation	27%	-
my-test-pool	multihost	off	default
my-test-pool	checkpoint	-	-
my-test-pool	load_guid	6932209575452951892	-
my-test-pool	autotrim	off	default
my-test-pool	feature@async_destroy	enabled	local
`,
			want: &Pool{
				Name: "my-test-pool",
				Properties: Properties{
					(zpoolprops.Size): {
						Name:     "my-test-pool",
						Property: "size",
						Value:    "352321536",
						Source:   "-",
					},
					(zpoolprops.Capacity): {
						Name:     "my-test-pool",
						Property: "capacity",
						Value:    "9",
						Source:   "-",
					},
					(zpoolprops.Health): {
						Name:     "my-test-pool",
						Property: "health",
						Value:    "ONLINE",
						Source:   "-",
					},
					(zpoolprops.GUID): {
						Name:     "my-test-pool",
						Property: "guid",
						Value:    "3298971372827319759",
						Source:   "-",
					},
					(zpoolprops.Version): {
						Name:     "my-test-pool",
						Property: "version",
						Value:    "-",
						Source:   "default",
					},
					(zpoolprops.AutoReplace): {
						Name:     "my-test-pool",
						Property: "autoreplace",
						Value:    "off",
						Source:   "default",
					},
					(zpoolprops.FailMode): {
						Name:     "my-test-pool",
						Property: "failmode",
						Value:    "wait",
						Source:   "default",
					},
					"dedupratio": {
						Name:     "my-test-pool",
						Property: "dedupratio",
						Value:    "1.00x",
						Source:   "-",
					},
					(zpoolprops.Free): {
						Name:     "my-test-pool",
						Property: "free",
						Value:    "317718528",
						Source:   "-",
					},
					(zpoolprops.Allocated): {
						Name:     "my-test-pool",
						Property: "allocated",
						Value:    "34917580",
						Source:   "-",
					},
					(zpoolprops.ReadOnly): {
						Name:     "my-test-pool",
						Property: "readonly",
						Value:    "off",
						Source:   "-",
					},
					(zpoolprops.Ashift): {
						Name:     "my-test-pool",
						Property: "ashift",
						Value:    "0",
						Source:   "default",
					},
					(zpoolprops.Freeing): {
						Name:     "my-test-pool",
						Property: "freeing",
						Value:    "0",
						Source:   "-",
					},
					(zpoolprops.Fragmentation): {
						Name:     "my-test-pool",
						Property: "fragmentation",
						Value:    "27%",
						Source:   "-",
					},
					(zpoolprops.MultiHost): {
						Name:     "my-test-pool",
						Property: "multihost",
						Value:    "off",
						Source:   "default",
					},
					"checkpoint": {
						Name:     "my-test-pool",
						Property: "checkpoint",
						Value:    "-",
						Source:   "-",
					},
					(zpoolprops.LoadGUID): {
						Name:     "my-test-pool",
						Property: "load_guid",
						Value:    "6932209575452951892",
						Source:   "-",
					},
					(zpoolprops.AutoTrim): {
						Name:     "my-test-pool",
						Property: "autotrim",
						Value:    "off",
						Source:   "default",
					},
					(zpoolprops.Feature("async_destroy")): {
						Name:     "my-test-pool",
						Property: "feature@async_destroy",
						Value:    "enabled",
						Source:   "local",
					},
				},
			},
		},
		{
			name: "custom properties",
			args: args{
				name:       "my-test-pool",
				properties: []string{zpoolprops.Size, zpoolprops.Health},
			},
			wantArgs: []string{
				"get", "-Hp", "-o", "name,property,value,source",
				"size,health", "my-test-pool",
			},
			stdout: `my-test-pool	size	352321536	-
my-test-pool	health	ONLINE	-
`,
			want: &Pool{
				Name: "my-test-pool",
				Properties: Properties{
					(zpoolprops.Size): {
						Name:     "my-test-pool",
						Property: "size",
						Value:    "352321536",
						Source:   "-",
					},
					(zpoolprops.Health): {
						Name:     "my-test-pool",
						Property: "health",
						Value:    "ONLINE",
						Source:   "-",
					},
				},
			},
		},
		{
			name: "command error",
			args: args{
				name: "my-other-pool",
			},
			wantArgs: []string{
				"get", "-Hp", "-o", "name,property,value,source",
				"all", "my-other-pool",
			},
			stderr: `cannot open 'my-other-pool': no such pool
usage:
	get [-Hp] [-o "all" | field[,...]] <"all" | property[,...]> <pool> ...
`,
			commandErr: errors.New("exit status 1"),
			wantErr: "zpool; exit status 1: cannot open " +
				"'my-other-pool': no such pool",
			wantErrTargets: []error{Err, ErrZpool},
		},
		{
			name: "output wrong pool name",
			args: args{
				name: "my-test-pool",
			},
			wantArgs: []string{
				"get", "-Hp", "-o", "name,property,value,source",
				"all", "my-test-pool",
			},
			stdout: "my-other-pool	size	352321536	-\n",
			want: &Pool{
				Name:       "my-test-pool",
				Properties: Properties{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := gomockctx.New(context.Background())
			ctrl := gomock.NewController(t)
			r := mock_runner.NewMockRunner(ctrl)
			if len(tt.wantArgs) > 0 {
				r.EXPECT().RunContext(
					gomockctx.Eq(ctx),
					gomock.Nil(),
					gomock.AssignableToTypeOf(ioWriter),
					gomock.AssignableToTypeOf(ioWriter),
					"zpool",
					tt.wantArgs,
				).DoAndReturn(func(
					_ context.Context,
					_ io.Reader,
					stdout io.Writer,
					stderr io.Writer,
					_ string,
					_ ...string,
				) error {
					_, _ = stdout.Write([]byte(tt.stdout))
					_, _ = stderr.Write([]byte(tt.stderr))

					return tt.commandErr
				})
			}

			m := &Manager{Runner: r}

			got, err := m.GetPool(ctx, tt.args.name, tt.args.properties...)
			if tt.wantErr != "" {
				assert.EqualError(t, err, tt.wantErr)
				assert.Empty(t, got)
				for _, target := range tt.wantErrTargets {
					assert.ErrorIs(t, err, target)
				}

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestManager_ListPools(t *testing.T) {
	ioWriter := reflect.TypeOf((*io.Writer)(nil)).Elem()

	type args struct {
		properties []string
	}
	tests := []struct {
		name           string
		args           args
		wantArgs       []string
		stdout         string
		stderr         string
		want           []*Pool
		commandErr     error
		wantErr        string
		wantErrTargets []error
	}{
		{
			name: "no results",
			wantArgs: []string{
				"get", "-Hp", "-o", "name,property,value,source", "all",
			},
			stdout: "\n",
			want:   []*Pool{},
		},
		{
			name: "many results",
			wantArgs: []string{
				"get", "-Hp", "-o", "name,property,value,source", "all",
			},
			stdout: `my-test-pool	size	352321536	-
my-test-pool	capacity	20	-
my-test-pool	health	ONLINE	-
my-test-pool	guid	3298971372827319759	-
my-other-pool	size	2147483648	-
my-other-pool	capacity	3	-
my-other-pool	health	DEGRADED	-
my-other-pool	guid	7323467451069414275	-
test-pool2	size	10737418240	-
test-pool2	capacity	2	-
test-pool2	health	ONLINE	-
test-pool2	guid	4199937496265218937	-
`,
			want: []*Pool{
				{
					Name: "my-test-pool",
					Properties: Properties{
						(zpoolprops.Size): {
							Name:     "my-test-pool",
							Property: "size",
							Value:    "352321536",
							Source:   "-",
						},
						(zpoolprops.Capacity): {
							Name:     "my-test-pool",
							Property: "capacity",
							Value:    "20",
							Source:   "-",
						},
						(zpoolprops.Health): {
							Name:     "my-test-pool",
							Property: "health",
							Value:    "ONLINE",
							Source:   "-",
						},
						(zpoolprops.GUID): {
							Name:     "my-test-pool",
							Property: "guid",
							Value:    "3298971372827319759",
							Source:   "-",
						},
					},
				},
				{
					Name: "my-other-pool",
					Properties: Properties{
						(zpoolprops.Size): {
							Name:     "my-other-pool",
							Property: "size",
							Value:    "2147483648",
							Source:   "-",
						},
						(zpoolprops.Capacity): {
							Name:     "my-other-pool",
							Property: "capacity",
							Value:    "3",
							Source:   "-",
						},
						(zpoolprops.Health): {
							Name:     "my-other-pool",
							Property: "health",
							Value:    "DEGRADED",
							Source:   "-",
						},
						(zpoolprops.GUID): {
							Name:     "my-other-pool",
							Property: "guid",
							Value:    "7323467451069414275",
							Source:   "-",
						},
					},
				},
				{
					Name: "test-pool2",
					Properties: Properties{
						(zpoolprops.Size): {
							Name:     "test-pool2",
							Property: "size",
							Value:    "10737418240",
							Source:   "-",
						},
						(zpoolprops.Capacity): {
							Name:     "test-pool2",
							Property: "capacity",
							Value:    "2",
							Source:   "-",
						},
						(zpoolprops.Health): {
							Name:     "test-pool2",
							Property: "health",
							Value:    "ONLINE",
							Source:   "-",
						},
						(zpoolprops.GUID): {
							Name:     "test-pool2",
							Property: "guid",
							Value:    "4199937496265218937",
							Source:   "-",
						},
					},
				},
			},
		},
		{
			name: "custom properties",
			args: args{
				properties: []string{zpoolprops.Size, zpoolprops.Capacity},
			},
			wantArgs: []string{
				"get", "-Hp", "-o", "name,property,value,source",
				"size,capacity",
			},
			stdout: `my-test-pool	size	352321536	-
my-test-pool	capacity	20	-
my-other-pool	size	2147483648	-
my-other-pool	capacity	3	-
test-pool2	size	10737418240	-
test-pool2	capacity	2	-
`,
			want: []*Pool{
				{
					Name: "my-test-pool",
					Properties: Properties{
						(zpoolprops.Size): {
							Name:     "my-test-pool",
							Property: "size",
							Value:    "352321536",
							Source:   "-",
						},
						(zpoolprops.Capacity): {
							Name:     "my-test-pool",
							Property: "capacity",
							Value:    "20",
							Source:   "-",
						},
					},
				},
				{
					Name: "my-other-pool",
					Properties: Properties{
						(zpoolprops.Size): {
							Name:     "my-other-pool",
							Property: "size",
							Value:    "2147483648",
							Source:   "-",
						},
						(zpoolprops.Capacity): {
							Name:     "my-other-pool",
							Property: "capacity",
							Value:    "3",
							Source:   "-",
						},
					},
				},
				{
					Name: "test-pool2",
					Properties: Properties{
						(zpoolprops.Size): {
							Name:     "test-pool2",
							Property: "size",
							Value:    "10737418240",
							Source:   "-",
						},
						(zpoolprops.Capacity): {
							Name:     "test-pool2",
							Property: "capacity",
							Value:    "2",
							Source:   "-",
						},
					},
				},
			},
		},
		{
			name: "command error",
			args: args{
				properties: []string{"size", "foobar"},
			},
			wantArgs: []string{
				"get", "-Hp", "-o", "name,property,value,source", "size,foobar",
			},
			stderr: `bad property list: invalid property 'foobar'
usage:
	get [-Hp] [-o "all" | field[,...]] <"all" | property[,...]> <pool> ...
`,
			commandErr: errors.New("exit status 3"),
			wantErr: "zpool; exit status 3: " +
				"bad property list: invalid property 'foobar'",
			wantErrTargets: []error{Err, ErrZpool},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := gomockctx.New(context.Background())
			ctrl := gomock.NewController(t)
			r := mock_runner.NewMockRunner(ctrl)
			if len(tt.wantArgs) > 0 {
				r.EXPECT().RunContext(
					gomockctx.Eq(ctx),
					gomock.Nil(),
					gomock.AssignableToTypeOf(ioWriter),
					gomock.AssignableToTypeOf(ioWriter),
					"zpool",
					tt.wantArgs,
				).DoAndReturn(func(
					_ context.Context,
					_ io.Reader,
					stdout io.Writer,
					stderr io.Writer,
					_ string,
					_ ...string,
				) error {
					_, _ = stdout.Write([]byte(tt.stdout))
					_, _ = stderr.Write([]byte(tt.stderr))

					return tt.commandErr
				})
			}

			m := &Manager{Runner: r}

			got, err := m.ListPools(ctx, tt.args.properties...)
			if tt.wantErr != "" {
				assert.EqualError(t, err, tt.wantErr)
				assert.Empty(t, got)
				for _, target := range tt.wantErrTargets {
					assert.ErrorIs(t, err, target)
				}

				return
			}

			require.NoError(t, err)
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

func TestManager_ListPoolNames(t *testing.T) {
	ioWriter := reflect.TypeOf((*io.Writer)(nil)).Elem()

	tests := []struct {
		name           string
		wantArgs       []string
		stdout         string
		stderr         string
		want           []string
		commandErr     error
		wantErr        string
		wantErrTargets []error
	}{
		{
			name:     "no pools",
			wantArgs: []string{"list", "-Hp", "-o", "name"},
			stdout:   "\n",
			want:     []string{},
		},
		{
			name:     "one pool",
			wantArgs: []string{"list", "-Hp", "-o", "name"},
			stdout:   "my-test-pool\n",
			want:     []string{"my-test-pool"},
		},
		{
			name:     "many pools",
			wantArgs: []string{"list", "-Hp", "-o", "name"},
			stdout:   "my-test-pool\nmy-other-pool\nanother-pool",
			want:     []string{"my-test-pool", "my-other-pool", "another-pool"},
		},
		{
			name:           "command error",
			wantArgs:       []string{"list", "-Hp", "-o", "name"},
			stderr:         "no such command 'zpool'\n",
			commandErr:     errors.New("exit status 2"),
			wantErr:        "zpool; exit status 2: no such command 'zpool'",
			wantErrTargets: []error{Err, ErrZpool},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := gomockctx.New(context.Background())
			ctrl := gomock.NewController(t)
			r := mock_runner.NewMockRunner(ctrl)
			if len(tt.wantArgs) > 0 {
				r.EXPECT().RunContext(
					gomockctx.Eq(ctx),
					gomock.Nil(),
					gomock.AssignableToTypeOf(ioWriter),
					gomock.AssignableToTypeOf(ioWriter),
					"zpool",
					tt.wantArgs,
				).DoAndReturn(func(
					_ context.Context,
					_ io.Reader,
					stdout io.Writer,
					stderr io.Writer,
					_ string,
					_ ...string,
				) error {
					_, _ = stdout.Write([]byte(tt.stdout))
					_, _ = stderr.Write([]byte(tt.stderr))

					return tt.commandErr
				})
			}

			m := &Manager{Runner: r}

			got, err := m.ListPoolNames(ctx)
			if tt.wantErr != "" {
				assert.EqualError(t, err, tt.wantErr)
				assert.Empty(t, got)
				for _, target := range tt.wantErrTargets {
					assert.ErrorIs(t, err, target)
				}

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestManager_DestroyPool(t *testing.T) {
	ioWriter := reflect.TypeOf((*io.Writer)(nil)).Elem()

	type args struct {
		name  string
		force bool
	}
	tests := []struct {
		name           string
		args           args
		wantArgs       []string
		want           *Pool
		stderr         string
		commandErr     error
		wantErr        string
		wantErrTargets []error
	}{
		{
			name: "empty pool name",
			args: args{
				name: "",
			},
			wantErr:        "zpool; invalid name",
			wantErrTargets: []error{Err, ErrZpool, ErrInvalidName},
		},
		{
			name: "invalid pool name",
			args: args{
				name: "my-pool/things",
			},
			wantErr:        "zpool; invalid name",
			wantErrTargets: []error{Err, ErrZpool, ErrInvalidName},
		},
		{
			name: "existing pool",
			args: args{
				name: "my-test-pool",
			},
			wantArgs: []string{"destroy", "my-test-pool"},
		},
		{
			name: "force",
			args: args{
				name:  "my-test-pool",
				force: true,
			},
			wantArgs: []string{"destroy", "-f", "my-test-pool"},
		},
		{
			name: "command error",
			args: args{
				name: "my-other-pool",
			},
			wantArgs: []string{"destroy", "my-other-pool"},
			stderr: `cannot open 'my-other-pool': no such pool
usage:
	destroy [-f] <pool>
`,
			commandErr: errors.New("exit status 1"),
			wantErr: "zpool; exit status 1: cannot open " +
				"'my-other-pool': no such pool",
			wantErrTargets: []error{Err, ErrZpool},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := gomockctx.New(context.Background())
			ctrl := gomock.NewController(t)
			r := mock_runner.NewMockRunner(ctrl)
			if len(tt.wantArgs) > 0 {
				r.EXPECT().RunContext(
					gomockctx.Eq(ctx),
					gomock.Nil(),
					gomock.AssignableToTypeOf(ioWriter),
					gomock.AssignableToTypeOf(ioWriter),
					"zpool",
					tt.wantArgs,
				).DoAndReturn(func(
					_ context.Context,
					_ io.Reader,
					_ io.Writer,
					stderr io.Writer,
					_ string,
					_ ...string,
				) error {
					_, _ = stderr.Write([]byte(tt.stderr))

					return tt.commandErr
				})
			}

			m := &Manager{Runner: r}

			err := m.DestroyPool(ctx, tt.args.name, tt.args.force)
			if tt.wantErr != "" {
				assert.EqualError(t, err, tt.wantErr)
				for _, target := range tt.wantErrTargets {
					assert.ErrorIs(t, err, target)
				}

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestManager_ImportPool(t *testing.T) {
	ioWriter := reflect.TypeOf((*io.Writer)(nil)).Elem()

	type args struct {
		options *ImportPoolOptions
	}
	tests := []struct {
		name           string
		args           args
		wantArgs       []string
		stderr         string
		commandErr     error
		wantErr        string
		wantErrTargets []error
	}{
		{
			name:     "nil options",
			args:     args{},
			wantArgs: []string{"import"},
		},
		{
			name: "empty options",
			args: args{
				options: &ImportPoolOptions{},
			},
			wantArgs: []string{"import"},
		},
		{
			name: "empty pool name",
			args: args{
				options: &ImportPoolOptions{
					Name: "",
				},
			},
			wantArgs: []string{"import"},
		},
		{
			name: "invalid pool name",
			args: args{
				options: &ImportPoolOptions{
					Name: "my-pool/things",
				},
			},
			wantErr:        "zpool; invalid name",
			wantErrTargets: []error{Err, ErrZpool, ErrInvalidName},
		},
		{
			name: "name",
			args: args{
				options: &ImportPoolOptions{
					Name: "my-test-pool",
				},
			},
			wantArgs: []string{"import", "my-test-pool"},
		},
		{
			name: "pool properties",
			args: args{
				options: &ImportPoolOptions{
					Name: "my-test-pool",
					Properties: map[string]string{
						(zpoolprops.AutoTrim): "off",
						(zpoolprops.Ashift):   "12",
					},
				},
			},
			wantArgs: []string{
				"import",
				"-o", "ashift=12", "-o", "autotrim=off",
				"my-test-pool",
			},
		},
		{
			name: "force",
			args: args{
				options: &ImportPoolOptions{
					Name:  "my-test-pool",
					Force: true,
				},
			},
			wantArgs: []string{
				"import", "-f", "my-test-pool",
			},
		},
		{
			name: "custom args",
			args: args{
				options: &ImportPoolOptions{
					Force: true,
					Args:  []string{"-D", "-m"},
				},
			},
			wantArgs: []string{"import", "-f", "-D", "-m"},
		},
		{
			name: "dir or device",
			args: args{
				options: &ImportPoolOptions{
					Name: "my-test-pool",
					DirOrDevice: []string{
						"/dev/test-a",
						"/dev/test-b",
						"/mnt/devices",
					},
				},
			},
			wantArgs: []string{
				"import",
				"-d", "/dev/test-a",
				"-d", "/dev/test-b",
				"-d", "/mnt/devices",
				"my-test-pool",
			},
		},
		{
			name: "all options",
			args: args{
				options: &ImportPoolOptions{
					Name: "my-test-pool",
					Properties: map[string]string{
						(zpoolprops.AutoTrim): "off",
						(zpoolprops.Ashift):   "12",
					},
					Force: true,
					Args:  []string{"-D", "-m"},
					DirOrDevice: []string{
						"/dev/test-a",
						"/dev/test-b",
						"/mnt/devices",
					},
				},
			},
			wantArgs: []string{
				"import",
				"-f",
				"-o", "ashift=12",
				"-o", "autotrim=off",
				"-d", "/dev/test-a",
				"-d", "/dev/test-b",
				"-d", "/mnt/devices",
				"-D", "-m",
				"my-test-pool",
			},
		},
		{
			name: "command error",
			args: args{
				options: &ImportPoolOptions{Name: "nopefoo"},
			},
			wantArgs: []string{"import", "nopefoo"},
			stderr: `cannot import 'nopefoo': no such pool available
usage:
	import [-d dir] [-D]
	import [-o mntopts] [-o property=value] ...
`,
			commandErr: errors.New("exit status 42"),
			wantErr: "zpool; exit status 42: " +
				"cannot import 'nopefoo': no such pool available",
			wantErrTargets: []error{Err, ErrZpool},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := gomockctx.New(context.Background())
			ctrl := gomock.NewController(t)
			r := mock_runner.NewMockRunner(ctrl)
			if len(tt.wantArgs) > 0 {
				r.EXPECT().RunContext(
					gomockctx.Eq(ctx),
					gomock.Nil(),
					gomock.AssignableToTypeOf(ioWriter),
					gomock.AssignableToTypeOf(ioWriter),
					"zpool",
					tt.wantArgs,
				).DoAndReturn(func(
					_ context.Context,
					_ io.Reader,
					_ io.Writer,
					stderr io.Writer,
					_ string,
					_ ...string,
				) error {
					_, _ = stderr.Write([]byte(tt.stderr))

					return tt.commandErr
				})
			}

			m := &Manager{Runner: r}

			err := m.ImportPool(ctx, tt.args.options)

			if tt.wantErr != "" {
				assert.EqualError(t, err, tt.wantErr)
				for _, target := range tt.wantErrTargets {
					assert.ErrorIs(t, err, target)
				}

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestManager_ExportPool(t *testing.T) {
	ioWriter := reflect.TypeOf((*io.Writer)(nil)).Elem()

	type args struct {
		name  string
		force bool
	}
	tests := []struct {
		name           string
		args           args
		wantArgs       []string
		want           *Pool
		stderr         string
		commandErr     error
		wantErr        string
		wantErrTargets []error
	}{
		{
			name: "empty pool name",
			args: args{
				name: "",
			},
			wantErr:        "zpool; invalid name",
			wantErrTargets: []error{Err, ErrZpool, ErrInvalidName},
		},
		{
			name: "invalid pool name",
			args: args{
				name: "my-pool/things",
			},
			wantErr:        "zpool; invalid name",
			wantErrTargets: []error{Err, ErrZpool, ErrInvalidName},
		},
		{
			name: "existing pool",
			args: args{
				name: "my-test-pool",
			},
			wantArgs: []string{"export", "my-test-pool"},
		},
		{
			name: "force",
			args: args{
				name:  "my-test-pool",
				force: true,
			},
			wantArgs: []string{"export", "-f", "my-test-pool"},
		},
		{
			name: "command error",
			args: args{
				name: "my-other-pool",
			},
			wantArgs: []string{"export", "my-other-pool"},
			stderr: `cannot open 'my-other-pool': no such pool
usage:
	export [-af] <pool> ...
`,
			commandErr: errors.New("exit status 2"),
			wantErr: "zpool; exit status 2: cannot open " +
				"'my-other-pool': no such pool",
			wantErrTargets: []error{Err, ErrZpool},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := gomockctx.New(context.Background())
			ctrl := gomock.NewController(t)
			r := mock_runner.NewMockRunner(ctrl)
			if len(tt.wantArgs) > 0 {
				r.EXPECT().RunContext(
					gomockctx.Eq(ctx),
					gomock.Nil(),
					gomock.AssignableToTypeOf(ioWriter),
					gomock.AssignableToTypeOf(ioWriter),
					"zpool",
					tt.wantArgs,
				).DoAndReturn(func(
					_ context.Context,
					_ io.Reader,
					_ io.Writer,
					stderr io.Writer,
					_ string,
					_ ...string,
				) error {
					_, _ = stderr.Write([]byte(tt.stderr))

					return tt.commandErr
				})
			}

			m := &Manager{Runner: r}

			err := m.ExportPool(ctx, tt.args.name, tt.args.force)
			if tt.wantErr != "" {
				assert.EqualError(t, err, tt.wantErr)
				for _, target := range tt.wantErrTargets {
					assert.ErrorIs(t, err, target)
				}

				return
			}

			require.NoError(t, err)
		})
	}
}
