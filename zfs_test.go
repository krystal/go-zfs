package zfs

import (
	"context"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	mock_runner "github.com/krystal/go-runner/mock"
	"github.com/krystal/go-zfs/zfsprops"
	"github.com/romdo/gomockctx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManager_GetDatasetProperty(t *testing.T) {
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
			name: "empty dataset name",
			args: args{
				name:     "",
				property: "size",
			},
			wantErr: "zfs; invalid name",
			wantErrTargets: []error{
				Err,
				ErrZFS,
				ErrInvalidName,
			},
		},
		{
			name: "slash prefix name",
			args: args{
				name:     "/tank/my-dataset",
				property: "size",
			},
			wantErr: "zfs; invalid name",
			wantErrTargets: []error{
				Err,
				ErrZFS,
				ErrInvalidName,
			},
		},
		{
			name: "slash suffix name",
			args: args{
				name:     "tank/my-dataset/",
				property: "size",
			},
			wantErr: "zfs; invalid name",
			wantErrTargets: []error{
				Err,
				ErrZFS,
				ErrInvalidName,
			},
		},
		{
			name: "empty property name",
			args: args{
				name:     "tank/my-dataset",
				property: "",
			},
			wantErr: "zfs; invalid property",
			wantErrTargets: []error{
				Err,
				ErrZFS,
				ErrInvalidProperty,
			},
		},
		{
			name: "blank value",
			args: args{
				name:     "tank/my-dataset",
				property: "size",
			},
			wantArgs: []string{
				"get", "-Hp", "-o", "value", "size", "tank/my-dataset",
			},
			stdout: "-",
			want:   "-",
		},
		{
			name: "empty value",
			args: args{
				name:     "tank/my-dataset",
				property: "size",
			},
			wantArgs: []string{
				"get", "-Hp", "-o", "value", "size", "tank/my-dataset",
			},
			want: "",
		},
		{
			name: "size",
			args: args{
				name:     "tank/my-dataset",
				property: "size",
			},
			wantArgs: []string{
				"get", "-Hp", "-o", "value", "size", "tank/my-dataset",
			},
			stdout: "336M\n",
			want:   "336M",
		},
		{
			name: "feature@async_destroy",
			args: args{
				name:     "tank/my-dataset",
				property: "feature@async_destroy",
			},
			wantArgs: []string{
				"get", "-Hp", "-o", "value", "feature@async_destroy",
				"tank/my-dataset",
			},
			stdout: "enabled\n",
			want:   "enabled",
		},
		{
			name: "dataset does not exist",
			args: args{
				name:     "tank/my-other-dataset",
				property: "size",
			},
			wantArgs: []string{
				"get", "-Hp", "-o", "value", "size", "tank/my-other-dataset",
			},
			stderr: "cannot open 'tank/my-other-dataset': " +
				"dataset does not exist\n",
			commandErr: errors.New("exit status 1"),
			wantErr: "zfs; not found; exit status 1: cannot open " +
				"'tank/my-other-dataset': dataset does not exist",
			wantErrTargets: []error{Err, ErrZFS, ErrNotFound},
		},
		{
			name: "command error",
			args: args{
				name:     "tank/my-dataset",
				property: "sizex",
			},
			wantArgs: []string{
				"get", "-Hp", "-o", "value", "sizex", "tank/my-dataset",
			},
			stderr: `bad property list: invalid property 'sizex'
usage:
	get [-rHp] [-d max] [-o "all" | field[,...]]
`,
			commandErr: errors.New("exit status 1"),
			wantErr: "zfs; exit status 1: " +
				"bad property list: invalid property 'sizex'",
			wantErrTargets: []error{Err, ErrZFS},
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
					"zfs",
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

			got, err := m.GetDatasetProperty(
				ctx,
				tt.args.name,
				tt.args.property,
			)
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

func TestManager_SetDatasetProperty(t *testing.T) {
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
			name: "empty dataset name",
			args: args{
				name:     "",
				property: "quota",
				value:    "1G",
			},
			wantErr: "zfs; invalid name",
			wantErrTargets: []error{
				Err,
				ErrZFS,
				ErrInvalidName,
			},
		},
		{
			name: "slash prefix name",
			args: args{
				name:     "/tank/my-dataset",
				property: "size",
			},
			wantErr: "zfs; invalid name",
			wantErrTargets: []error{
				Err,
				ErrZFS,
				ErrInvalidName,
			},
		},
		{
			name: "slash suffix name",
			args: args{
				name:     "tank/my-dataset/",
				property: "size",
			},
			wantErr: "zfs; invalid name",
			wantErrTargets: []error{
				Err,
				ErrZFS,
				ErrInvalidName,
			},
		},
		{
			name: "empty property name",
			args: args{
				name:     "tank/my-dataset",
				property: "",
				value:    "2G",
			},
			wantErr: "zfs; invalid property: empty property name",
			wantErrTargets: []error{
				Err,
				ErrZFS,
				ErrInvalidProperty,
			},
		},
		{
			name: "all",
			args: args{
				name:     "tank/my-dataset",
				property: "all",
				value:    "what",
			},
			wantErr: "zfs; invalid property: 'all' is not a valid property",
			wantErrTargets: []error{
				Err,
				ErrZFS,
				ErrInvalidProperty,
			},
		},
		{
			name: "quota",
			args: args{
				name:     "tank/my-dataset",
				property: "quota",
				value:    "10G",
			},
			wantArgs: []string{"set", "quota=10G", "tank/my-dataset"},
		},
		{
			name: "feature@async_destroy",
			args: args{
				name:     "tank/my-dataset",
				property: "feature@async_destroy",
				value:    "disabled",
			},
			wantArgs: []string{
				"set", "feature@async_destroy=disabled", "tank/my-dataset",
			},
		},
		{
			name: "dataset does not exist",
			args: args{
				name:     "tank/my-other-dataset",
				property: "quota",
				value:    "10G",
			},
			wantArgs: []string{
				"set", "quota=10G", "tank/my-other-dataset",
			},
			stderr: "cannot open 'tank/my-other-dataset': " +
				"dataset does not exist\n",
			commandErr: errors.New("exit status 1"),
			wantErr: "zfs; not found; exit status 1: cannot open " +
				"'tank/my-other-dataset': dataset does not exist",
			wantErrTargets: []error{Err, ErrZFS, ErrNotFound},
		},
		{
			name: "command error",
			args: args{
				name:     "tank/my-dataset",
				property: "sync",
				value:    "dontdoit",
			},
			wantArgs: []string{
				"set", "sync=dontdoit", "tank/my-dataset",
			},
			//nolint:lll
			stderr: `cannot set property for 'tank/my-dataset': 'sync' must be one of 'standard | always | disabled'
usage:
	set <property=value> ... <filesystem|volume|snapshot> ...
`,
			commandErr: errors.New("exit status 1"),
			wantErr: "zfs; exit status 1: " +
				"cannot set property for 'tank/my-dataset': " +
				"'sync' must be one of 'standard | always | disabled'",
			wantErrTargets: []error{Err, ErrZFS},
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
					"zfs",
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

			err := m.SetDatasetProperty(
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

func TestManager_SetDatasetProperties(t *testing.T) {
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
			wantErr: "zfs; invalid name",
			wantErrTargets: []error{
				Err,
				ErrZFS,
				ErrInvalidName,
			},
		},
		{
			name: "slash prefix name",
			args: args{
				name: "/tank/my-dataset",
				properties: map[string]string{
					"quota": "10G",
				},
			},
			wantErr: "zfs; invalid name",
			wantErrTargets: []error{
				Err,
				ErrZFS,
				ErrInvalidName,
			},
		},
		{
			name: "slash suffix name",
			args: args{
				name: "tank/my-dataset/",
				properties: map[string]string{
					"quota": "10G",
				},
			},
			wantErr: "zfs; invalid name",
			wantErrTargets: []error{
				Err,
				ErrZFS,
				ErrInvalidName,
			},
		},
		{
			name: "empty property name",
			args: args{
				name: "tank/my-dataset",
				properties: map[string]string{
					"":      "what",
					"quota": "10G",
				},
			},
			wantErr: "zfs; invalid property: empty property name",
			wantErrTargets: []error{
				Err,
				ErrZFS,
				ErrInvalidProperty,
			},
		},
		{
			name: "all",
			args: args{
				name: "tank/my-dataset",
				properties: map[string]string{
					"all":   "what",
					"quota": "10G",
				},
			},
			wantErr: "zfs; invalid property: 'all' is not a valid property",
			wantErrTargets: []error{
				Err,
				ErrZFS,
				ErrInvalidProperty,
			},
		},
		{
			name: "single property",
			args: args{
				name: "tank/my-dataset",
				properties: map[string]string{
					"quota": "10G",
				},
			},
			wantArgs: []string{"set", "quota=10G", "tank/my-dataset"},
		},
		{
			name: "multiple properties",
			args: args{
				name: "tank/my-dataset",
				properties: map[string]string{
					"quota":                 "10G",
					"feature@async_destroy": "disabled",
				},
			},
			wantArgs: []string{
				"set", "feature@async_destroy=disabled", "quota=10G",
				"tank/my-dataset",
			},
		},
		{
			name: "dataset does not exist",
			args: args{
				name: "tank/my-other-dataset",
				properties: map[string]string{
					"quota": "10G",
				},
			},
			wantArgs: []string{
				"set", "quota=10G", "tank/my-other-dataset",
			},
			stderr: "cannot open 'tank/my-other-dataset': " +
				"dataset does not exist\n",
			commandErr: errors.New("exit status 1"),
			wantErr: "zfs; not found; exit status 1: cannot open " +
				"'tank/my-other-dataset': dataset does not exist",
			wantErrTargets: []error{Err, ErrZFS, ErrNotFound},
		},
		{
			name: "command error",
			args: args{
				name: "tank/my-dataset",
				properties: map[string]string{
					"sync": "dontdoit",
				},
			},
			wantArgs: []string{
				"set", "sync=dontdoit", "tank/my-dataset",
			},
			//nolint:lll
			stderr: `cannot set property for 'tank/my-dataset': 'sync' must be one of 'standard | always | disabled'
usage:
	set <property=value> ... <filesystem|volume|snapshot> ...
`,
			commandErr: errors.New("exit status 1"),
			wantErr: "zfs; exit status 1: " +
				"cannot set property for 'tank/my-dataset': " +
				"'sync' must be one of 'standard | always | disabled'",
			wantErrTargets: []error{Err, ErrZFS},
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
					"zfs",
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

			err := m.SetDatasetProperties(ctx, tt.args.name, tt.args.properties)

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

func TestManager_InheritDatasetProperty(t *testing.T) {
	ioWriter := reflect.TypeOf((*io.Writer)(nil)).Elem()

	type args struct {
		name      string
		property  string
		recursive bool
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
				name:      "",
				property:  "quota",
				recursive: false,
			},
			wantErr: "zfs; invalid name",
			wantErrTargets: []error{
				Err,
				ErrZFS,
				ErrInvalidName,
			},
		},
		{
			name: "slash prefix name",
			args: args{
				name:      "/tank/my-dataset",
				property:  "quota",
				recursive: false,
			},
			wantErr: "zfs; invalid name",
			wantErrTargets: []error{
				Err,
				ErrZFS,
				ErrInvalidName,
			},
		},
		{
			name: "slash suffix name",
			args: args{
				name:      "tank/my-dataset/",
				property:  "quota",
				recursive: false,
			},
			wantErr: "zfs; invalid name",
			wantErrTargets: []error{
				Err,
				ErrZFS,
				ErrInvalidName,
			},
		},
		{
			name: "empty property name",
			args: args{
				name:      "tank/my-dataset",
				property:  "",
				recursive: false,
			},
			wantErr: "zfs; invalid property",
			wantErrTargets: []error{
				Err,
				ErrZFS,
				ErrInvalidProperty,
			},
		},
		{
			name: "non-recursive",
			args: args{
				name:      "tank/my-dataset",
				property:  "quota",
				recursive: false,
			},
			wantArgs: []string{"inherit", "quota", "tank/my-dataset"},
		},
		{
			name: "non-recursive",
			args: args{
				name:      "tank/my-dataset",
				property:  "quota",
				recursive: true,
			},
			wantArgs: []string{"inherit", "-r", "quota", "tank/my-dataset"},
		},
		{
			name: "feature@async_destroy",
			args: args{
				name:      "tank/my-dataset",
				property:  "feature@async_destroy",
				recursive: false,
			},
			wantArgs: []string{
				"inherit", "feature@async_destroy", "tank/my-dataset",
			},
		},
		{
			name: "dataset does not exist",
			args: args{
				name:      "tank/my-other-dataset",
				property:  "quota",
				recursive: false,
			},
			wantArgs: []string{
				"inherit", "quota", "tank/my-other-dataset",
			},
			stderr: "cannot open 'tank/my-other-dataset': " +
				"dataset does not exist\n",
			commandErr: errors.New("exit status 1"),
			wantErr: "zfs; not found; exit status 1: cannot open " +
				"'tank/my-other-dataset': dataset does not exist",
			wantErrTargets: []error{Err, ErrZFS, ErrNotFound},
		},
		{
			name: "command error",
			args: args{
				name:      "tank/my-dataset",
				property:  "foosync",
				recursive: false,
			},
			wantArgs: []string{
				"inherit", "foosync", "tank/my-dataset",
			},
			stderr: `invalid property 'foosync'
usage:
	inherit [-rS] <property> <filesystem|volume|snapshot> ...
`,
			commandErr:     errors.New("exit status 1"),
			wantErr:        "zfs; exit status 1: invalid property 'foosync'",
			wantErrTargets: []error{Err, ErrZFS},
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
					"zfs",
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

			err := m.InheritDatasetProperty(
				ctx,
				tt.args.name,
				tt.args.property,
				tt.args.recursive,
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

func TestManager_CreateDataset(t *testing.T) {
	ioWriter := reflect.TypeOf((*io.Writer)(nil)).Elem()

	type args struct {
		options *CreateDatasetOptions
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
			wantErr: "zfs; invalid create options",
			wantErrTargets: []error{
				Err,
				ErrZFS,
				ErrInvalidCreateOptions,
			},
		},
		{
			name: "empty options",
			args: args{
				options: &CreateDatasetOptions{},
			},
			wantErr: "zfs; invalid create options; invalid name",
			wantErrTargets: []error{
				Err,
				ErrZFS,
				ErrInvalidName,
				ErrInvalidCreateOptions,
			},
		},
		{
			name: "empty dataset name",
			args: args{
				options: &CreateDatasetOptions{
					Name: "",
				},
			},
			wantErr: "zfs; invalid create options; invalid name",
			wantErrTargets: []error{
				Err,
				ErrZFS,
				ErrInvalidName,
				ErrInvalidCreateOptions,
			},
		},
		{
			name: "slash prefix name",
			args: args{
				options: &CreateDatasetOptions{
					Name: "/tank/my-dataset",
				},
			},
			wantErr: "zfs; invalid create options; invalid name",
			wantErrTargets: []error{
				Err,
				ErrZFS,
				ErrInvalidName,
				ErrInvalidCreateOptions,
			},
		},
		{
			name: "slash suffix name",
			args: args{
				options: &CreateDatasetOptions{
					Name: "tank/my-dataset/",
				},
			},
			wantErr: "zfs; invalid create options; invalid name",
			wantErrTargets: []error{
				Err,
				ErrZFS,
				ErrInvalidName,
				ErrInvalidCreateOptions,
			},
		},
		{
			name: "invalid 'all' property",
			args: args{
				options: &CreateDatasetOptions{
					Name: "tank/my-dataset",
					Properties: map[string]string{
						"all":            "off",
						(zfsprops.Atime): "off",
					},
				},
			},
			wantErr: "zfs; invalid property: 'all' is not a valid property",
			wantErrTargets: []error{
				Err,
				ErrZFS,
				ErrInvalidProperty,
			},
		},
		{
			name: "invalid empty property",
			args: args{
				options: &CreateDatasetOptions{
					Name: "tank/my-dataset",
					Properties: map[string]string{
						"":               "off",
						(zfsprops.Atime): "off",
					},
				},
			},
			wantErr: "zfs; invalid property: empty property name",
			wantErrTargets: []error{
				Err,
				ErrZFS,
				ErrInvalidProperty,
			},
		},
		{
			name: "filesystem",
			args: args{
				options: &CreateDatasetOptions{
					Name: "tank/my-dataset",
				},
			},
			wantArgs: []string{"create", "tank/my-dataset"},
		},
		{
			name: "filesystem with properties",
			args: args{
				options: &CreateDatasetOptions{
					Name: "tank/my-dataset",
					Properties: map[string]string{
						(zfsprops.Quota):      "10G",
						(zfsprops.Mountpoint): "/mnt/my-tank",
					},
				},
			},
			wantArgs: []string{
				"create", "-o", "mountpoint=/mnt/my-tank", "-o", "quota=10G",
				"tank/my-dataset",
			},
		},
		{
			name: "filesystem with create parents",
			args: args{
				options: &CreateDatasetOptions{
					Name:          "tank/thing/other/mine",
					CreateParents: true,
				},
			},
			wantArgs: []string{"create", "-p", "tank/thing/other/mine"},
		},
		{
			name: "filesystem all options",
			args: args{
				options: &CreateDatasetOptions{
					Name: "tank/my-dataset",
					Properties: map[string]string{
						(zfsprops.Quota):      "12G",
						(zfsprops.Mountpoint): "/mnt/my-tank",
					},
					CreateParents: true,
					Unmounted:     true,
				},
			},
			wantArgs: []string{
				"create", "-p", "-u",
				"-o", "mountpoint=/mnt/my-tank", "-o", "quota=12G",
				"tank/my-dataset",
			},
		},
		{
			name: "filesystem ignores volume options",
			args: args{
				options: &CreateDatasetOptions{
					Name:      "tank/my-dataset",
					BlockSize: "4K",
					Sparse:    true,
				},
			},
			wantArgs: []string{"create", "tank/my-dataset"},
		},
		{
			name: "volume",
			args: args{
				options: &CreateDatasetOptions{
					Name:       "tank/my-dataset",
					VolumeSize: "32G",
				},
			},
			wantArgs: []string{"create", "-V", "32G", "tank/my-dataset"},
		},
		{
			name: "volume with properties",
			args: args{
				options: &CreateDatasetOptions{
					Name: "tank/my-dataset",
					Properties: map[string]string{
						(zfsprops.Sync):        "disabled",
						(zfsprops.Compression): "lz4",
					},
					VolumeSize: "32G",
				},
			},
			wantArgs: []string{
				"create",
				"-o", "compression=lz4", "-o", "sync=disabled",
				"-V", "32G", "tank/my-dataset",
			},
		},
		{
			name: "volume with create parents",
			args: args{
				options: &CreateDatasetOptions{
					Name:          "tank/thing/other/mine",
					CreateParents: true,
					VolumeSize:    "48G",
				},
			},
			wantArgs: []string{
				"create", "-p", "-V", "48G", "tank/thing/other/mine",
			},
		},
		{
			name: "volume with block size",
			args: args{
				options: &CreateDatasetOptions{
					Name:       "tank/my-dataset",
					VolumeSize: "32G",
					BlockSize:  "8K",
				},
			},
			wantArgs: []string{
				"create", "-b", "8K", "-V", "32G", "tank/my-dataset",
			},
		},
		{
			name: "volume with sparse",
			args: args{
				options: &CreateDatasetOptions{
					Name:       "tank/my-dataset",
					VolumeSize: "32G",
					Sparse:     true,
				},
			},
			wantArgs: []string{"create", "-s", "-V", "32G", "tank/my-dataset"},
		},
		{
			name: "volume all options",
			args: args{
				options: &CreateDatasetOptions{
					Name: "tank/my-dataset",
					Properties: map[string]string{
						(zfsprops.Sync):        "disabled",
						(zfsprops.Compression): "lz4",
					},
					CreateParents: true,
					VolumeSize:    "32G",
					BlockSize:     "8K",
					Sparse:        true,
				},
			},
			wantArgs: []string{
				"create", "-p", "-b", "8K", "-s",
				"-o", "compression=lz4", "-o", "sync=disabled",
				"-V", "32G", "tank/my-dataset",
			},
		},
		{
			name: "volume ignores filesystem options",
			args: args{
				options: &CreateDatasetOptions{
					Name:       "tank/my-dataset",
					Unmounted:  true,
					VolumeSize: "32G",
				},
			},
			wantArgs: []string{"create", "-V", "32G", "tank/my-dataset"},
		},
		{
			name: "properties",
			args: args{
				options: &CreateDatasetOptions{
					Name: "tank/my-dataset",
					Properties: map[string]string{
						(zfsprops.Quota):      "10G",
						(zfsprops.Mountpoint): "/mnt/my-tank",
					},
				},
			},
			wantArgs: []string{
				"create", "-o", "mountpoint=/mnt/my-tank", "-o", "quota=10G",
				"tank/my-dataset",
			},
		},
		{
			name: "deeply nested without create parents",
			args: args{
				options: &CreateDatasetOptions{
					Name: "tank/my-dataset/foo/bar",
				},
			},
			wantArgs: []string{"create", "tank/my-dataset/foo/bar"},
			stderr: "cannot create 'tank/my-dataset/foo/bar': " +
				"parent does not exist\n",
			commandErr: errors.New("exit status 1"),
			wantErr: "zfs; not found; exit status 1: cannot create " +
				"'tank/my-dataset/foo/bar': parent does not exist",
			wantErrTargets: []error{Err, ErrZFS, ErrNotFound},
		},
		{
			name: "no such pool",
			args: args{
				options: &CreateDatasetOptions{
					Name: "tankz/my-dataset",
				},
			},
			wantArgs: []string{"create", "tankz/my-dataset"},
			stderr: "cannot create 'tankz/my-dataset': " +
				"no such pool 'tankz'\n",
			commandErr: errors.New("exit status 1"),
			wantErr: "zfs; not found; exit status 1: " +
				"cannot create 'tankz/my-dataset': no such pool 'tankz'",
			wantErrTargets: []error{Err, ErrZFS, ErrNotFound},
		},
		{
			name: "command error",
			args: args{
				options: &CreateDatasetOptions{
					Name: "tank/my-dataset",
					Properties: map[string]string{
						zfsprops.Quota: "what",
					},
				},
			},
			wantArgs: []string{
				"create", "-o", "quota=what", "tank/my-dataset",
			},
			stderr: `cannot create 'tank/my-dataset': bad numeric value 'what'
usage:
	create [-Pnpuv] [-o property=value] ... <filesystem>
	create [-Pnpsv] [-b blocksize] [-o property=value] ... -V <size> <volume>
`,
			commandErr: errors.New("exit status 1"),
			wantErr: "zfs; exit status 1: cannot create 'tank/my-dataset': " +
				"bad numeric value 'what'",
			wantErrTargets: []error{Err, ErrZFS},
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
					"zfs",
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

			err := m.CreateDataset(ctx, tt.args.options)

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

func TestManager_GetDataset(t *testing.T) {
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
		want           *Dataset
		commandErr     error
		wantErr        string
		wantErrTargets []error
	}{
		{
			name: "empty dataset name",
			args: args{
				name: "",
			},
			wantErr: "zfs; invalid name",
			wantErrTargets: []error{
				Err,
				ErrZFS,
				ErrInvalidName,
			},
		},
		{
			name: "slash prefix name",
			args: args{
				name: "/tank/my-dataset",
			},
			wantErr: "zfs; invalid name",
			wantErrTargets: []error{
				Err,
				ErrZFS,
				ErrInvalidName,
			},
		},
		{
			name: "slash suffix name",
			args: args{
				name: "tank/my-dataset/",
			},
			wantErr: "zfs; invalid name",
			wantErrTargets: []error{
				Err,
				ErrZFS,
				ErrInvalidName,
			},
		},
		{
			name: "no properties",
			args: args{
				name: "tank/my-dataset",
			},
			wantArgs: []string{
				"get", "-Hp", "-o", "name,property,value,source",
				"all", "tank/my-dataset",
			},
			stdout: "\n",
			want: &Dataset{
				Name:       "tank/my-dataset",
				Properties: Properties{},
			},
		},
		{
			name: "many properties",
			args: args{
				name: "tank/my-dataset",
			},
			wantArgs: []string{
				"get", "-Hp", "-o", "name,property,value,source",
				"all", "tank/my-dataset",
			},
			stdout: `tank/my-dataset	type	filesystem	-
tank/my-dataset	creation	1651487872	-
tank/my-dataset	used	20717056	-
tank/my-dataset	mounted	yes	-
tank/my-dataset	mountpoint	/mnt/my-tank	default
tank/my-dataset	overlay	on	default
tank/my-dataset	com.apple.ignoreowner	off	default
`,
			want: &Dataset{
				Name: "tank/my-dataset",
				Properties: Properties{
					(zfsprops.Type): {
						Name:     "tank/my-dataset",
						Property: "type",
						Value:    "filesystem",
						Source:   "-",
					},
					(zfsprops.Creation): {
						Name:     "tank/my-dataset",
						Property: "creation",
						Value:    "1651487872",
						Source:   "-",
					},
					(zfsprops.Used): {
						Name:     "tank/my-dataset",
						Property: "used",
						Value:    "20717056",
						Source:   "-",
					},
					(zfsprops.Mounted): {
						Name:     "tank/my-dataset",
						Property: "mounted",
						Value:    "yes",
						Source:   "-",
					},
					(zfsprops.Mountpoint): {
						Name:     "tank/my-dataset",
						Property: "mountpoint",
						Value:    "/mnt/my-tank",
						Source:   "default",
					},
					(zfsprops.Overlay): {
						Name:     "tank/my-dataset",
						Property: "overlay",
						Value:    "on",
						Source:   "default",
					},
					"com.apple.ignoreowner": {
						Name:     "tank/my-dataset",
						Property: "com.apple.ignoreowner",
						Value:    "off",
						Source:   "default",
					},
				},
			},
		},
		{
			name: "custom properties",
			args: args{
				name:       "tank/my-dataset",
				properties: []string{zfsprops.Type, zfsprops.Used},
			},
			wantArgs: []string{
				"get", "-Hp", "-o", "name,property,value,source",
				"type,used", "tank/my-dataset",
			},
			stdout: `tank/my-dataset	type	filesystem	-
tank/my-dataset	used	20717056	-
`,
			want: &Dataset{
				Name: "tank/my-dataset",
				Properties: Properties{
					(zfsprops.Type): {
						Name:     "tank/my-dataset",
						Property: "type",
						Value:    "filesystem",
						Source:   "-",
					},
					(zfsprops.Used): {
						Name:     "tank/my-dataset",
						Property: "used",
						Value:    "20717056",
						Source:   "-",
					},
				},
			},
		},
		{
			name: "dataset does not exist",
			args: args{
				name: "tank/my-other-dataset",
			},
			wantArgs: []string{
				"get", "-Hp", "-o", "name,property,value,source",
				"all", "tank/my-other-dataset",
			},
			stderr: "cannot open 'tank/my-other-dataset': " +
				"dataset does not exist\n",
			commandErr: errors.New("exit status 1"),
			wantErr: "zfs; not found; exit status 1: cannot open " +
				"'tank/my-other-dataset': dataset does not exist",
			wantErrTargets: []error{Err, ErrZFS, ErrNotFound},
		},
		{
			name: "command error",
			args: args{
				name:       "tank/my-other-dataset",
				properties: []string{"nothing"},
			},
			wantArgs: []string{
				"get", "-Hp", "-o", "name,property,value,source",
				"nothing", "tank/my-other-dataset",
			},
			stderr: `bad property list: invalid property 'nothing'
usage:
	get [-rHp] [-d max] [-o "all" | field[,...]]
`,
			commandErr: errors.New("exit status 1"),
			wantErr: "zfs; exit status 1: " +
				"bad property list: invalid property 'nothing'",
			wantErrTargets: []error{Err, ErrZFS},
		},
		{
			name: "output has wrong dataset name",
			args: args{
				name: "tank/my-dataset",
			},
			wantArgs: []string{
				"get", "-Hp", "-o", "name,property,value,source",
				"all", "tank/my-dataset",
			},
			stdout: "tank/my-other-dataset	type	filesystem	-\n",
			want: &Dataset{
				Name:       "tank/my-dataset",
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
					"zfs",
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

			got, err := m.GetDataset(ctx, tt.args.name, tt.args.properties...)
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

func TestManager_ListDatasets(t *testing.T) {
	ioWriter := reflect.TypeOf((*io.Writer)(nil)).Elem()

	type args struct {
		filter     string
		depth      uint64
		typ        DatasetType
		properties []string
	}
	tests := []struct {
		name           string
		args           args
		wantArgs       []string
		stdout         string
		stderr         string
		want           []*Dataset
		commandErr     error
		wantErr        string
		wantErrTargets []error
	}{
		{
			name: "no results",
			args: args{
				filter: "",
				depth:  0,
				typ:    FilesystemType,
			},
			wantArgs: []string{
				"get", "-Hp", "-o", "name,property,value,source", "-r",
				"-t", "filesystem", "all",
			},
			stdout: "\n",
			want:   []*Dataset{},
		},
		{
			name: "many results",
			args: args{
				filter: "",
				depth:  0,
				typ:    AllTypes,
			},
			wantArgs: []string{
				"get", "-Hp", "-o", "name,property,value,source", "-r",
				"-t", "all", "all",
			},
			stdout: `tank/my-dataset	type	filesystem	-
tank/my-dataset	used	20717056	-
tank/my-dataset	mountpoint	/mnt/my-tank	default
tank/my-other-dataset	type	filesystem	-
tank/my-other-dataset	used	349895	-
tank/my-other-dataset	mountpoint	/mnt/other-tank	default
`,
			want: []*Dataset{
				{
					Name: "tank/my-dataset",
					Properties: Properties{
						(zfsprops.Type): {
							Name:     "tank/my-dataset",
							Property: "type",
							Value:    "filesystem",
							Source:   "-",
						},
						(zfsprops.Used): {
							Name:     "tank/my-dataset",
							Property: "used",
							Value:    "20717056",
							Source:   "-",
						},
						(zfsprops.Mountpoint): {
							Name:     "tank/my-dataset",
							Property: "mountpoint",
							Value:    "/mnt/my-tank",
							Source:   "default",
						},
					},
				},
				{
					Name: "tank/my-other-dataset",
					Properties: Properties{
						(zfsprops.Type): {
							Name:     "tank/my-other-dataset",
							Property: "type",
							Value:    "filesystem",
							Source:   "-",
						},
						(zfsprops.Used): {
							Name:     "tank/my-other-dataset",
							Property: "used",
							Value:    "349895",
							Source:   "-",
						},
						(zfsprops.Mountpoint): {
							Name:     "tank/my-other-dataset",
							Property: "mountpoint",
							Value:    "/mnt/other-tank",
							Source:   "default",
						},
					},
				},
			},
		},
		{
			name: "custom properties",
			args: args{
				filter: "",
				depth:  0,
				typ:    AllTypes,
				properties: []string{
					"used",
					"mountpoint",
				},
			},
			wantArgs: []string{
				"get", "-Hp", "-o", "name,property,value,source", "-r",
				"-t", "all", "used,mountpoint",
			},
			stdout: `tank/my-dataset	used	20717056	-
tank/my-dataset	mountpoint	/mnt/my-tank	default
tank/my-other-dataset	used	349895	-
tank/my-other-dataset	mountpoint	/mnt/other-tank	default
`,
			want: []*Dataset{
				{
					Name: "tank/my-dataset",
					Properties: Properties{
						(zfsprops.Used): {
							Name:     "tank/my-dataset",
							Property: "used",
							Value:    "20717056",
							Source:   "-",
						},
						(zfsprops.Mountpoint): {
							Name:     "tank/my-dataset",
							Property: "mountpoint",
							Value:    "/mnt/my-tank",
							Source:   "default",
						},
					},
				},
				{
					Name: "tank/my-other-dataset",
					Properties: Properties{
						(zfsprops.Used): {
							Name:     "tank/my-other-dataset",
							Property: "used",
							Value:    "349895",
							Source:   "-",
						},
						(zfsprops.Mountpoint): {
							Name:     "tank/my-other-dataset",
							Property: "mountpoint",
							Value:    "/mnt/other-tank",
							Source:   "default",
						},
					},
				},
			},
		},
		{
			name: "filesystem type",
			args: args{
				filter: "",
				depth:  0,
				typ:    FilesystemType,
			},
			wantArgs: []string{
				"get", "-Hp", "-o", "name,property,value,source", "-r",
				"-t", "filesystem", "all",
			},
			stdout: `tank/my-dataset	type	filesystem	-
tank/my-other-dataset	type	filesystem	-
`,
			want: []*Dataset{
				{
					Name: "tank/my-dataset",
					Properties: Properties{
						(zfsprops.Type): {
							Name:     "tank/my-dataset",
							Property: "type",
							Value:    "filesystem",
							Source:   "-",
						},
					},
				},
				{
					Name: "tank/my-other-dataset",
					Properties: Properties{
						(zfsprops.Type): {
							Name:     "tank/my-other-dataset",
							Property: "type",
							Value:    "filesystem",
							Source:   "-",
						},
					},
				},
			},
		},
		{
			name: "volume type",
			args: args{
				filter: "",
				depth:  0,
				typ:    VolumeType,
			},
			wantArgs: []string{
				"get", "-Hp", "-o", "name,property,value,source", "-r",
				"-t", "volume", "all",
			},
			stdout: `rpool/ubuntu/swap	type	volume	-
tank/cache/media	type	volume	-
`,
			want: []*Dataset{
				{
					Name: "rpool/ubuntu/swap",
					Properties: Properties{
						(zfsprops.Type): {
							Name:     "rpool/ubuntu/swap",
							Property: "type",
							Value:    "volume",
							Source:   "-",
						},
					},
				},
				{
					Name: "tank/cache/media",
					Properties: Properties{
						(zfsprops.Type): {
							Name:     "tank/cache/media",
							Property: "type",
							Value:    "volume",
							Source:   "-",
						},
					},
				},
			},
		},
		{
			name: "snapshot type",
			args: args{
				filter: "",
				depth:  0,
				typ:    SnapshotType,
			},
			wantArgs: []string{
				"get", "-Hp", "-o", "name,property,value,source", "-r",
				"-t", "snapshot", "all",
			},
			stdout: `tank/my-dataset@before-reset	type	snapshot	-
tank/my-other-dataset@after-cleanup	type	snapshot	-
`,
			want: []*Dataset{
				{
					Name: "tank/my-dataset@before-reset",
					Properties: Properties{
						(zfsprops.Type): {
							Name:     "tank/my-dataset@before-reset",
							Property: "type",
							Value:    "snapshot",
							Source:   "-",
						},
					},
				},
				{
					Name: "tank/my-other-dataset@after-cleanup",
					Properties: Properties{
						(zfsprops.Type): {
							Name:     "tank/my-other-dataset@after-cleanup",
							Property: "type",
							Value:    "snapshot",
							Source:   "-",
						},
					},
				},
			},
		},
		{
			name: "bookmark type",
			args: args{
				filter: "",
				depth:  0,
				typ:    BookmarkType,
			},
			wantArgs: []string{
				"get", "-Hp", "-o", "name,property,value,source", "-r",
				"-t", "bookmark", "all",
			},
			stdout: `tank/my-dataset#daily	type	bookmark	-
tank/my-other-dataset#weekly	type	bookmark	-
`,
			want: []*Dataset{
				{
					Name: "tank/my-dataset#daily",
					Properties: Properties{
						(zfsprops.Type): {
							Name:     "tank/my-dataset#daily",
							Property: "type",
							Value:    "bookmark",
							Source:   "-",
						},
					},
				},
				{
					Name: "tank/my-other-dataset#weekly",
					Properties: Properties{
						(zfsprops.Type): {
							Name:     "tank/my-other-dataset#weekly",
							Property: "type",
							Value:    "bookmark",
							Source:   "-",
						},
					},
				},
			},
		},
		{
			name: "multiple types",
			args: args{
				filter: "",
				depth:  0,
				typ:    JoinTypes(FilesystemType, BookmarkType),
			},
			wantArgs: []string{
				"get", "-Hp", "-o", "name,property,value,source", "-r",
				"-t", "filesystem,bookmark", "all",
			},
			stdout: `tank/my-dataset	type	filesystem	-
tank/my-dataset#daily	type	bookmark	-
tank/my-other-dataset	type	filesystem	-
tank/my-other-dataset#weekly	type	bookmark	-
`,
			want: []*Dataset{
				{
					Name: "tank/my-dataset",
					Properties: Properties{
						(zfsprops.Type): {
							Name:     "tank/my-dataset",
							Property: "type",
							Value:    "filesystem",
							Source:   "-",
						},
					},
				},
				{
					Name: "tank/my-dataset#daily",
					Properties: Properties{
						(zfsprops.Type): {
							Name:     "tank/my-dataset#daily",
							Property: "type",
							Value:    "bookmark",
							Source:   "-",
						},
					},
				},
				{
					Name: "tank/my-other-dataset",
					Properties: Properties{
						(zfsprops.Type): {
							Name:     "tank/my-other-dataset",
							Property: "type",
							Value:    "filesystem",
							Source:   "-",
						},
					},
				},
				{
					Name: "tank/my-other-dataset#weekly",
					Properties: Properties{
						(zfsprops.Type): {
							Name:     "tank/my-other-dataset#weekly",
							Property: "type",
							Value:    "bookmark",
							Source:   "-",
						},
					},
				},
			},
		},
		{
			name: "all types",
			args: args{
				filter: "",
				depth:  0,
				typ:    AllTypes,
			},
			wantArgs: []string{
				"get", "-Hp", "-o", "name,property,value,source", "-r",
				"-t", "all", "all",
			},
			stdout: `tank/my-dataset	type	filesystem	-
tank/my-dataset@before-reset	type	snapshot	-
tank/my-dataset#daily	type	bookmark	-
rpool/ubuntu/swap	type	volume	-
`,
			want: []*Dataset{
				{
					Name: "tank/my-dataset",
					Properties: Properties{
						(zfsprops.Type): {
							Name:     "tank/my-dataset",
							Property: "type",
							Value:    "filesystem",
							Source:   "-",
						},
					},
				},
				{
					Name: "tank/my-dataset@before-reset",
					Properties: Properties{
						(zfsprops.Type): {
							Name:     "tank/my-dataset@before-reset",
							Property: "type",
							Value:    "snapshot",
							Source:   "-",
						},
					},
				},
				{
					Name: "tank/my-dataset#daily",
					Properties: Properties{
						(zfsprops.Type): {
							Name:     "tank/my-dataset#daily",
							Property: "type",
							Value:    "bookmark",
							Source:   "-",
						},
					},
				},
				{
					Name: "rpool/ubuntu/swap",
					Properties: Properties{
						(zfsprops.Type): {
							Name:     "rpool/ubuntu/swap",
							Property: "type",
							Value:    "volume",
							Source:   "-",
						},
					},
				},
			},
		},
		{
			name: "filter",
			args: args{
				filter: "tank/my-dataset/images",
				depth:  0,
				typ:    FilesystemType,
			},
			wantArgs: []string{
				"get", "-Hp", "-o", "name,property,value,source", "-r",
				"-t", "filesystem", "all", "tank/my-dataset/images",
			},
			stdout: `tank/my-dataset/images	type	filesystem	-
tank/my-dataset/images/gifs	type	filesystem	-
`,
			want: []*Dataset{
				{
					Name: "tank/my-dataset/images",
					Properties: Properties{
						(zfsprops.Type): {
							Name:     "tank/my-dataset/images",
							Property: "type",
							Value:    "filesystem",
							Source:   "-",
						},
					},
				},
				{
					Name: "tank/my-dataset/images/gifs",
					Properties: Properties{
						(zfsprops.Type): {
							Name:     "tank/my-dataset/images/gifs",
							Property: "type",
							Value:    "filesystem",
							Source:   "-",
						},
					},
				},
			},
		},
		{
			name: "depth 1",
			args: args{
				filter: "tank/my-dataset",
				depth:  1,
				typ:    FilesystemType,
			},
			wantArgs: []string{
				"get", "-Hp", "-o", "name,property,value,source", "-d", "1",
				"-t", "filesystem", "all", "tank/my-dataset",
			},
			stdout: `tank/my-dataset	type	filesystem	-
tank/my-dataset/images	type	filesystem	-
`,
			want: []*Dataset{
				{
					Name: "tank/my-dataset",
					Properties: Properties{
						(zfsprops.Type): {
							Name:     "tank/my-dataset",
							Property: "type",
							Value:    "filesystem",
							Source:   "-",
						},
					},
				},
				{
					Name: "tank/my-dataset/images",
					Properties: Properties{
						(zfsprops.Type): {
							Name:     "tank/my-dataset/images",
							Property: "type",
							Value:    "filesystem",
							Source:   "-",
						},
					},
				},
			},
		},
		{
			name: "depth 2",
			args: args{
				filter: "tank/my-dataset",
				depth:  2,
				typ:    FilesystemType,
			},
			wantArgs: []string{
				"get", "-Hp", "-o", "name,property,value,source", "-d", "2",
				"-t", "filesystem", "all", "tank/my-dataset",
			},
			stdout: `tank/my-dataset	type	filesystem	-
tank/my-dataset/images	type	filesystem	-
tank/my-dataset/images/gifs	type	filesystem	-
`,
			want: []*Dataset{
				{
					Name: "tank/my-dataset",
					Properties: Properties{
						(zfsprops.Type): {
							Name:     "tank/my-dataset",
							Property: "type",
							Value:    "filesystem",
							Source:   "-",
						},
					},
				},
				{
					Name: "tank/my-dataset/images",
					Properties: Properties{
						(zfsprops.Type): {
							Name:     "tank/my-dataset/images",
							Property: "type",
							Value:    "filesystem",
							Source:   "-",
						},
					},
				},
				{
					Name: "tank/my-dataset/images/gifs",
					Properties: Properties{
						(zfsprops.Type): {
							Name:     "tank/my-dataset/images/gifs",
							Property: "type",
							Value:    "filesystem",
							Source:   "-",
						},
					},
				},
			},
		},
		{
			name: "command error",
			args: args{
				filter: "",
				depth:  0,
				typ:    DatasetType("foobar"),
			},
			wantArgs: []string{
				"get", "-Hp", "-o", "name,property,value,source", "-r",
				"-t", "foobar", "all",
			},
			stderr: `invalid type '(null)'
usage:
	get [-rHp] [-d max] [-o "all" | field[,...]]
`,
			commandErr:     errors.New("exit status 3"),
			wantErr:        "zfs; exit status 3: invalid type '(null)'",
			wantErrTargets: []error{Err, ErrZFS},
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
					"zfs",
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

			got, err := m.ListDatasets(
				ctx,
				tt.args.filter,
				tt.args.depth,
				tt.args.typ,
				tt.args.properties...,
			)
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

func TestManager_ListDatasetNames(t *testing.T) {
	ioWriter := reflect.TypeOf((*io.Writer)(nil)).Elem()

	type args struct {
		filter string
		depth  uint64
		typ    DatasetType
	}
	tests := []struct {
		name           string
		args           args
		wantArgs       []string
		stdout         string
		stderr         string
		want           []string
		commandErr     error
		wantErr        string
		wantErrTargets []error
	}{
		{
			name: "no results",
			args: args{
				filter: "",
				depth:  0,
				typ:    FilesystemType,
			},
			wantArgs: []string{
				"list", "-H", "-o", "name", "-r", "-t", "filesystem",
			},
			stdout: "\n",
			want:   []string{},
		},
		{
			name: "many results",
			args: args{
				filter: "",
				depth:  0,
				typ:    AllTypes,
			},
			wantArgs: []string{
				"list", "-H", "-o", "name", "-r", "-t", "all",
			},
			stdout: "tank/my-dataset\ntank/my-other-dataset\n",
			want:   []string{"tank/my-dataset", "tank/my-other-dataset"},
		},
		{
			name: "filesystem type",
			args: args{
				filter: "",
				depth:  0,
				typ:    FilesystemType,
			},
			wantArgs: []string{
				"list", "-H", "-o", "name", "-r", "-t", "filesystem",
			},
			stdout: "tank/my-dataset\ntank/my-other-dataset\n",
			want:   []string{"tank/my-dataset", "tank/my-other-dataset"},
		},
		{
			name: "volume type",
			args: args{
				filter: "",
				depth:  0,
				typ:    VolumeType,
			},
			wantArgs: []string{
				"list", "-H", "-o", "name", "-r", "-t", "volume",
			},
			stdout: "rpool/ubuntu/swap\ntank/cache/media\n",
			want:   []string{"rpool/ubuntu/swap", "tank/cache/media"},
		},
		{
			name: "snapshot type",
			args: args{
				filter: "",
				depth:  0,
				typ:    SnapshotType,
			},
			wantArgs: []string{
				"list", "-H", "-o", "name", "-r", "-t", "snapshot",
			},
			stdout: `tank/my-dataset@before-reset
tank/my-other-dataset@after-cleanup
`,
			want: []string{
				"tank/my-dataset@before-reset",
				"tank/my-other-dataset@after-cleanup",
			},
		},
		{
			name: "bookmark type",
			args: args{
				filter: "",
				depth:  0,
				typ:    BookmarkType,
			},
			wantArgs: []string{
				"list", "-H", "-o", "name", "-r", "-t", "bookmark",
			},
			stdout: "tank/my-dataset#daily\ntank/my-other-dataset#weekly\n",
			want: []string{
				"tank/my-dataset#daily", "tank/my-other-dataset#weekly",
			},
		},
		{
			name: "multiple types",
			args: args{
				filter: "",
				depth:  0,
				typ:    JoinTypes(FilesystemType, VolumeType),
			},
			wantArgs: []string{
				"list", "-H", "-o", "name", "-r", "-t", "filesystem,volume",
			},
			stdout: `tank/my-dataset
tank/my-dataset#daily
tank/my-other-dataset
tank/my-other-dataset#weekly
`,
			want: []string{
				"tank/my-dataset",
				"tank/my-dataset#daily",
				"tank/my-other-dataset",
				"tank/my-other-dataset#weekly",
			},
		},
		{
			name: "all types",
			args: args{
				filter: "",
				depth:  0,
				typ:    AllTypes,
			},
			wantArgs: []string{
				"list", "-H", "-o", "name", "-r", "-t", "all",
			},
			stdout: `tank/my-dataset
tank/my-dataset@before-reset
tank/my-dataset#daily
rpool/ubuntu/swap
`,
			want: []string{
				"tank/my-dataset",
				"tank/my-dataset@before-reset",
				"tank/my-dataset#daily",
				"rpool/ubuntu/swap",
			},
		},
		{
			name: "filter",
			args: args{
				filter: "tank/my-dataset/images",
				depth:  0,
				typ:    FilesystemType,
			},
			wantArgs: []string{
				"list", "-H", "-o", "name", "-r", "-t", "filesystem",
				"tank/my-dataset/images",
			},
			stdout: "tank/my-dataset/images\ntank/my-dataset/images/gifs\n",
			want: []string{
				"tank/my-dataset/images", "tank/my-dataset/images/gifs",
			},
		},
		{
			name: "depth 1",
			args: args{
				filter: "tank/my-dataset",
				depth:  1,
				typ:    FilesystemType,
			},
			wantArgs: []string{
				"list", "-H", "-o", "name", "-d", "1", "-t", "filesystem",
				"tank/my-dataset",
			},
			stdout: "tank/my-dataset\ntank/my-dataset/images\n",
			want: []string{
				"tank/my-dataset", "tank/my-dataset/images",
			},
		},
		{
			name: "depth 2",
			args: args{
				filter: "tank/my-dataset",
				depth:  2,
				typ:    FilesystemType,
			},
			wantArgs: []string{
				"list", "-H", "-o", "name", "-d", "2", "-t", "filesystem",
				"tank/my-dataset",
			},
			stdout: `tank/my-dataset
tank/my-dataset/images
tank/my-dataset/images/gifs
`,
			want: []string{
				"tank/my-dataset",
				"tank/my-dataset/images",
				"tank/my-dataset/images/gifs",
			},
		},
		{
			name: "command error",
			args: args{
				filter: "",
				depth:  0,
				typ:    DatasetType("foobar"),
			},
			wantArgs: []string{
				"list", "-H", "-o", "name", "-r", "-t", "foobar",
			},
			stderr: `invalid type '(null)'
usage:
	get [-rHp] [-d max] [-o "all" | field[,...]]
`,
			commandErr:     errors.New("exit status 3"),
			wantErr:        "zfs; exit status 3: invalid type '(null)'",
			wantErrTargets: []error{Err, ErrZFS},
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
					"zfs",
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

			got, err := m.ListDatasetNames(
				ctx,
				tt.args.filter,
				tt.args.depth,
				tt.args.typ,
			)
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

func TestManager_DestroyDataset(t *testing.T) {
	ioWriter := reflect.TypeOf((*io.Writer)(nil)).Elem()

	type args struct {
		name  string
		flags []DestroyDatasetFlag
	}
	tests := []struct {
		name           string
		args           args
		wantArgs       []string
		want           *Dataset
		stderr         string
		commandErr     error
		wantErr        string
		wantErrTargets []error
	}{
		{
			name: "empty dataset name",
			args: args{
				name: "",
			},
			wantErr: "zfs; invalid name",
			wantErrTargets: []error{
				Err,
				ErrZFS,
				ErrInvalidName,
			},
		},
		{
			name: "slash prefix name",
			args: args{
				name: "/tank/my-dataset",
			},
			wantErr: "zfs; invalid name",
			wantErrTargets: []error{
				Err,
				ErrZFS,
				ErrInvalidName,
			},
		},
		{
			name: "slash suffix name",
			args: args{
				name: "tank/my-dataset/",
			},
			wantErr: "zfs; invalid name",
			wantErrTargets: []error{
				Err,
				ErrZFS,
				ErrInvalidName,
			},
		},
		{
			name: "zerod flag",
			args: args{
				name:  "tank/my-dataset",
				flags: []DestroyDatasetFlag{DestroyDatasetFlag(0)},
			},
			wantArgs: []string{"destroy", "tank/my-dataset"},
		},
		{
			name: "recursive flag",
			args: args{
				name:  "tank/my-dataset",
				flags: []DestroyDatasetFlag{DestroyRecursive},
			},
			wantArgs: []string{"destroy", "-r", "tank/my-dataset"},
		},
		{
			name: "recursive clones flag",
			args: args{
				name:  "tank/my-dataset",
				flags: []DestroyDatasetFlag{DestroyRecursiveClones},
			},
			wantArgs: []string{"destroy", "-R", "tank/my-dataset"},
		},
		{
			name: "defer deletion flag",
			args: args{
				name:  "tank/my-dataset",
				flags: []DestroyDatasetFlag{DestroyDeferDeletion},
			},
			wantArgs: []string{"destroy", "-d", "tank/my-dataset"},
		},
		{
			name: "force unmount flag",
			args: args{
				name:  "tank/my-dataset",
				flags: []DestroyDatasetFlag{DestroyForceUnmount},
			},
			wantArgs: []string{"destroy", "-f", "tank/my-dataset"},
		},
		{
			name: "recursive and force unmount flag",
			args: args{
				name: "tank/my-dataset",
				flags: []DestroyDatasetFlag{
					DestroyRecursive,
					DestroyForceUnmount,
				},
			},
			wantArgs: []string{"destroy", "-r", "-f", "tank/my-dataset"},
		},
		{
			name: "recursive clones, defer deletiong, and force unmount flag",
			args: args{
				name: "tank/my-dataset@last-week",
				flags: []DestroyDatasetFlag{
					DestroyRecursiveClones,
					DestroyDeferDeletion,
					DestroyForceUnmount,
				},
			},
			wantArgs: []string{
				"destroy", "-R", "-d", "-f", "tank/my-dataset@last-week",
			},
		},
		{
			name: "all flags",
			args: args{
				name: "tank/my-dataset@last-week",
				flags: []DestroyDatasetFlag{
					DestroyRecursive,
					DestroyRecursiveClones,
					DestroyDeferDeletion,
					DestroyForceUnmount,
				},
			},
			wantArgs: []string{
				"destroy", "-r", "-R", "-d", "-f", "tank/my-dataset@last-week",
			},
		},
		{
			name: "dataset does not exist",
			args: args{
				name: "tank/my-other-dataset",
			},
			wantArgs: []string{
				"destroy", "tank/my-other-dataset",
			},
			stderr: "cannot open 'tank/my-other-dataset': " +
				"dataset does not exist\n",
			commandErr: errors.New("exit status 1"),
			wantErr: "zfs; not found; exit status 1: cannot open " +
				"'tank/my-other-dataset': dataset does not exist",
			wantErrTargets: []error{Err, ErrZFS, ErrNotFound},
		},
		{
			name: "command error",
			args: args{
				name: "tank/my-other-dataset",
			},
			wantArgs: []string{"destroy", "tank/my-other-dataset"},
			stderr: `destroy is broken right now
usage:
	destroy [-fnpRrv] <filesystem|volume>
`,
			commandErr:     errors.New("exit status 1"),
			wantErr:        "zfs; exit status 1: destroy is broken right now",
			wantErrTargets: []error{Err, ErrZFS},
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
					"zfs",
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

			err := m.DestroyDataset(ctx, tt.args.name, tt.args.flags...)
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
