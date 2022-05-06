package zfs

import (
	"testing"

	"github.com/krystal/go-runner"
	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	mgr := New()

	assert.NotNil(t, mgr)
	assert.IsType(t, (*Manager)(nil), mgr)

	// Assert that Runner defaults to a new instance of *runner.Local as
	// returned from runner.New().
	assert.NotNil(t, mgr.Runner)
	assert.IsType(t, (*runner.Local)(nil), mgr.Runner)
}

func TestJoin(t *testing.T) {
	type args struct {
		parts []string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "none",
			args: args{
				parts: []string{},
			},
			want: "",
		},
		{
			name: "one part",
			args: args{
				parts: []string{"foo"},
			},
			want: "foo",
		},
		{
			name: "many parts",
			args: args{
				parts: []string{"foo", "bar", "baz"},
			},
			want: "foo/bar/baz",
		},
		{
			name: "with trailing slashes",
			args: args{
				parts: []string{"foo/", "bar/", "baz/"},
			},
			want: "foo/bar/baz",
		},
		{
			name: "with trailing and leading slashes",
			args: args{
				parts: []string{"/foo/", "/bar/", "/baz/"},
			},
			want: "foo/bar/baz",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Join(tt.args.parts...)

			assert.Equal(t, tt.want, got)
		})
	}
}
