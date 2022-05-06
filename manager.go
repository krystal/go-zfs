package zfs

import (
	"bytes"
	"errors"
	"fmt"
	"path"
	"strings"

	"github.com/krystal/go-runner"
)

var (
	Err                     = errors.New("")
	ErrZFS                  = fmt.Errorf("%wzfs", Err)
	ErrZpool                = fmt.Errorf("%wzpool", Err)
	ErrInvalidName          = fmt.Errorf("%winvalid name", Err)
	ErrInvalidProperty      = fmt.Errorf("%winvalid property", Err)
	ErrInvalidCreateOptions = fmt.Errorf("%winvalid create options", Err)
)

// Manager is used to perform all zfs and zpool operations.
//
// A runner.Runner is used to execute all commands. You can use a custom runner
// to modify the behavior of the executed commands. The runner package for
// example provides a "Sudo" runner struct that executes all commands via sudo.
type Manager struct {
	Runner runner.Runner
}

// New returns a new Manager instance which is used to perform all zfs and zpool
// operations.
//
// The default Runner assigned will execute all zfs and zpool commands on the
// local host machine, without sudo. As zfs operations typically need to be
// performed as root, you most likely need to run the Go application as root, or
// use a runner.Sudo instance to execute zfs and zpool commands via sudo.
func New() *Manager {
	return &Manager{
		Runner: runner.New(),
	}
}

// Join joins the given parts with a "/" separator. Useful for building dataset
// names.
func Join(parts ...string) string {
	return strings.TrimPrefix(path.Join(parts...), "/")
}

// cleanUpStderr tidies up stderr output from zfs and zpool commands by:
//
//  - Removing the usage/help message if included.
//  - Removing leading and trailing whitespace.
//  - Removing empty lines.
//  - Joining lines with a ": " separator.
func cleanUpStderr(stderr []byte) []byte {
	if i := bytes.Index(stderr, []byte("\nusage:\n")); i != -1 {
		stderr = stderr[0:i]
	}

	out := [][]byte{}
	lines := bytes.Split(bytes.TrimSpace(stderr), []byte("\n"))

	for _, line := range lines {
		if v := bytes.TrimSpace(line); len(v) > 0 {
			out = append(out, v)
		}
	}

	return bytes.Join(out, []byte(": "))
}
