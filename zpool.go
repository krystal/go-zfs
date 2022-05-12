package zfs

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"go.uber.org/multierr"
)

var (
	errInvalidPoolName          = multierr.Append(ErrZpool, ErrInvalidName)
	errInvalidPoolProperty      = multierr.Append(ErrZpool, ErrInvalidProperty)
	errInvalidCreatePoolOptions = multierr.Append(
		ErrZpool, ErrInvalidCreateOptions,
	)
)

func (m *Manager) zpool(
	ctx context.Context,
	args ...string,
) ([][]string, error) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := m.Runner.RunContext(ctx, nil, &stdout, &stderr, "zpool", args...)
	if err != nil {
		return nil, multierr.Append(
			ErrZpool,
			fmt.Errorf("%w: %s", err, cleanUpStderr(stderr.Bytes())),
		)
	}

	return parseTabular(stdout.Bytes()), nil
}

func (m *Manager) validPoolName(name string) bool {
	return len(name) > 0 && !strings.Contains(name, "/")
}

// GetProperty returns the value of property on zpool with name.
func (m *Manager) GetPoolProperty(
	ctx context.Context,
	name string,
	property string,
) (string, error) {
	if !m.validPoolName(name) {
		return "", errInvalidPoolName
	}

	if property == "" || property == allProperty {
		return "", errInvalidPoolProperty
	}

	records, err := m.zpool(ctx, "get", "-Hp", "-o", "value", property, name)
	if err != nil {
		return "", err
	}

	return records[0][0], nil
}

// SetProperty sets property to value on zpool with name.
func (m *Manager) SetPoolProperty(
	ctx context.Context,
	name string,
	property string,
	value string,
) error {
	return m.SetPoolProperties(ctx, name, map[string]string{property: value})
}

// SetPoolProperties sets given properties on pool with name.
func (m *Manager) SetPoolProperties(
	ctx context.Context,
	name string,
	properties map[string]string,
) error {
	if !m.validPoolName(name) {
		return errInvalidPoolName
	}

	args := []string{"set"}
	propArgs, err := propertyMapFlags("", properties)
	if err != nil {
		return multierr.Append(ErrZpool, err)
	}
	args = append(args, propArgs...)
	args = append(args, name)

	_, err = m.zpool(ctx, args...)

	return err
}

// CreatePoolOptions are options for creating a new zpool.
type CreatePoolOptions struct {
	// Name of the pool. (required)
	Name string

	// Vdevs is a list of vdevs to pass to zpool create. (required)
	Vdevs []string

	// Properties is a map of properties (-o) to set on the pool.
	Properties map[string]string

	// FilesystemProperties is a map of filesystem properties (-O) to set on the
	// pool.
	FilesystemProperties map[string]string

	// Mountpoint is the mountpoint (-m) for the pool.
	Mountpoint string

	// Root is the root (-R) for the pool.
	Root string

	// Force indicates whether to force flag (-f) should be set.
	Force bool

	// DisableFeatures indicates whether to disable features flag (-d) should be
	// set.
	DisableFeatures bool

	// Args is a list of additional arguments to pass to zpool create.
	Args []string
}

// CreatePool create a new pool with the given options.
func (m *Manager) CreatePool(
	ctx context.Context,
	options *CreatePoolOptions,
) error {
	if options == nil {
		return errInvalidCreatePoolOptions
	}
	if !m.validPoolName(options.Name) {
		return multierr.Combine(
			ErrZpool,
			ErrInvalidCreateOptions,
			ErrInvalidName,
		)
	}
	if len(options.Vdevs) == 0 {
		return fmt.Errorf("%w: no vdevs specified", errInvalidCreatePoolOptions)
	}

	args := []string{"create"}
	if options.Mountpoint != "" {
		args = append(args, "-m", options.Mountpoint)
	}
	if options.Root != "" {
		args = append(args, "-R", options.Root)
	}
	if options.Force {
		args = append(args, "-f")
	}
	if options.DisableFeatures {
		args = append(args, "-d")
	}

	poolProps, err := propertyMapFlags("-o", options.Properties)
	if err != nil {
		return multierr.Append(ErrZpool, err)
	}
	args = append(args, poolProps...)

	fsProps, err := propertyMapFlags("-O", options.FilesystemProperties)
	if err != nil {
		return multierr.Append(ErrZpool, err)
	}
	args = append(args, fsProps...)

	args = append(args, options.Args...)
	args = append(args, options.Name)
	args = append(args, options.Vdevs...)

	_, err = m.zpool(ctx, args...)

	return err
}

// GetPool returns a *Pool instance for named pool.
//
// If properties are specified, only those properties are returned for the pool,
// otherwise all properties are returned.
func (m *Manager) GetPool(
	ctx context.Context,
	name string,
	properties ...string,
) (*Pool, error) {
	if !m.validPoolName(name) {
		return nil, errInvalidPoolName
	}
	if len(properties) == 0 {
		properties = []string{allProperty}
	}

	records, err := m.zpool(ctx,
		"get", "-Hp", "-o", "name,property,value,source",
		strings.Join(properties, ","), name,
	)
	if err != nil {
		return nil, err
	}

	props := newProperties(records)

	return newPool(name, props[name]), nil
}

// ListPools returns a slice of *Pool instances for all pools.
//
// If properties are specified, only those properties are returned for each
// pool, otherwise all properties are returned.
func (m *Manager) ListPools(
	ctx context.Context,
	properties ...string,
) ([]*Pool, error) {
	if len(properties) == 0 {
		properties = []string{allProperty}
	}

	records, err := m.zpool(ctx,
		"get", "-Hp", "-o", "name,property,value,source",
		strings.Join(properties, ","),
	)
	if err != nil {
		return nil, err
	}

	props := newProperties(records)
	pools := make([]*Pool, 0, len(props))
	for name, poolProps := range props {
		pools = append(pools, newPool(name, poolProps))
	}

	return pools, nil
}

// ListPoolNames returns a string slice of all pool names.
func (m *Manager) ListPoolNames(ctx context.Context) ([]string, error) {
	records, err := m.zpool(ctx, "list", "-Hp", "-o", "name")
	if err != nil {
		return nil, err
	}

	names := []string{}
	for _, record := range records {
		if len(record) > 0 && record[0] != "" {
			names = append(names, record[0])
		}
	}

	return names, nil
}

// DestroyPool destroys the named pool, optionally passing the force flag (-f)
// to zpool destroy.
func (m *Manager) DestroyPool(
	ctx context.Context,
	name string,
	force bool,
) error {
	if !m.validPoolName(name) {
		return errInvalidPoolName
	}

	args := []string{"destroy"}
	if force {
		args = append(args, "-f")
	}
	args = append(args, name)

	_, err := m.zpool(ctx, args...)

	return err
}

// ImportPoolOptions are options for importing a pool.
type ImportPoolOptions struct {
	// Name of the pool to import.
	Name string

	// Properties is a map of properties (-o) to set on the pool.
	Properties map[string]string

	// Force indicates whether to force flag (-f) should be set.
	Force bool

	// Args is a list of additional arguments to pass to zpool import.
	Args []string

	// DirOrDevice is a list of directories or devices, each passed with the -d
	// flag to zpool import.
	DirOrDevice []string
}

// ImportPool imports the named pool based on the given options.
func (m *Manager) ImportPool(
	ctx context.Context,
	options *ImportPoolOptions,
) error {
	if options == nil {
		options = &ImportPoolOptions{}
	}
	if options.Name != "" && !m.validPoolName(options.Name) {
		return errInvalidPoolName
	}

	args := []string{"import"}
	if options.Force {
		args = append(args, "-f")
	}

	poolProps, err := propertyMapFlags("-o", options.Properties)
	if err != nil {
		return multierr.Append(ErrZpool, err)
	}
	args = append(args, poolProps...,
	)
	if len(options.DirOrDevice) > 0 {
		for _, v := range options.DirOrDevice {
			args = append(args, "-d", v)
		}
	}
	args = append(args, options.Args...)
	if options.Name != "" {
		args = append(args, options.Name)
	}

	_, err = m.zpool(ctx, args...)

	return err
}

// ExportPool exports the named pool, optionally passing the force flag (-f) to
// zpool export.
func (m *Manager) ExportPool(
	ctx context.Context,
	name string,
	force bool,
) error {
	if !m.validPoolName(name) {
		return errInvalidPoolName
	}

	args := []string{"export"}
	if force {
		args = append(args, "-f")
	}
	args = append(args, name)

	_, err := m.zpool(ctx, args...)

	return err
}
