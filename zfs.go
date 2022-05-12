// Package zfs enables ZFS pool and dataset management by wrapping "zfs" and
// "zpool" CLI commands.
//
// All interactions with ZFS are done through Manager.
package zfs

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"

	"go.uber.org/multierr"
)

var (
	errInvalidDatasetName     = multierr.Append(ErrZFS, ErrInvalidName)
	errInvalidDatasetProperty = multierr.Append(ErrZFS, ErrInvalidProperty)
)

func (m *Manager) zfs(ctx context.Context, args ...string) ([][]string, error) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := m.Runner.RunContext(ctx, nil, &stdout, &stderr, "zfs", args...)
	if err != nil {
		return nil, multierr.Append(
			ErrZFS,
			fmt.Errorf("%w: %s", err, cleanUpStderr(stderr.Bytes())),
		)
	}

	return parseTabular(stdout.Bytes()), nil
}

func (m *Manager) validDatasetName(name string) bool {
	return len(name) > 0 && name[0] != '/' && name[len(name)-1] != '/'
}

// GetDatasetProperty returns the value of the given property for the given
// dataset.
func (m *Manager) GetDatasetProperty(
	ctx context.Context,
	name string,
	property string,
) (string, error) {
	if !m.validDatasetName(name) {
		return "", errInvalidDatasetName
	}

	if property == "" || property == allProperty {
		return "", errInvalidDatasetProperty
	}

	records, err := m.zfs(ctx, "get", "-Hp", "-o", "value", property, name)
	if err != nil {
		return "", err
	}

	return records[0][0], nil
}

// SetDatasetProperty sets property to value on dataset with name.
func (m *Manager) SetDatasetProperty(
	ctx context.Context,
	name string,
	property string,
	value string,
) error {
	if !m.validDatasetName(name) {
		return errInvalidDatasetName
	}

	if property == "" || property == allProperty {
		return errInvalidDatasetProperty
	}

	_, err := m.zfs(ctx, "set", fmt.Sprintf("%s=%s", property, value), name)

	return err
}

// InheritDatasetProperty sets property to inherit from parent dataset.
func (m *Manager) InheritDatasetProperty(
	ctx context.Context,
	name string,
	property string,
	recursive bool,
) error {
	if !m.validDatasetName(name) {
		return errInvalidDatasetName
	}

	if property == "" {
		return errInvalidDatasetProperty
	}

	args := []string{"inherit"}
	if recursive {
		args = append(args, "-r")
	}
	args = append(args, property, name)

	_, err := m.zfs(ctx, args...)

	return err
}

// CreateDatasetOptions are options for creating a new dataset.
type CreateDatasetOptions struct {
	// Name of the dataset. (required)
	Name string

	// Properties is a map of properties (-o) to set on the dataset.
	Properties map[string]string

	// CreateParents indicates whether to create any missing parent datasets by
	// passing the -p flag.
	CreateParents bool

	// Unmounted indicates whether to create the dataset without mounting it by
	// passing the -u flag.
	//
	// Ignored when VolumeSize is set.
	Unmounted bool

	// VolumeSize indicates we are creating a volume dataset instead of a
	// filesystem dataset. Hence to create a filesystem, VolumeSize must be
	// empty.
	VolumeSize string

	// BlockSize is the block size to use for the volume dataset by passing the
	// -b flag.
	//
	// Ignored when VolumeSize is empty.
	BlockSize string

	// Sparse indicates whether to create a sparse volume by passing the -s
	// flag.
	//
	// Ignored when VolumeSize is empty.
	Sparse bool
}

// CreateDataset creates a new dataset with the given options.
func (m *Manager) CreateDataset(
	ctx context.Context,
	options *CreateDatasetOptions,
) error {
	if options == nil {
		return multierr.Append(ErrZFS, ErrInvalidCreateOptions)
	}
	if !m.validDatasetName(options.Name) {
		return multierr.Combine(
			ErrZFS,
			ErrInvalidCreateOptions,
			ErrInvalidName,
		)
	}

	args := []string{"create"}
	if options.CreateParents {
		args = append(args, "-p")
	}
	if options.VolumeSize == "" {
		if options.Unmounted {
			args = append(args, "-u")
		}
	} else {
		if options.BlockSize != "" {
			args = append(args, "-b", options.BlockSize)
		}
		if options.Sparse {
			args = append(args, "-s")
		}
	}

	args = append(
		args, propertyMapFlags("-o", options.Properties)...,
	)
	if options.VolumeSize != "" {
		args = append(args, "-V", options.VolumeSize)
	}

	args = append(args, options.Name)

	_, err := m.zfs(ctx, args...)

	return err
}

// GetDataset returns a *Dataset instance for named dataset.
//
// If properties are specified, only those properties are returned for the
// dataset, otherwise all properties are returned.
func (m *Manager) GetDataset(
	ctx context.Context,
	name string,
	properties ...string,
) (*Dataset, error) {
	if !m.validDatasetName(name) {
		return nil, errInvalidDatasetName
	}
	if len(properties) == 0 {
		properties = []string{allProperty}
	}

	records, err := m.zfs(ctx,
		"get", "-Hp", "-o", "name,property,value,source",
		strings.Join(properties, ","), name,
	)
	if err != nil {
		return nil, err
	}

	props := newProperties(records)

	return NewDataset(name, props[name]), nil
}

// ListDatasets returns a slice of *Dataset instances based on the given
// arguments.
//
// If properties are specified, only those properties are returned for each
// dataset, otherwise all properties are returned.
func (m *Manager) ListDatasets(
	ctx context.Context,
	filter string,
	depth uint64,
	typ DatasetType,
	properties ...string,
) ([]*Dataset, error) {
	args := []string{"get", "-Hp", "-o", "name,property,value,source"}

	if depth > 0 {
		args = append(args, "-d", strconv.FormatUint(depth, 10))
	} else {
		args = append(args, "-r")
	}

	args = append(args, "-t", string(typ))

	if len(properties) == 0 {
		args = append(args, allProperty)
	} else {
		args = append(args, strings.Join(properties, ","))
	}

	if filter != "" {
		args = append(args, filter)
	}

	records, err := m.zfs(ctx, args...)
	if err != nil {
		return nil, err
	}

	props := newProperties(records)
	datasets := make([]*Dataset, 0, len(props))
	for name, datasetProps := range props {
		datasets = append(datasets, NewDataset(name, datasetProps))
	}

	return datasets, nil
}

// ListDatasetNames returns a string slice of dataset names matching the given
// arguments.
func (m *Manager) ListDatasetNames(
	ctx context.Context,
	filter string,
	depth uint64,
	typ DatasetType,
) ([]string, error) {
	args := []string{"list", "-H", "-o", "name"}

	if depth > 0 {
		args = append(args, "-d", strconv.FormatUint(depth, 10))
	} else {
		args = append(args, "-r")
	}

	args = append(args, "-t", string(typ))

	if filter != "" {
		args = append(args, filter)
	}

	records, err := m.zfs(ctx, args...)
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

// DestroyDatasetFlag is a value that is passed to DestroyDataset to specify the
// destruction behavior for datasets.
type DestroyDatasetFlag int

const (
	// DestroyRecursive indicates that the -r flag should be passed to zfs
	// destroy.
	//
	// When destroying filesystems and volumes:
	//
	//    Recursively destroy all children.
	//
	// When destroying snapshots:
	//
	//    Destroy (or mark for deferred deletion) all snapshots with this name
	//    in descendent file systems.
	DestroyRecursive DestroyDatasetFlag = iota + 1

	// DestroyRecursiveClones indicates that the -R flag should be passed to zfs
	// destroy.
	//
	// When destroying filesystems and volumes:
	//
	//    Recursively destroy all dependents, including cloned file systems
	//    outside the target hierarchy.
	//
	// When destroying snapshots:
	//
	//    Recursively destroy all clones of these snapshots, including the
	//    clones, snapshots, and children.  If this flag is specified,
	//    DestroyDeferDeletion will have no effect.
	DestroyRecursiveClones

	// DestroyDeferDeletion indicates that the -d flag should be passed to zfs
	// destroy.
	//
	// Destroy immediately. If a snapshot cannot be destroyed now, mark
	// it for deferred destruction.
	//
	// Only valid when destroying snapshots.
	DestroyDeferDeletion

	// DestroyForceUnmount indicates that the -f flag should be passed to zfs
	// destroy.
	//
	// Force an unmount of any file systems using the unmount -f command. This
	// option has no effect on non-filesystems or unmounted filesystems.
	DestroyForceUnmount
)

// DestroyDataset destroys the named dataset.
func (m *Manager) DestroyDataset(
	ctx context.Context,
	name string,
	flags ...DestroyDatasetFlag,
) error {
	if !m.validDatasetName(name) {
		return errInvalidDatasetName
	}

	args := []string{"destroy"}
	fm := map[DestroyDatasetFlag]struct{}{}
	for _, flag := range flags {
		fm[flag] = struct{}{}
	}

	if _, ok := fm[DestroyRecursive]; ok {
		args = append(args, "-r")
	}
	if _, ok := fm[DestroyRecursiveClones]; ok {
		args = append(args, "-R")
	}
	if _, ok := fm[DestroyDeferDeletion]; ok {
		args = append(args, "-d")
	}
	if _, ok := fm[DestroyForceUnmount]; ok {
		args = append(args, "-f")
	}

	args = append(args, name)

	_, err := m.zfs(ctx, args...)

	return err
}
