package zfs

import (
	"github.com/krystal/go-zfs/zpoolprops"
)

const (
	HealthDegraded    = "DEGRADED"
	HealthFaulted     = "FAULTED"
	HealthOffline     = "OFFLINE"
	HealthOnline      = "ONLINE"
	HealthRemoved     = "REMOVED"
	HealthUnavailable = "UNAVAIL"
)

type Pool struct {
	// Name of the pool.
	Name string

	// Properties of the pool.
	Properties
}

func newPool(name string, properties Properties) *Pool {
	props := Properties{}

	for _, prop := range properties {
		if prop.Name == name {
			props[prop.Property] = prop
		}
	}

	return &Pool{
		Name:       name,
		Properties: props,
	}
}

// ReadOnly returns the value of the "readonly" property as a bool.
//
// The second return value indicates if the property is present in the Pool
// instance.
func (p *Pool) ReadOnly() (bool, bool) {
	return p.Bool(zpoolprops.ReadOnly)
}

// Allocated returns the value of the "allocated" property as number of bytes.
//
// The second return value indicates if the property is present in the Pool
// instance.
func (p *Pool) Allocated() (uint64, bool) {
	return p.Bytes(zpoolprops.Allocated)
}

// Free returns the value of the "free" property as number of bytes.
//
// The second return value indicates if the property is present in the Pool
// instance.
func (p *Pool) Free() (uint64, bool) {
	return p.Bytes(zpoolprops.Free)
}

// Freeing returns the value of the "freeing" property as number of bytes.
//
// The second return value indicates if the property is present in the Pool
// instance.
func (p *Pool) Freeing() (uint64, bool) {
	return p.Bytes(zpoolprops.Freeing)
}

// Leaked returns the value of the "leaked" property as number of bytes.
//
// The second return value indicates if the property is present in the Pool
// instance.
func (p *Pool) Leaked() (uint64, bool) {
	return p.Bytes(zpoolprops.Leaked)
}

// Size returns the value of the "size" property as number of bytes.
//
// The second return value indicates if the property is present in the Pool
// instance.
func (p *Pool) Size() (uint64, bool) {
	return p.Bytes(zpoolprops.Size)
}

// Capacity returns the value of the "capacity" property as number of bytes.
//
// The second return value indicates if the property is present in the Pool
// instance.
func (p *Pool) Capacity() (uint64, bool) {
	return p.Percent(zpoolprops.Capacity)
}

// Fragmentation returns the value of the "fragmentation" property as number of
// bytes.
//
// The second return value indicates if the property is present in the Pool
// instance.
func (p *Pool) Fragmentation() (uint64, bool) {
	return p.Percent(zpoolprops.Fragmentation)
}

// Health returns the value of the "health" property.
//
// The second return value indicates if the property is present in the Pool
// instance.
func (p *Pool) Health() (string, bool) {
	return p.String(zpoolprops.Health)
}
