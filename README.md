<h1 align="center">
  go-zfs
</h1>

<p align="center">
  <strong>
    Go package that enables ZFS pool and dataset management by wrapping
    <code>zfs</code> and <code>zpool</code> CLI commands.
  </strong>
</p>

## Import

```go
import "github.com/krystal/go-zfs"
```

## Usage

```go
import (
	"github.com/krystal/go-zfs"
	"github.com/krystal/go-zfs/zfsprops"
)

// Create a *zfs.Manager instance.
z := zfs.New()

// Get details for a specific dataset.
ds, _ := z.GetDataset(ctx, "tank/my-data")

fmt.Printf("Name: %s\n", ds.Name)

// Get "used" dataset property via Used() helper method, which returns the value
// as a uint64.
if v, ok := ds.Used(); ok {
	fmt.Printf("Used: %d bytes\n", v)
}

// Get "quota" dataset property by directly accessing the properties map, while
// also using the zfsprops helper package.
if prop, ok := ds.Properties[zfsprops.Quota]; ok {
	fmt.Printf("Quota: %s bytes\n", prop.Value)
}
```

```
Name: tank/my-data
Used: 150470656 bytes
Quota: 1073741824 bytes
```
