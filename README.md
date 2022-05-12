<h1 align="center">
  go-zfs
</h1>

<p align="center">
  <strong>
    Go package that enables ZFS pool and dataset management by wrapping
    <code>zfs</code> and <code>zpool</code> CLI commands.
  </strong>
</p>

<p align="center">
  <a href="https://pkg.go.dev/github.com/krystal/go-zfs">
    <img src="https://img.shields.io/badge/%E2%80%8B-reference-387b97.svg?logo=go&logoColor=white"
  alt="Go Reference">
  </a>
  <a href="https://github.com/krystal/go-zfs/releases">
    <img src="https://img.shields.io/github/v/tag/krystal/go-zfs?label=release" alt="GitHub tag (latest SemVer)">
  </a>
  <a href="https://github.com/krystal/go-zfs/actions">
    <img src="https://img.shields.io/github/workflow/status/krystal/go-zfs/CI.svg?logo=github" alt="Actions Status">
  </a>
  <a href="https://codeclimate.com/github/krystal/go-zfs">
    <img src="https://img.shields.io/codeclimate/coverage/krystal/go-zfs.svg?logo=code%20climate" alt="Coverage">
  </a>
  <a href="https://github.com/krystal/go-zfs/commits/main">
    <img src="https://img.shields.io/github/last-commit/krystal/go-zfs.svg?style=flat&logo=github&logoColor=white"
alt="GitHub last commit">
  </a>
  <a href="https://github.com/krystal/go-zfs/issues">
    <img src="https://img.shields.io/github/issues-raw/krystal/go-zfs.svg?style=flat&logo=github&logoColor=white"
alt="GitHub issues">
  </a>
  <a href="https://github.com/krystal/go-zfs/pulls">
    <img src="https://img.shields.io/github/issues-pr-raw/krystal/go-zfs.svg?style=flat&logo=github&logoColor=white" alt="GitHub pull requests">
  </a>
  <a href="https://github.com/krystal/go-zfs/blob/master/LICENSE">
    <img src="https://img.shields.io/github/license/krystal/go-zfs.svg?style=flat" alt="License Status">
  </a>
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

## Documentation

Please see the
[Go Reference](https://pkg.go.dev/github.com/krystal/go-zfs#section-documentation)
for documentation and examples.

## License

[MIT](https://github.com/krystal/go-zfs/blob/main/LICENSE)
