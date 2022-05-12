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

## Packages

### [`zfs`](https://pkg.go.dev/github.com/krystal/go-zfs)

The primary package used for interacting with ZFS, through the `*zfs.Manager`
type.

```go
import "github.com/krystal/go-zfs"
```

### [`zfsprops`](https://pkg.go.dev/github.com/krystal/go-zfs/zfsprops)

A helper package which defines a long list of string constants for most native
properties available, based on OpenZFS'
[`zfsprops`](https://openzfs.github.io/openzfs-docs/man/7/zfsprops.7.html)
manpage.

```go
import "github.com/krystal/go-zfs/zfsprops"
```

### [`zpoolprops`](https://pkg.go.dev/github.com/krystal/go-zfs/zpoolprops)

A helper package which defines a long list of string constants for most zpool
properties available, based on OpenZFS'
[`zpoolprops`](https://openzfs.github.io/openzfs-docs/man/7/zpoolprops.7.html)
manpage.

```go
import "github.com/krystal/go-zfs/zpoolprops"
```

## Usage

Create a new `*zfs.Manager` instance to manage ZFS pools and datasets with:

```go
z := zfs.New()
```

Get details for a specific dataset:

```go
ds, err := z.GetDataset(ctx, "tank/my-data")
fmt.Printf("Name: %s\n", ds.Name) // => Name: tank/my-data
```

Get `used` dataset property via `Used()` helper method, which returns the value
as a uint64 of bytes:

```go
if v, ok := ds.Used(); ok {
	fmt.Printf("Used: %d bytes\n", v) // => Used: 150470656 bytes
}
```

Get `used` dataset property by directly accessing the `Properties` map,
returning a `Property` type which has a string `Value`.

```go
if prop, ok := ds.Properties[zfsprops.Used]; ok {
	fmt.Printf("Used: %s bytes\n", prop.Value) // => Used: 150470656 bytes
}
```

Create a new pool, using both `zpoolprops` and `zfsprops` helper packages:

```go
err = z.CreatePool(ctx, &zfs.CreatePoolOptions{
	Name: "scratch",
	Properties: map[string]string{
		zpoolprops.Ashift:                   "12",
		zpoolprops.AutoTrim:                 "on",
		zpoolprops.Feature("async_destroy"): "enabled",
	},
	FilesystemProperties: map[string]string{
		zfsprops.Atime:       "off",
		zfsprops.CanMount:    "on",
		zfsprops.Compression: "lz4",
	},
	Mountpoint: "/mnt/scratch",
	Vdevs:      []string{"mirror", "/dev/sde", "/dev/sdf"},
})
```

Create and get a new dataset:

```go
err = z.CreateDataset(ctx, &zfs.CreateDatasetOptions{
	Name: "scratch/http/cache",
	Properties: map[string]string{
		zfsprops.Compression: "off",
	},
	CreateParents: true,
})

ds, err = z.GetDataset(ctx, "scratch/http/cache")
fmt.Printf("Name: %s\n", ds.Name) // => Name: scratch/http/cache
```

Set dataset quota to 100 GiB and turn on atime:

```go
err = z.SetDatasetProperties(ctx, "scratch/http/cache", map[string]string{
	zfsprops.Quota: "100G",
	zfsprops.Atime: "on",
})
```

## Documentation

Please see the
[Go Reference](https://pkg.go.dev/github.com/krystal/go-zfs#section-documentation)
for documentation and examples.

## License

[MIT](https://github.com/krystal/go-zfs/blob/main/LICENSE)
