package zfs_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/krystal/go-runner"
	"github.com/krystal/go-zfs"
	"github.com/krystal/go-zfs/zfsprops"
	"github.com/krystal/go-zfs/zpoolprops"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_poolCreateGetDestroy(t *testing.T) {
	useZFS(t)
	ctx := context.Background()
	z := newZFSManager(t)

	poolName := nextTestPoolName()
	dir := t.TempDir()
	vdevs := []string{
		mkTempFile(t, dir, int64(512*1024*1024)), // 512 MiB
		mkTempFile(t, dir, int64(512*1024*1024)), // 512 MiB
	}

	t.Logf("creating ZFS test pool %s, backed by: %+v", poolName, vdevs)
	err := z.CreatePool(ctx, &zfs.CreatePoolOptions{
		Name: poolName,
		FilesystemProperties: map[string]string{
			"canmount": "off",
		},
		Mountpoint: "none",
		Vdevs:      vdevs,
	})
	require.NoError(t, err)

	pool, err := z.GetPool(ctx, poolName)
	assert.NoError(t, err)
	assert.Equal(t, poolName, pool.Name)

	err = z.DestroyPool(ctx, poolName, true)
	require.NoError(t, err)

	_, err = z.GetPool(ctx, poolName)
	assert.Error(t, err)
}

func TestIntegration_poolGetList(t *testing.T) {
	ctx := context.Background()
	z := newZFSManager(t)

	poolName1, _ := createTestPool(t, z)
	poolName2, _ := createTestPool(t, z)

	pool1, err := z.GetPool(ctx, poolName1)
	require.NoError(t, err)
	assert.Equal(t, poolName1, pool1.Name)

	pool2, err := z.GetPool(ctx, poolName2)
	require.NoError(t, err)
	assert.Equal(t, poolName2, pool2.Name)

	pools, err := z.ListPools(ctx)
	require.NoError(t, err)
	names := make([]string, len(pools))
	for i, p := range pools {
		names[i] = p.Name
	}
	assert.Contains(t, names, pool1.Name)
	assert.Contains(t, names, pool2.Name)

	names, err = z.ListPoolNames(ctx)
	require.NoError(t, err)
	assert.Contains(t, names, pool1.Name)
	assert.Contains(t, names, pool2.Name)
}

func TestIntegration_poolSetAndGetProperties(t *testing.T) {
	ctx := context.Background()
	z := newZFSManager(t)
	poolName, _ := createTestPool(t, z)

	current, err := z.GetPoolProperty(ctx, poolName, zpoolprops.Delegation)
	require.NoError(t, err)

	newVal := "on"
	if newVal == current {
		newVal = "off"
	}
	err = z.SetPoolProperty(ctx, poolName, zpoolprops.Delegation, newVal)
	require.NoError(t, err)

	current, err = z.GetPoolProperty(ctx, poolName, zpoolprops.Delegation)
	require.NoError(t, err)

	assert.Equal(t, newVal, current)
}

func TestIntegration_poolExportImport(t *testing.T) {
	ctx := context.Background()
	z := newZFSManager(t)

	poolName, dir := createTestPool(t, z)

	pool, err := z.GetPool(ctx, poolName)
	require.NoError(t, err)
	assert.Equal(t, poolName, pool.Name)

	err = z.ExportPool(ctx, poolName, true)
	require.NoError(t, err)

	_, err = z.GetPool(ctx, poolName)
	require.Error(t, err)

	err = z.ImportPool(ctx, &zfs.ImportPoolOptions{
		Name:        poolName,
		DirOrDevice: []string{dir},
	})
	require.NoError(t, err)

	pool, err = z.GetPool(ctx, poolName)
	require.NoError(t, err)
	assert.Equal(t, poolName, pool.Name)
}

func TestIntegration_datasetCreateGetDestroy(t *testing.T) {
	ctx := context.Background()
	z := newZFSManager(t)
	poolName, _ := createTestPool(t, z)
	datasetName := zfs.Join(poolName, t.Name(), "test")

	err := z.CreateDataset(ctx, &zfs.CreateDatasetOptions{
		Name: datasetName,
		Properties: map[string]string{
			zfsprops.CanMount: "off",
		},
		CreateParents: true,
	})
	require.NoError(t, err)

	ds, err := z.GetDataset(ctx, datasetName)
	assert.NoError(t, err)
	assert.Equal(t, datasetName, ds.Name)

	err = z.DestroyDataset(ctx, datasetName, zfs.DestroyForceUnmount)
	require.NoError(t, err)

	_, err = z.GetDataset(ctx, datasetName)
	assert.Error(t, err)
}

func TestIntegration_datasetCreateGetDestroyMounted(t *testing.T) {
	ctx := context.Background()
	z := newZFSManager(t)
	poolName, _ := createTestPool(t, z)
	level0Name := zfs.Join(poolName, "level-0")
	level0Mountpoint := t.TempDir()

	err := z.CreateDataset(ctx, &zfs.CreateDatasetOptions{
		Name: level0Name,
		Properties: map[string]string{
			zfsprops.CanMount:   "on",
			zfsprops.Mountpoint: level0Mountpoint,
		},
		CreateParents: true,
	})
	require.NoError(t, err)

	ds, err := z.GetDataset(ctx, level0Name)
	require.NoError(t, err)

	gotMountpoint, ok := ds.Mountpoint()
	assert.True(t, ok)
	assert.Equal(t, level0Mountpoint, gotMountpoint)

	level1Name := zfs.Join(ds.Name, "level-1")
	level1Mountpoint := filepath.Join(level0Mountpoint, "level-1")

	level2Name := zfs.Join(level1Name, "level-2")
	level2Mountpoint := filepath.Join(level1Mountpoint, "level-2")
	assert.NoDirExists(t, level2Mountpoint)

	err = z.CreateDataset(ctx, &zfs.CreateDatasetOptions{
		Name:          level2Name,
		CreateParents: true,
	})
	require.NoError(t, err)
	assert.DirExists(t, level2Mountpoint)

	level2, err := z.GetDataset(ctx, level2Name)
	require.NoError(t, err)

	gotMountpoint, ok = level2.Mountpoint()
	assert.True(t, ok)
	assert.Equal(t, level2Mountpoint, gotMountpoint)

	level0File := filepath.Join(level0Mountpoint, "hello.txt")
	err = os.WriteFile(level0File, []byte("hello dataset"), 0o600)
	require.NoError(t, err)

	level2File := filepath.Join(level2Mountpoint, "hello.txt")
	err = os.WriteFile(level2File, []byte("hello child"), 0o600)
	require.NoError(t, err)

	err = z.DestroyDataset(
		ctx, level1Name, zfs.DestroyRecursive, zfs.DestroyForceUnmount,
	)
	require.NoError(t, err)
	assert.NoDirExists(t, level1Mountpoint)

	assert.DirExists(t, level0Mountpoint)
	assert.FileExists(t, level0File)
	err = z.DestroyDataset(
		ctx, level0Name, zfs.DestroyRecursive, zfs.DestroyForceUnmount,
	)
	require.NoError(t, err)

	// File should be gone, as the ZFS dataset is destroyed.
	assert.NoFileExists(t, level0File)

	// The mountpoint directory should still exist however.
	assert.DirExists(t, level0Mountpoint)
}

func TestIntegration_datasetGetList(t *testing.T) {
	ctx := context.Background()
	z := newZFSManager(t)
	poolName, _ := createTestPool(t, z)
	datasetName1 := zfs.Join(poolName, t.Name(), "test1")
	datasetName2 := zfs.Join(poolName, t.Name(), "test2")

	err := z.CreateDataset(ctx, &zfs.CreateDatasetOptions{
		Name: datasetName1,
		Properties: map[string]string{
			zfsprops.CanMount: "off",
		},
		CreateParents: true,
	})
	require.NoError(t, err)

	err = z.CreateDataset(ctx, &zfs.CreateDatasetOptions{
		Name: datasetName2,
		Properties: map[string]string{
			zfsprops.CanMount: "off",
		},
		CreateParents: true,
	})
	require.NoError(t, err)

	dataset1, err := z.GetDataset(ctx, datasetName1)
	require.NoError(t, err)
	assert.Equal(t, datasetName1, dataset1.Name)
	canMount1, ok := dataset1.CanMount()
	assert.True(t, ok)
	assert.False(t, canMount1)

	dataset2, err := z.GetDataset(ctx, datasetName2)
	require.NoError(t, err)
	assert.Equal(t, datasetName2, dataset2.Name)
	canMount2, ok := dataset2.CanMount()
	assert.True(t, ok)
	assert.False(t, canMount2)

	datasets, err := z.ListDatasets(ctx, poolName, 0, zfs.FilesystemType)
	require.NoError(t, err)
	names := make([]string, len(datasets))
	for i, ds := range datasets {
		names[i] = ds.Name
	}
	assert.Contains(t, names, dataset1.Name)
	assert.Contains(t, names, dataset2.Name)

	names, err = z.ListDatasetNames(ctx, poolName, 0, zfs.FilesystemType)
	require.NoError(t, err)
	assert.Contains(t, names, dataset1.Name)
	assert.Contains(t, names, dataset2.Name)
}

func TestIntegration_datasetSetGetInheritProperties(t *testing.T) {
	ctx := context.Background()
	z := newZFSManager(t)
	poolName, _ := createTestPool(t, z)
	datasetName := zfs.Join(poolName, t.Name(), "test")

	err := z.CreateDataset(ctx, &zfs.CreateDatasetOptions{
		Name: datasetName,
		Properties: map[string]string{
			zfsprops.CanMount: "off",
		},
		CreateParents: true,
	})
	require.NoError(t, err)

	current, err := z.GetDatasetProperty(ctx, datasetName, zfsprops.Atime)
	require.NoError(t, err)
	assert.Equal(t, "off", current)

	err = z.SetDatasetProperty(ctx, datasetName, zfsprops.Atime, "on")
	require.NoError(t, err)

	current, err = z.GetDatasetProperty(ctx, datasetName, zfsprops.Atime)
	require.NoError(t, err)
	assert.Equal(t, "on", current)

	err = z.InheritDatasetProperty(ctx, datasetName, zfsprops.Atime, true)
	require.NoError(t, err)

	current, err = z.GetDatasetProperty(ctx, datasetName, zfsprops.Atime)
	require.NoError(t, err)
	assert.Equal(t, "off", current)
}

func TestIntegration_datasetUserProperties(t *testing.T) {
	ctx := context.Background()
	z := newZFSManager(t)
	poolName, _ := createTestPool(t, z)
	datasetName := zfs.Join(poolName, t.Name(), "test")

	err := z.CreateDataset(ctx, &zfs.CreateDatasetOptions{
		Name: datasetName,
		Properties: map[string]string{
			zfsprops.CanMount: "off",
		},
		CreateParents: true,
	})
	require.NoError(t, err)

	propName := "com.github.krystal.go-zfs:test_prop"

	current, err := z.GetDatasetProperty(ctx, datasetName, propName)
	require.NoError(t, err)
	assert.Equal(t, "-", current)

	err = z.SetDatasetProperty(ctx, datasetName, propName, "echo 123")
	require.NoError(t, err)

	current, err = z.GetDatasetProperty(ctx, datasetName, propName)
	require.NoError(t, err)
	assert.Equal(t, "echo 123", current)

	err = z.InheritDatasetProperty(ctx, datasetName, propName, true)
	require.NoError(t, err)

	current, err = z.GetDatasetProperty(ctx, datasetName, propName)
	require.NoError(t, err)
	assert.Equal(t, "-", current)
}

//
// Helpers
//

const testPoolNamePrefix = "go-zfs-tests-"

var (
	testRunID string
	poolID    uint64
	mux       sync.Mutex
)

func newZFSManager(t *testing.T) *zfs.Manager {
	t.Helper()

	z := zfs.New()
	r := z.Runner
	z.Runner = &runner.Testing{
		Runner:   r,
		TestingT: t,
	}

	return z
}

func mkTempFile(t *testing.T, dir string, size int64) string {
	t.Helper()

	f, err := os.CreateTemp(dir, "")
	if err != nil {
		t.Fatal(err)
	}

	err = f.Truncate(size)
	if err != nil {
		t.Fatal(err)
	}

	return f.Name()
}

func useZFS(t *testing.T) {
	t.Helper()

	if v := os.Getenv("USE_ZFS"); v != "" {
		var err error
		v, err := strconv.ParseBool(v)
		if err != nil {
			panic(fmt.Errorf(
				"failed to parse USE_ZFS environment variable: %w", err,
			))
		}

		if v {
			return
		}
	}
	t.Skip("ZFS-backed tests are disabled, set USE_ZFS=1 to enable")
}

func nextTestPoolName() string {
	mux.Lock()
	defer mux.Unlock()
	if testRunID == "" {
		testRunID = randString(8)
	}
	poolID++
	poolName := fmt.Sprintf("%s%s-%d", testPoolNamePrefix, testRunID, poolID)

	return poolName
}

func createTestPool(
	t *testing.T,
	poolMgr *zfs.Manager,
) (poolName string, dir string) {
	t.Helper()
	useZFS(t)

	ctx := context.Background()
	poolName = nextTestPoolName()
	dir = t.TempDir()

	vdevs := []string{
		mkTempFile(t, dir, int64(1024*1024*1024)), // 1 GiB
		mkTempFile(t, dir, int64(1024*1024*1024)), // 1 GiB
	}

	t.Logf("creating ZFS test pool %s, backed by: %+v", poolName, vdevs)
	err := poolMgr.CreatePool(ctx, &zfs.CreatePoolOptions{
		Name: poolName,
		FilesystemProperties: map[string]string{
			zfsprops.Atime:    "off",
			zfsprops.CanMount: "off",
		},
		Mountpoint: "none",
		Vdevs:      vdevs,
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		err := poolMgr.DestroyPool(ctx, poolName, true)
		require.NoError(t, err)
	})

	return poolName, dir
}

const randAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
	"abcdefghijklmnopqrstuvwxyz" +
	"0123456789"

func randString(n int) string {
	l := big.NewInt(int64(len(randAlphabet)))
	b := make([]byte, n)
	for i := 0; i < n; i++ {
		index, err := rand.Int(rand.Reader, l)
		if err != nil {
			panic(err)
		}
		b[i] = randAlphabet[index.Int64()]
	}

	return string(b)
}
