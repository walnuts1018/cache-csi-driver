//go:build linux

package driver

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/walnuts1018/cache-csi-driver/internal/cache"
	"golang.org/x/sys/unix"
)

// TestMountUsesOpenTreeMountSetattrMoveMountはdriverが使用するLinux mount APIを実際に呼び出して検証する。通常のunit testではmount権限がない環境をskipし、専用integration taskではCACHE_CSI_REQUIRE_MOUNT_APIで実mountを必須にする。
func TestMountUsesOpenTreeMountSetattrMoveMount(t *testing.T) {
	requireLinuxMountAPI(t)

	root := t.TempDir()
	source := filepath.Join(root, "source")
	target := filepath.Join(root, "target")
	if err := os.Mkdir(source, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "entry"), []byte("cache data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := mount(source, target, true, true); err != nil {
		if errors.Is(err, unix.EPERM) || errors.Is(err, unix.EACCES) || errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EOPNOTSUPP) {
			if os.Getenv("CACHE_CSI_REQUIRE_MOUNT_API") == "1" {
				t.Fatalf("required Linux mount API integration failed: %v", err)
			}
			t.Skipf("open_tree/mount_setattr/move_mount integration requires Linux mount API support and CAP_SYS_ADMIN: %v", err)
		}
		t.Fatalf("mount cache with the Linux mount API: %v", err)
	}
	t.Cleanup(func() {
		if err := unmount(target); err != nil && !errors.Is(err, unix.EINVAL) && !errors.Is(err, unix.ENOENT) {
			t.Errorf("unmount integration target: %v", err)
		}
	})

	same, err := sameCacheMount(source, target, true, true)
	if err != nil {
		t.Fatalf("inspect mounted cache: %v", err)
	}
	if !same {
		t.Fatal("mounted cache source or required mount attributes did not match")
	}
	readOnly, err := filesystemReadOnly(target)
	if err != nil {
		t.Fatalf("inspect readonly mount attribute: %v", err)
	}
	if !readOnly {
		t.Fatal("mount is not readonly")
	}
	if _, err := os.Create(filepath.Join(target, "unexpected-write")); !errors.Is(err, unix.EROFS) {
		t.Fatalf("write through readonly mount: got %v, want %v", err, unix.EROFS)
	}
}

func TestRecoverPreparingLeaseAfterDriverRestartWithAttachedMount(t *testing.T) {
	requireLinuxMountAPI(t)
	root := t.TempDir()
	storeRoot := filepath.Join(root, "store")
	target := filepath.Join(root, "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := cache.NewStore(storeRoot, cache.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	openStore := store
	t.Cleanup(func() {
		if openStore != nil {
			if err := openStore.Close(); err != nil {
				t.Errorf("close cache Store: %v", err)
			}
		}
	})
	identity, err := cache.Identity("namespace-uid", "default", "class-uid", "mount-recovery", "v1")
	if err != nil {
		t.Fatal(err)
	}
	const leaseID = "mount-recovery-volume"
	source, created, err := store.Acquire(cache.AcquireOptions{
		Identity: identity,
		Lease: cache.Lease{
			ID:        leaseID,
			Target:    target,
			Namespace: "default",
			PodName:   "mount-recovery",
			PodUID:    "mount-recovery-pod",
			ReadOnly:  true,
			NoExec:    true,
		},
		Policy: cache.Policy{SharingPolicy: cache.SharingPolicyShared, NoExec: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("initial mount recovery publish did not create a lease")
	}
	source, err = store.Expose(identity)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "payload"), []byte("cache data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := mount(source, target, true, true); err != nil {
		if mountAPISkipError(t, err) {
			return
		}
		t.Fatalf("attach cache mount before simulated restart: %v", err)
	}
	mounted := true
	t.Cleanup(func() {
		if mounted {
			if err := unmount(target); err != nil {
				t.Errorf("unmount recovered cache: %v", err)
			}
		}
	})
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	openStore = nil

	restartedStore, err := cache.NewStore(storeRoot, cache.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	openStore = restartedStore
	if err := recoverCacheLeases(restartedStore, newMounter()); err != nil {
		t.Fatalf("recover preparing lease with a real mount: %v", err)
	}
	_, lease, recoveredSource, _, found, err := restartedStore.LeaseDetails(leaseID)
	if err != nil || !found || lease.Preparing {
		t.Fatalf("recovered lease = (%+v, %t, %v), want a committed lease", lease, found, err)
	}
	if recoveredSource != source {
		t.Fatalf("recovered source = %q, want %q", recoveredSource, source)
	}
	if data, err := os.ReadFile(filepath.Join(target, "payload")); err != nil || string(data) != "cache data" {
		t.Fatalf("read recovered cache mount = (%q, %v), want cache data", data, err)
	}
	if err := unmount(target); err != nil {
		t.Fatal(err)
	}
	mounted = false
}

func requireLinuxMountAPI(t *testing.T) {
	t.Helper()
	if err := PreflightMountAPI(); err != nil {
		if mountAPISkipError(t, err) {
			return
		}
		t.Fatalf("Linux mount API preflight failed: %v", err)
	}
}

func mountAPISkipError(t *testing.T, err error) bool {
	t.Helper()
	if !errors.Is(err, unix.EPERM) && !errors.Is(err, unix.EACCES) && !errors.Is(err, unix.ENOSYS) && !errors.Is(err, unix.EOPNOTSUPP) {
		return false
	}
	if os.Getenv("CACHE_CSI_REQUIRE_MOUNT_API") == "1" {
		t.Fatalf("required Linux mount API integration failed: %v", err)
	}
	t.Skipf("open_tree/mount_setattr/move_mount integration requires Linux mount API support and CAP_SYS_ADMIN: %v", err)
	return true
}
