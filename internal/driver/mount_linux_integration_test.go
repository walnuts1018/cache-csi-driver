//go:build linux

package driver

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

// TestMountUsesOpenTreeMountSetattrMoveMountはdriverが使用するLinux mount APIを実際に呼び出して検証する。通常のunit testではmount権限がない環境をskipし、専用integration taskではCACHE_CSI_REQUIRE_MOUNT_APIで実mountを必須にする。
func TestMountUsesOpenTreeMountSetattrMoveMount(t *testing.T) {
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
