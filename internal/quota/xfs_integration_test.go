//go:build linux

package quota

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

//nolint:paralleltest // このテストは共有loopback XFSと固定project IDを使うため並列実行しない。
func TestXFSProjectQuotaEnforcesHardLimit(t *testing.T) {
	if os.Getenv("CACHE_CSI_REQUIRE_XFS_QUOTA") != "1" {
		t.Skip("XFS project quota integration runs only in the dedicated loopback filesystem task")
	}
	root := os.Getenv("CACHE_CSI_XFS_ROOT")
	if root == "" {
		t.Fatal("CACHE_CSI_XFS_ROOT must point to the loopback XFS mount")
	}
	var filesystem unix.Statfs_t
	if err := unix.Statfs(root, &filesystem); err != nil {
		t.Fatal(err)
	}
	if filesystem.Type != xfsSuperMagic {
		t.Fatalf("required XFS quota integration is running on filesystem type %#x", filesystem.Type)
	}

	generationPath := filepath.Join(root, "cache-csi-quota-integration")
	if err := os.Mkdir(generationPath, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(generationPath); err != nil {
			t.Errorf("remove XFS quota test directory: %v", err)
		}
	})
	const projectID = uint32(2_100_000_001)
	const maxBytes = int64(1 << 20)
	if err := (XFS{}).Configure(t.Context(), root, generationPath, projectID, maxBytes); err != nil {
		t.Fatalf("configure required XFS project quota: %v", err)
	}
	if err := os.Chmod(generationPath, 0o777); err != nil {
		t.Fatal(err)
	}

	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(t.Context(), executable, "-test.run=^TestXFSQuotaWriteHelper$")
	command.Env = append(os.Environ(), "CACHE_CSI_XFS_WRITE_PATH="+filepath.Join(generationPath, "over-limit"))
	command.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: 65534, Gid: 65534, NoSetGroups: true}}
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("write beyond XFS project hard limit: %v: %s", err, output)
	}
	if strings.TrimSpace(string(output)) != "PASS" {
		t.Fatalf("quota write helper returned unexpected output: %s", output)
	}
	info, err := os.Stat(filepath.Join(generationPath, "over-limit"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() < maxBytes-(64<<10) || info.Size() > maxBytes {
		t.Fatalf("quota-limited file size = %d bytes, want between %d and %d", info.Size(), maxBytes-(64<<10), maxBytes)
	}
}

//nolint:paralleltest // このhelperは上位の共有XFS integration testから再実行される。
func TestXFSQuotaWriteHelper(t *testing.T) {
	path := os.Getenv("CACHE_CSI_XFS_WRITE_PATH")
	if path == "" {
		t.Skip("quota write helper is invoked by the XFS integration test")
	}
	payload, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	block := make([]byte, 64<<10)
	var writeErr error
	for range 128 {
		if _, writeErr = payload.Write(block); writeErr != nil {
			break
		}
	}
	if writeErr == nil {
		writeErr = payload.Sync()
	}
	if closeErr := payload.Close(); closeErr != nil {
		writeErr = errors.Join(writeErr, closeErr)
	}
	if !errors.Is(writeErr, unix.EDQUOT) && !errors.Is(writeErr, unix.ENOSPC) {
		t.Fatalf("write beyond project hard limit error = %v, want EDQUOT or ENOSPC", writeErr)
	}
}
