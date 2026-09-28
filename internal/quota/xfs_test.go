package quota

import "testing"

func TestProjectQuotaEnforced(t *testing.T) {
	state := "User quota state on /cache\n  Accounting: OFF\n  Enforcement: OFF\nProject quota state on /cache\n  Accounting: ON\n  Enforcement: ON"
	if !projectQuotaEnforced(state) {
		t.Fatal("project quota should be recognized as enabled")
	}
	if projectQuotaEnforced("Project quota state on /cache\n  Accounting: ON\n  Enforcement: OFF") {
		t.Fatal("disabled project quota enforcement was accepted")
	}
}

func TestProjectLimitPresent(t *testing.T) {
	report := "Project quota on /cache (/dev/sdb1)\n                        Blocks\nProject ID       Used   Soft   Hard Warn/Grace\n#0                0       0      0 00 [------]\n#10042            4      0  16384 00 [------]"
	if !projectLimitPresent(report, 10042, 16*1024*1024) {
		t.Fatal("configured project hard quota was not found")
	}
	if projectLimitPresent(report, 10042, 17*1024*1024) {
		t.Fatal("smaller hard quota was accepted")
	}
}
