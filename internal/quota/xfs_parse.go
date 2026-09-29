package quota

import (
	"strconv"
	"strings"
)

func projectQuotaEnforced(output string) bool {
	inProjectSection := false
	accounting, enforcement := false, false
	for line := range strings.SplitSeq(output, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "Project quota state") {
			inProjectSection = true
			continue
		}
		if strings.HasSuffix(line, "quota state") || strings.Contains(line, "quota state on ") {
			inProjectSection = false
		}
		if !inProjectSection {
			continue
		}
		accounting = accounting || line == "Accounting: ON"
		enforcement = enforcement || line == "Enforcement: ON"
	}
	return accounting && enforcement
}

func projectLimitPresent(output string, projectID uint32, maxBytes int64) bool {
	if maxBytes <= 0 {
		return false
	}
	requiredBlocks := (uint64(maxBytes) + 1023) / 1024
	for line := range strings.SplitSeq(output, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}
		reportedID, err := strconv.ParseUint(strings.TrimPrefix(fields[0], "#"), 10, 32)
		if err != nil || reportedID != uint64(projectID) {
			continue
		}
		hardBlocks, err := strconv.ParseUint(fields[3], 10, 64)
		if err == nil && hardBlocks == requiredBlocks {
			return true
		}
	}
	return false
}
