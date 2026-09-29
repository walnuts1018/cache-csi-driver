package driver

import (
	"context"
	"errors"

	"github.com/walnuts1018/cache-csi-driver/internal/cache"
)

func IsMountedAt(target string) (bool, error) { return mountedAt(target) }

// VerifyCacheMountはleaseの情報を使ってcache bind mountを検証する。targetが不明な場合はsourceがmountされているか確認する。
func VerifyCacheMount(source string, lease cache.Lease, policy cache.Policy) (bool, error) {
	return verifyCacheMount(newMounter(), source, lease, policy)
}

func verifyCacheMount(mount mounter, source string, lease cache.Lease, _ cache.Policy) (bool, error) {
	if lease.Target == "" {
		return mount.sourceMounted(source)
	}
	return mount.sameCacheMount(source, lease.Target, lease.ReadOnly, lease.NoExec)
}

func RecoverCacheLeases(store *cache.Store) error { return recoverCacheLeases(store, newMounter()) }

func recoverCacheLeases(store *cache.Store, mount mounter) error {
	return store.RecoverLeases(func(source string, lease cache.Lease, policy cache.Policy) (bool, error) {
		return verifyCacheMount(mount, source, lease, policy)
	})
}

func ApplyQuota(ctx context.Context, store *cache.Store, quotaManager ProjectQuota, policy cache.Policy, identity, source string) error {
	if !policy.QuotaEnabled {
		return nil
	}
	if quotaManager == nil {
		return errors.New("XFS project quota support is unavailable")
	}
	projectID, assignProject, setLimit, err := store.QuotaState(identity, policy.MaxBytes)
	if err != nil {
		return err
	}
	if assignProject || setLimit {
		if err := quotaManager.Configure(ctx, store.Root(), source, projectID, policy.MaxBytes); err != nil {
			return err
		}
	}
	if assignProject {
		if err := store.MarkProjectAssigned(identity); err != nil {
			return err
		}
	}
	if setLimit {
		if err := store.MarkQuotaApplied(identity, policy.MaxBytes); err != nil {
			return err
		}
	}
	return nil
}
