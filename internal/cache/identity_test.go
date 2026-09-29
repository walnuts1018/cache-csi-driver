package cache

import "testing"

func TestIdentitySeparatesServiceAccountScopes(t *testing.T) {
	t.Parallel()

	namespaceScoped, err := Identity("namespace-uid", "compiler", "class-uid", "go-build", "v1")
	if err != nil {
		t.Fatal(err)
	}
	firstServiceAccount, err := IdentityWithServiceAccount("namespace-uid", "service-account-a", "compiler", "class-uid", "go-build", "v1")
	if err != nil {
		t.Fatal(err)
	}
	secondServiceAccount, err := IdentityWithServiceAccount("namespace-uid", "service-account-b", "compiler", "class-uid", "go-build", "v1")
	if err != nil {
		t.Fatal(err)
	}
	if firstServiceAccount == secondServiceAccount || firstServiceAccount == namespaceScoped || secondServiceAccount == namespaceScoped {
		t.Fatal("cache identities unexpectedly crossed ServiceAccount or namespace scope")
	}
	if _, err := IdentityWithServiceAccount("namespace-uid", "", "compiler", "class-uid", "go-build", "v1"); err == nil {
		t.Fatal("service-account-scoped identity accepted an empty ServiceAccount UID")
	}
}
