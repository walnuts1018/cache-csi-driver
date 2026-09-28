package v1alpha1

import "testing"

func TestCacheClassSpecValidateSharingPolicy(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name          string
		sharingPolicy SharingPolicy
		wantErr       bool
	}{
		{name: "server default", sharingPolicy: ""},
		{name: "shared", sharingPolicy: SharingPolicyShared},
		{name: "exclusive", sharingPolicy: SharingPolicyExclusive},
		{name: "unknown", sharingPolicy: SharingPolicy("Unknown"), wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			gotErr := (CacheClassSpec{SharingPolicy: test.sharingPolicy}).Validate()
			if (gotErr != nil) != test.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %t", gotErr, test.wantErr)
			}
		})
	}
}
