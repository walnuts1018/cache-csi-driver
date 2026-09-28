package kube

import (
	"context"
	"errors"
	"io"
	"net"
	"syscall"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const testCacheClassesResource = "cacheclasses"

func TestIsTemporaryAPIError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "service unavailable", err: apierrors.NewServiceUnavailable("temporarily unavailable"), want: true},
		{name: "request timeout", err: apierrors.NewTimeoutError("request timed out", 0), want: true},
		{name: "wrapped too many requests", err: errors.Join(errors.New("resolver request failed"), apierrors.NewTooManyRequests("rate limited", 1)), want: true},
		{name: "network timeout", err: &net.DNSError{IsTimeout: true, Err: "resolver timeout"}, want: true},
		{name: "API connection refused", err: &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}, want: true},
		{name: "deadline exceeded", err: context.DeadlineExceeded, want: true},
		{name: "unexpected EOF", err: io.ErrUnexpectedEOF, want: true},
		{name: "not found", err: apierrors.NewNotFound(schema.GroupResource{Resource: testCacheClassesResource}, "missing"), want: false},
		{name: "forbidden", err: apierrors.NewForbidden(schema.GroupResource{Resource: testCacheClassesResource}, "restricted", errors.New("access denied")), want: false},
		{name: "request canceled", err: context.Canceled, want: false},
		{name: "DNS name not found", err: &net.DNSError{IsNotFound: true, Err: "no such host"}, want: false},
		{name: "unknown API status", err: apierrors.NewGenericServerResponse(499, "get", schema.GroupResource{Resource: testCacheClassesResource}, "", "client closed request", 0, false), want: false},
		{name: "unrelated Kubernetes status", err: apierrors.NewBadRequest("invalid request"), want: false},
		{name: "ordinary error", err: errors.New("invalid CacheClass configuration"), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := IsTemporaryAPIError(tt.err); got != tt.want {
				t.Fatalf("IsTemporaryAPIError(%v) = %t, want %t", tt.err, got, tt.want)
			}
		})
	}
}
