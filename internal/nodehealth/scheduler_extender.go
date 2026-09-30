package nodehealth

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
)

const (
	SchedulerName        = "cache-csi-scheduler"
	schedulerFilterLimit = 4 << 20
)

type schedulerExtenderArgs struct {
	Pod       *corev1.Pod      `json:"pod"`
	Nodes     *corev1.NodeList `json:"nodes,omitempty"`
	NodeNames *[]string        `json:"nodeNames,omitempty"`
}

type schedulerExtenderResult struct {
	Nodes       *corev1.NodeList  `json:"nodes,omitempty"`
	NodeNames   *[]string         `json:"nodeNames,omitempty"`
	FailedNodes map[string]string `json:"failedNodes,omitempty"`
	Error       string            `json:"error,omitempty"`
}

func SchedulerExtenderHandler(apiCache *APICache) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /filter", func(writer http.ResponseWriter, request *http.Request) {
		filterSchedulerNodes(writer, request, apiCache)
	})
	for _, path := range []string{"/healthz", "/readyz"} {
		mux.HandleFunc("GET "+path, func(writer http.ResponseWriter, _ *http.Request) {
			if apiCache == nil || !apiCache.Synced() {
				http.Error(writer, "Kubernetes informer caches are not synchronized", http.StatusServiceUnavailable)
				return
			}
			writer.WriteHeader(http.StatusOK)
		})
	}
	return mux
}

func ServeSchedulerExtender(ctx context.Context, address string, handler http.Handler) error {
	if address == "" || handler == nil {
		return errors.New("scheduler extender address and handler are required")
	}
	server := &http.Server{
		Addr:              address,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	serveError := make(chan error, 1)
	go func() {
		serveError <- server.ListenAndServe()
	}()
	select {
	case err := <-serveError:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
	}

	shutdownContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownContext); err != nil {
		_ = server.Close()
		return fmt.Errorf("shut down scheduler extender: %w", err)
	}
	if err := <-serveError; !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func filterSchedulerNodes(writer http.ResponseWriter, request *http.Request, apiCache *APICache) {
	if apiCache == nil {
		writeSchedulerExtenderResult(writer, schedulerExtenderResult{Error: "cache health Lease client is not configured"})
		return
	}
	request.Body = http.MaxBytesReader(writer, request.Body, schedulerFilterLimit)
	data, err := io.ReadAll(request.Body)
	if err != nil {
		writeSchedulerExtenderResult(writer, schedulerExtenderResult{Error: fmt.Sprintf("read scheduler filter request: %v", err)})
		return
	}
	var args schedulerExtenderArgs
	if err := json.Unmarshal(data, &args); err != nil {
		writeSchedulerExtenderResult(writer, schedulerExtenderResult{Error: fmt.Sprintf("decode scheduler filter request: %v", err)})
		return
	}
	if args.Pod == nil {
		writeSchedulerExtenderResult(writer, schedulerExtenderResult{Error: "scheduler filter request does not contain a Pod"})
		return
	}
	if !podHasCacheCSIVolume(args.Pod) {
		writeSchedulerExtenderResult(writer, schedulerExtenderResult{Nodes: args.Nodes, NodeNames: args.NodeNames})
		return
	}
	if args.Nodes == nil {
		writeSchedulerExtenderResult(writer, schedulerExtenderResult{Error: "scheduler must provide candidate Node objects"})
		return
	}
	leases, err := apiCache.Leases()
	if err != nil {
		writeSchedulerExtenderResult(writer, schedulerExtenderResult{Error: fmt.Sprintf("read cache node health Lease informer cache: %v", err)})
		return
	}
	leaseByNode := make(map[string]*coordinationv1.Lease, len(leases))
	for _, lease := range leases {
		nodeName := lease.Annotations[LeaseNodeNameKey]
		if nodeName == "" || LeaseName(nodeName) != lease.Name {
			continue
		}
		leaseByNode[nodeName] = lease
	}

	now := time.Now()
	filteredNodes := &corev1.NodeList{ListMeta: args.Nodes.ListMeta}
	failedNodes := make(map[string]string)
	for index := range args.Nodes.Items {
		node := &args.Nodes.Items[index]
		if IsSchedulableLease(leaseByNode[node.Name], now) {
			filteredNodes.Items = append(filteredNodes.Items, *node)
			continue
		}
		failedNodes[node.Name] = "cache node health Lease is missing, stale, or unavailable"
	}
	writeSchedulerExtenderResult(writer, schedulerExtenderResult{Nodes: filteredNodes, FailedNodes: failedNodes})
}

func writeSchedulerExtenderResult(writer http.ResponseWriter, result schedulerExtenderResult) {
	writer.Header().Set("Content-Type", "application/json")
	data, err := json.Marshal(result)
	if err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	_, _ = writer.Write(data)
}
