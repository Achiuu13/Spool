# Deploying Spool on Kubernetes

`spool.yaml` runs the three-node replicated cluster from
[docs/replication.md](../../docs/replication.md) as a StatefulSet:

- **Stable identity**: pods `spool-0/1/2` get fixed DNS names through the
  headless Service, which is what the static `-peers` list and the leader
  redirect (`-advertise`) URLs are built from.
- **Persistent volumes**: `volumeClaimTemplates` give each node its own
  disk for the replicated operation log. A deleted or rescheduled pod
  reattaches its volume, replays its log, and rejoins as a follower.
- **Probes**: liveness is "the process serves HTTP"; readiness is "this
  node knows an elected leader", so a node cut off from the quorum drops
  out of the client Service instead of serving stale answers or 503s.
- **Parallel pod management**: all three nodes must be up to elect a
  leader, so pods start together rather than gated on each other's
  readiness.

## Run it (kind or minikube)

```sh
docker build -t spool:latest .

# kind
kind create cluster
kind load docker-image spool:latest

# or minikube
# minikube start && minikube image load spool:latest

kubectl apply -f deploy/kubernetes/spool.yaml
kubectl rollout status statefulset/spool
```

Talk to the cluster. The broker image is distroless (no shell), so use a
throwaway curl pod for in-cluster requests — followers redirect to the
leader's pod DNS name, which only resolves inside the cluster:

```sh
kubectl run curl-client --image=curlimages/curl --restart=Never -- sleep 3600
kubectl wait --for=condition=Ready pod/curl-client

kubectl exec curl-client -- curl -sL -X POST \
  http://spool:8080/streams/orders/records -d '{"key":"k1","value":"hello"}'
```

## Failover demo: delete the leader pod

```sh
# 1. Find the leader.
for i in 0 1 2; do
  kubectl exec curl-client -- curl -s http://spool-$i.spool-headless:8080/raft/status; echo
done

# 2. Publish some records (acked = durable on a majority).

# 3. Kill the leader pod.
kubectl delete pod spool-1   # whichever /raft/status showed as leader

# 4. Within the election timeout the survivors elect a new leader and keep
#    serving; the deleted pod is recreated by the StatefulSet, reattaches
#    its PersistentVolume, replays its operation log, and rejoins as a
#    follower. Every acknowledged record is still readable.
```

This is the Kubernetes rendition of `TestFailoverPreservesAckedRecords`
and `TestKilledNodeRejoinsAndCatchesUp` in `tests/replication_e2e_test.go`,
which run the same scenario against real broker processes on every
`go test ./...`.

## Verified run (kind v0.33.0, Docker 29.5.2, macOS)

The demo above, executed as written: 10 records published through a
follower redirect while `spool-1` led term 3; `kubectl delete pod spool-1`
promoted `spool-2` to leader in term 4 in about a second of wall time
(election itself is bounded by the 300–600ms randomized timeout); all
10 acknowledged records were readable from the new leader; the recreated
`spool-1` reattached `data-spool-1`, replayed its operation log to
`applied=11`, and rejoined as a follower; the cluster accepted new writes
throughout recovery.

## Notes

- The manifests assume the default namespace; the `-peers` URLs are
  namespace-relative (`spool-0.spool-headless`), so deploying to another
  namespace needs no edits as long as all pods share it.
- Client traffic from outside the cluster needs the leader's redirect URLs
  to resolve; front the service with an in-cluster proxy or use
  `kubectl port-forward` against the leader pod for demos.
- Storage class, resources, and replica count are demo-sized; replication
  assumes exactly three nodes.
