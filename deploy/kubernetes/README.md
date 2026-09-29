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

Talk to the cluster:

```sh
kubectl port-forward svc/spool 8080:8080 &

# Followers redirect to the leader's pod DNS name, so publish from inside
# the cluster (or port-forward the leader pod directly):
kubectl run client --rm -it --image=curlimages/curl --restart=Never -- \
  sh -c 'curl -sL -X POST http://spool:8080/streams/orders/records -d "{\"value\":\"hello\"}"'
```

## Failover demo: delete the leader pod

```sh
# 1. Find the leader.
for i in 0 1 2; do
  kubectl exec spool-$i -- wget -qO- http://localhost:8080/raft/status
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

## Notes

- The manifests assume the default namespace; the `-peers` URLs are
  namespace-relative (`spool-0.spool-headless`), so deploying to another
  namespace needs no edits as long as all pods share it.
- Client traffic from outside the cluster needs the leader's redirect URLs
  to resolve; front the service with an in-cluster proxy or use
  `kubectl port-forward` against the leader pod for demos.
- Storage class, resources, and replica count are demo-sized; replication
  assumes exactly three nodes.
