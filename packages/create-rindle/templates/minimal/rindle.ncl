# The Rindle topology for this app — the ONE topology (design 214): a `rindle-replicator`
# write-master + one `rindled` read-follower. `followers = 1` is the colocated pair, the smallest
# shape; raise it for more read replicas.
#
# A topology is a plain record of inputs; `rindle dev` (local) and `rindle deploy` / `rindle render`
# (cloud) merge it onto the topology library embedded in the `rindle` binary — nothing to import
# or install. LOCAL dev reads THIS file: `rindle dev` renders it and supervises the pair (see the
# package.json `dev` script). The same file drives the cloud:
#
#   rindle login                  # once — authenticate to Rindle Cloud
#   rindle deploy                 # provision / re-attach the managed app (writes .rindle/cloud.json)
#   rindle migrate apply --remote # push migrations/*.sql to the deployed write-master
#
# `app` becomes the cloud display name and groups every component under one app in `rindle ps`.
# `followers = 1` maps to the cloud `replicated` rung (a separate write-master + one follower,
# backed up; scales 1→N in place); `followers > 1` maps to `read-scaled`. `localRetention = true`
# maps to `colocated` (one box, both processes, no streaming backup).
#
# Every follower count renders a local fleet edge;
# FOLLOWER-AFFINITY-DESIGN.md §10). `rindle dev` injects the server's unified connection from this
# file; browser query leases carry the public ws endpoint + placement ticket. Changing
# `followers = 1` to a wider fleet therefore requires no application configuration change.
# Set `RINDLE_DEV_CLIENT_REGION=A` to simulate a client region; the edge always mints an affinity
# placement ticket, with a fleet of one resolving trivially to its only follower.
{
  profile = "replicated",
  app = "__PROJECT_NAME__",
  followers = 1,
}
