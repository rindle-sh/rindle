# The Rindle topology for the forum — the ONE topology (design 214): a `rindle-replicator`
# write-master + one `rindled` read-follower. `followers = 1` is the colocated pair, the smallest
# shape; raise it for more read replicas.
#
# A topology is a plain record of inputs; `rindle up` (local dev) merges it onto its embedded
# topology library and supervises the whole fleet — nothing to import or
# install. LOCAL dev reads THIS file: `server/dev.ts` runs `rindle up` from here, which renders it
# (`.rindle/*.json` + `rindle.json`) and supervises the pair. The rendered ports are fixed:
#
#   • replicator (write-master):  http :7611  ·  fan-out ws :7610  (loopback)
#   • fleet edge (stable app entry): http/ws :7650 → follower-0
#   • follower-0 (private `rindled`): http :7600  ·  ws :7601
#
# The dev harness reads the rendered bindings: app reads/subscriptions enter through the fleet edge
# and writes enter through the master. The bindings do not change when `followers` increases.
{
  profile = "replicated",
  app = "forum",
  followers = 1,
}
