# The Rindle topology for this app — the ONE topology (design 214): a `rindle-replicator`
# write-master + one `rindled` read-follower. `followers = 1` is the colocated pair, the smallest
# shape; raise it for more read replicas.
#
# A topology is a plain record of inputs; `rindle up` (local dev) merges it onto its embedded
# topology library and supervises the whole fleet — nothing to import or
# install. LOCAL dev reads THIS file: bare `rindle up` renders it (`.rindle/*.json` + `rindle.json`)
# and supervises the pair (see package.json `dev:daemon`). Ports are allocated PER PROJECT, so
# several Rindle projects (or several worktrees of this one) can run at once without contending;
# the CLI remembers this project's block in `~/.rindle/ports.json`. Offsets inside the block:
#
#   • follower-0 (private `rindled`): http portBase+0  ·  ws portBase+1
#   • replicator (write-master):      http portBase+11 ·  fan-out ws portBase+10  (loopback)
#   • fleet edge (stable app entry):  http/ws portBase+50 → follower-0
#
# `rindle render` prints the resolved URLs, and `rindle.json` `bindings` carries them. Set
# `portBase` below to pin the block instead.
#
# `rindle exec` derives the web tier's bindings: reads/subscriptions enter through the fleet edge and
# writes enter through the master. The same bindings work unchanged when `followers` increases.
{
  profile = "replicated",
  app = "issue-tracker",
  followers = 1,
}
