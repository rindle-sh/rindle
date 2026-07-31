# The only supported data topology: a rindle-replicator write-master and a rindled follower.
# One follower is the smallest local pair; apps use the stable fleet edge (portBase+50) for reads/ws
# and the write-master (portBase+11). Those bindings stay unchanged as followers are added.
# The port block is allocated per project so several Rindle projects can run at once; `rindle render`
# prints the resolved URLs. Set `portBase` here to pin it.
{
  profile = "replicated",
  app = "rindle-todo-mvc",
  followers = 1,
}
