# The only supported data topology: a rindle-replicator write-master and a rindled follower.
# One follower is the smallest local pair; apps use the stable fleet edge on :7650 for reads/ws and
# the write-master on :7611. Those bindings stay unchanged as followers are added.
{
  profile = "replicated",
  app = "rindle-todo-mvc",
  followers = 1,
}
