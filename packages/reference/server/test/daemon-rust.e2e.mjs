// The Rust pair (`rindle-replicator` write-master + `rindled` follower) under the topology
// conformance suite (daemon-conformance.mjs) — ONE deployment shape, one wire contract
// (design 214). Builds both binaries via cargo (cheap when warm), boots the pair on
// ephemeral ports with the conformance schema, runs the suite, kills both.

import { CONFORMANCE_TABLES, runDaemonConformance } from "./daemon-conformance.mjs";
import { buildPairBinaries, startPair } from "./spawn-pair.mjs";

buildPairBinaries();
const pair = await startPair({
  tables: CONFORMANCE_TABLES,
  authToken: "secret",
  prefix: "rindled-e2e-",
});

try {
  await runDaemonConformance({
    writeUrl: pair.writeUrl,
    readUrl: pair.readUrl,
    wsUrl: pair.wsUrl,
    authToken: "secret",
  });
  console.log("pair e2e passed (rindle-replicator + rindled)");
} finally {
  pair.cleanup();
}
