# @rindle/affinity

The TypeScript twin of the `rindle-affinity` Rust crate: mint and verify the **follower-affinity
ticket** — the HMAC-signed placement token that pins a browser session to one follower machine so
its live-query ws stream and its api-server-issued lease co-locate. Used by the local dev-edge and
any JS-side verify; the native fleet edge mints and verifies in production.

A ticket is `aff.<base64url(payload)>.<base64url(hmac_sha256(base64url(payload), key))>`. `mint` here
is **byte-for-byte identical** to the crate for the same input (pinned by a shared frozen test
vector), so a Rust-minted ticket verifies here and vice-versa. It is **placement, not
authorization**: a forged ticket only aims a connection at a machine, granting no data access.

Full docs — regional followers and fleet-edge routing:
**[rindle.sh/docs/cloud-scaling](https://rindle.sh/docs/cloud-scaling)** · for agents:
[llms.txt](https://rindle.sh/llms.txt)

```ts
import { mint, verify } from "@rindle/affinity";

const ticket = mint(
  { app, mid, region, sub: clientId, iat: now, exp: now + 3600, gen: 0 },
  process.env.RINDLE_AFFINITY_KEY!,
);

const result = verify(ticket, [currentKey, previousKey], { app, now, minGen: 0 });
if (result.ok) routeTo(result.payload.mid);
```
