# Rindle plugin for Claude Code

Teaches Claude Code to build [Rindle](https://rindle.sh) apps — local-first,
synced apps where live queries are kept exact by incremental view maintenance.

## What it ships

| Component | Name | What it does |
| --- | --- | --- |
| Skill | `rindle:building-rindle-apps` | The full canon: schema/migrations, isomorphic mutators, named queries + fragments, client + API wiring, with 8 reference files of working code |
| Skill | `rindle:quickstart` | Zero → running three-tier app via `npm create rindle@latest` |
| Command | `/rindle:quickstart [app-name]` | Explicit entry point for the above |
| Agent | `rindle-reviewer` | Audits app code against the correctness canon (deterministic mutators, named-query wire rule, token hygiene, …) |
| Hook | `PreToolUse` lint | Blocks removed APIs (`fromShared`) and non-deterministic mutator bodies before they're written; no-ops outside Rindle code |

## Install

From the repo (the marketplace manifest at the **repo root** points here):

```
/plugin marketplace add rindle-sh/rindle
/plugin install rindle@rindle
```

From a local checkout — either path works, since this directory carries its own
manifest too:

```
/plugin marketplace add .                # the root manifest → ./claude-plugin
/plugin marketplace add ./claude-plugin  # this directory directly
/plugin install rindle@rindle
```

There are therefore **two** marketplace manifests, and they describe the same
plugin: `.claude-plugin/marketplace.json` at the repo root (`source:
"./claude-plugin"`) is what `/plugin marketplace add <owner>/<repo>` resolves,
and `claude-plugin/.claude-plugin/marketplace.json` (`source: "./"`) serves the
local path and becomes the root manifest if this directory is ever split into its
own repo. `product-page/tests/plugin-manifest.test.ts` fails the build if they
drift on anything but `source`.

Community-marketplace listing (self-serve, gives users a no-setup
`rindle@claude-plugins-community`): submit at
<https://console.anthropic.com/plugins/submit>. The `claude-plugins-official`
marketplace is curated by Anthropic — partnership contact, no public form.

## Develop

```sh
claude --plugin-dir ./claude-plugin      # run a session with the local plugin
claude plugin validate ./claude-plugin   # schema/frontmatter/hooks checks
/reload-plugins                          # pick up edits mid-session
```

The `building-rindle-apps` skill is the packaged twin of
`.claude/skills/building-rindle-apps` in the monorepo — when that skill
changes, re-copy it here (and bump `version` in `.claude-plugin/plugin.json`;
users only receive updates on a version bump).

## Versioning

`version` in `.claude-plugin/plugin.json` is the distribution knob: bump it on
every user-visible change. Marketplace entries can override it, but keeping the
manifest authoritative is simpler.
