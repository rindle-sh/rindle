---
description: Scaffold and boot a new Rindle app (local-first, synced, three tiers)
argument-hint: "[app-name]"
---

Create a new Rindle app using the `rindle:quickstart` skill.

App name / extra instructions from the user: $ARGUMENTS

Follow the quickstart skill's steps exactly: scaffold with
`npm create rindle@latest`, boot with `pnpm dev`, verify the daemon and the
optimistic→synced write loop work, and only then start making the user's
requested changes (using the `rindle:building-rindle-apps` skill and its
reference files for each tier you touch).
