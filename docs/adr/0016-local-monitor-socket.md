# 0016 - Local monitor socket

## Status

Accepted

## Context

The terminal monitor in ADR 0015 reads an admin token from the caller's configuration or SQLite database. A NixOS service runs under a dynamic user with its database in `/var/lib/underclass`, so an operator's `utop` cannot read that token. Running it with `sudo` does not put the service token in root's configuration either.

## Decision

Serve only `GET /monitor` on a Unix domain socket alongside the existing token-protected HTTP endpoint. The NixOS service places the socket at `/run/underclass/monitor.sock`; its runtime directory is searchable and the socket mode is `0666`, allowing local users to read the limited monitor snapshot without possessing a token. A non-NixOS server places the socket beside its database unless `UNDERCLASS_MONITOR_SOCKET` overrides the path.

Without `--url`, `underclass top` prefers its local socket, then the NixOS socket, then its existing token-authenticated HTTP path. An explicit `--url` continues to require `UNDERCLASS_UI_TOKEN`. The browser UI and every network `/admin/api/*` endpoint retain token authentication.

## Consequences

- Any local user can view monitor data, including account labels and usage, while the NixOS service runs. The socket exposes no account controls, credentials, or full prompts.
- The monitor works for an operator without access to the service's database or admin token.
- Remote monitoring and the browser UI still require the admin token.
