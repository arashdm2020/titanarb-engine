# TitanArb WSS Provider Pool Remediation

## Executive Summary

The WSS client now applies per-endpoint cooldown/backoff, avoids switching to cooling endpoints, resets failure state after a successful connection, and drops duplicate or older block heads. WSS endpoints are independently registered from explicit values or safely derived from existing authenticated HTTP endpoints for Alchemy, Ankr and QuickNode. HTTP routing and execution behavior are unchanged.

## Provider Capability Matrix

| Provider | HTTP | WSS | Evidence |
|---|---:|---:|---|
| alchemy_1/2/3 | yes | derived/registered as alchemy_1_wss..3_wss | runtime config |
| ankr_1/2 | yes | derived/registered | runtime config |
| chainstack | yes | explicit chainstack_wss | runtime config |
| arbitrum_official | yes | existing fallback | runtime config |
| quicknode | yes | derived/registered | runtime config |

## Runtime and Validation

After deployment, `alchemy_1_wss` connected successfully and remained active during observation. The prior chainstack/official ping-pong stopped after the initial migration: post-deploy there was one failover to Alchemy and one stable connection, with no repeated failover loop observed. HTTP polling remains available and was not removed.

## Safety

Only new-head transport behavior changed. Scheduler/detector, route generation, HTTP RPC, execution, profitability, simulation, signer and broadcast semantics were not changed.

## Tests

`go test ./cmd/titanarb ./internal/...`, relevant race tests, `go vet ./cmd/titanarb ./internal/...`, and production build all passed in the isolated validation tree.

## Deployment

Commit `5b0bdd4` was deployed with a recoverable binary backup. Service is active/running. At observation time reconciliation was still warming the route cache (`24/50` units in the latest sample); this is independent of WSS registration.

## Final Verdict

- WSS providers active: `alchemy_1_wss` observed active; additional configured endpoints are registered candidates.
- Alchemy WSS: all three registered by derived endpoint logic when HTTP endpoints exist.
- Excluded providers: endpoints in cooldown are skipped; when all are unavailable the current endpoint is retried with bounded backoff and HTTP fallback remains available.
- Failover loop: fixed by cooldown-aware selection and deduplicated heads; no post-deploy ping-pong observed.
- HTTP fallback: still required and preserved.
- Real WSS redundancy: yes, multiple independently selected endpoints are now available.
