# WSS Priority Remediation

WSS ordering is now explicitly independent of HTTP RPC ordering. The normal pool is assembled as `alchemy_1_wss -> alchemy_2_wss -> alchemy_3_wss -> quicknode_wss -> chainstack_wss -> arbitrum_official_wss`. Ankr remains HTTP-registered but is excluded from WSS rotation while its probe is failing.

WSS remains sticky while healthy. On failure the active endpoint is cooled and the next non-cooled endpoint in this ordered pool is selected. A recovered higher-priority endpoint does not preempt the active connection. Duplicate/older heads remain suppressed and HTTP polling fallback is unchanged.

Before:
`arbitrum_official -> chainstack -> alchemy_1 -> alchemy_2 -> alchemy_3 -> ankr_1 -> ankr_2 -> quicknode`

After:
`alchemy_1_wss -> alchemy_2_wss -> alchemy_3_wss -> quicknode_wss -> chainstack_wss -> arbitrum_official_wss`

HTTP RPC order changed: NO.

Validation passed: full tests, relevant race tests, vet and production build. Service restarted once and reported `alchemy_1_wss` connected. No healthy WSS connection was intentionally killed.
