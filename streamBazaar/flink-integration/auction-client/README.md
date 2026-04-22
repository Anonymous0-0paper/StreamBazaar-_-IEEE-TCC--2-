# Auction Client (Placeholder)

This folder is reserved for the Flink-side client library that communicates with
`auction-orchestrator` and `pricing-engine`.

Expected responsibilities:
- Submit real-time bids per tenant/operator.
- Receive allocation decisions.
- Emit scheduler-side metrics (preemptions, queue pressure, bid outcomes).
