# Drain in-flight work after an overlapping pipeline failure

When translation or speech synthesis fails permanently during stage overlap, the coordinator stops launching new requests but allows already-started provider requests to finish and durably commits their successful artifacts. Immediate cancellation cannot reliably prevent provider billing and would discard reusable paid work; draining is therefore the safer cost and recovery trade-off even though failure completion may take slightly longer.
