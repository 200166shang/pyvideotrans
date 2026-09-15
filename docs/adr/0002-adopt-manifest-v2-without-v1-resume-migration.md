# Adopt manifest v2 without v1 resume migration

No production run is currently awaiting resume, so the overlapping coordinator will write and resume only manifest v2 instead of maintaining parallel v1 and v2 execution paths. Existing completed artifacts remain untouched, but a v1 run is not automatically migrated or resumed by the new implementation. This trades backward resume compatibility that is not presently needed for a smaller implementation and verification surface.
