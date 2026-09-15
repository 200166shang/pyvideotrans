# Dispatch only sealed synthesis chunks

The overlapping pipeline may build the speech-synthesis plan incrementally, but it submits only append-only sealed chunks with stable identities. Translation results may complete out of order and wait in a reorder buffer; synthesis chunks are sealed from the contiguous source-order prefix and final assembly remains source ordered. This preserves deterministic recovery and output equivalence while allowing translation and synthesis to overlap.
