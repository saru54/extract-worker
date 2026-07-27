# Standalone extraction worker

This service reads encrypted pending jobs from Cloudflare R2, calls the existing
link-provider protocol, and writes the result back to R2. It has no registration
or Roxy dependency.

Required environment: R2_* variables, OPERATOR_DATA_KEY, and EXTRACT_LINK_*.
Run with the provided systemd unit.
