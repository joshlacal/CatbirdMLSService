---
status: verifying
trigger: "mls-service-unavailable - User can't create MLS conversations, getting 'MLS service not available' error"
created: 2026-01-30T19:56:00Z
updated: 2026-01-30T20:10:00Z
---

## Current Focus

hypothesis: Server is rejecting ALL key packages with "Key package validation error" - likely related to recent credential identity format change (DID vs DID#deviceUUID)
test: Check server-side key package validation code to understand what validation is failing
expecting: Server validation expects old credential identity format (bare DID) but client is now sending new format (DID#deviceUUID)
next_action: Check server's publish_key_packages handler and key package validation code

## Symptoms

expected: MLS conversations should be creatable between devices
actual: "MLS service not available" error when attempting to create conversations
errors: "MLS service not available" - exact error message
reproduction: Try to create an MLS conversation from iOS client
started: Just started after pushing credential identity format fix. Previously had InvalidSignature error for iPhone→Mac, Mac→iPhone worked.

## Eliminated

(none yet)

## Evidence

- timestamp: 2026-01-30T19:56:00Z
  checked: Server service status via systemctl
  found: catbird-mls-server.service is RUNNING (active since 18:28:11 UTC, 1h 27min ago)
  implication: Server is NOT down - issue is client-side or rate limiting

- timestamp: 2026-01-30T19:56:00Z
  checked: Server logs (journalctl -u catbird-mls-server.service -n 100)
  found: Multiple WARN messages showing "Rate limit exceeded for did:web:api.catbird.blue (mode: device): /xrpc/blue.catbird.mls.registerDevice (retry after X seconds)"
  implication: Client is aggressively trying to register device and hitting rate limits

- timestamp: 2026-01-30T19:56:00Z
  checked: Key package stats in logs
  found: "Key package stats for did:plc:7nmnou7umkr46rp7u2hbd3nb: available=0, threshold=5, needs_replenish=true"
  implication: User has 0 key packages available - this could be why MLS service appears unavailable

- timestamp: 2026-01-30T20:00:00Z
  checked: Server logs for ERROR level (last 30 min)
  found: MASSIVE number of "Failed to store key package N: Key package validation error" for user did:plc:34x52srgxttjewbke5hguloh
  implication: **ROOT CAUSE FOUND** - Server is rejecting ALL key packages with validation error. This is happening AFTER the credential identity format change (DID#deviceUUID).

- timestamp: 2026-01-30T20:00:00Z
  checked: Error pattern in logs
  found: ALL 50 key packages (0-49) rejected at 19:46:53 with "Key package validation error"
  implication: The server's validation logic is incompatible with the new key package format. Since key packages can't be stored, users have 0 available packages, which prevents MLS conversations from being created.

## Resolution

root_cause: SERVER-CLIENT FORMAT MISMATCH: The MLS server explicitly rejects key packages with credential identity format "DID#deviceUUID". The server validation code in db.rs:845-854 enforces "bare DID only" policy:
```rust
if credential_identity != did_owned {
    bail!(
        "KeyPackage credential identity must be the bare user DID ({}), got {} instead. \
         Device DIDs (with #device-id) are not allowed in MLS credentials.",
        did_owned,
        credential_identity
    );
}
```
The client was recently changed to use "DID#deviceUUID" format for credential identities (MLSDeviceManager.swift fix). This creates a mismatch - client sends DID#deviceUUID but server only accepts bare DID.

fix: REVERTED CLIENT to use bare DID for credential identities.
Changed MLSDeviceManager.swift:
1. Line 297: Changed `mlsCredentialIdentity = Self.makeClientIdentity(...)` to `mlsCredentialIdentity = normalizedUserDid`
2. Line 622: Changed `getClientIdentity()` to return bare DID instead of info.mlsDid
Server validation enforces bare DID format - no changes needed server-side.

verification: CatbirdMLSService builds successfully. Next steps:
1. User needs to rebuild Catbird iOS app
2. Delete app and reinstall to force re-registration
3. New key packages will be uploaded with bare DID format
4. Server should accept them without "Key package validation error"
files_changed:
  - CatbirdMLSService/Sources/CatbirdMLSService/MLSDeviceManager.swift
