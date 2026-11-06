# PBKDF2 password hashing

## Scope

Milvus hashes user passwords with bcrypt, which is not a FIPS-approved algorithm and runs outside the validated module even in a `MILVUS_FIPS_ENABLED=ON` build. This adds an opt-in, FIPS-approved alternative. It does not make Milvus FIPS-certified.

## Change

`common.security.passwordHashAlgorithm` selects the algorithm for new and updated passwords:

- `bcrypt` (default): behavior is unchanged.
- `pbkdf2-sha512`: PBKDF2-HMAC-SHA-512.

An unknown value makes `PasswordEncrypt` fail rather than fall back to bcrypt.

### Hash format

```
$pbkdf2-sha512$i=<iterations>$<salt>$<key>
```

Salt and key use unpadded base64. New hashes use a 16-byte random salt, a 64-byte key and 210000 iterations. The count is read from the hash so it can be raised later, and values outside 1000 to 10000000 are treated as malformed.

### Verification

`VerifyStoredPassword` selects the algorithm from the hash format, not the configuration, so a credential verifies whatever the local setting is. Anything without the PBKDF2 prefix takes the existing bcrypt path unchanged, including its error classes. A malformed PBKDF2 hash returns `ErrServiceInternal`, and a mismatch returns `ErrPrivilegeNotAuthenticated`.

A rate-limited warning is logged when a bcrypt hash verifies while `pbkdf2-sha512` is configured.

## Why not `crypto/pbkdf2`

In Go 1.26, `crypto/pbkdf2` uses Go's native FIPS module and bypasses BoringCrypto, which `MILVUS_FIPS_ENABLED=ON` builds use. `crypto/hmac` and `crypto/sha512` route to BoringCrypto under `GOEXPERIMENT=boringcrypto`, so PBKDF2 is built on those.

## Password length

The 72-byte limit stays for new hashes under both algorithms. `proxy.maxPasswordLength` already caps at 72 regardless of configuration, and keeping it leaves the pre-lookup rejection in `adminauth` correct. Verifying a PBKDF2 hash has no such cap.

## Upgrade and CDC

- Upgrade all nodes, and all clusters in a CDC topology, before selecting `pbkdf2-sha512`. Older versions cannot verify the new hashes.
- A downgrade leaves users with a PBKDF2 hash unable to authenticate until their passwords are reset.
- The hash is computed on the proxy before the broadcast and carried in the `AlterUser` message, so a secondary applies the same value and verifies it by format, whatever its own setting.
- Existing hashes, including the root hash created at first startup, stay bcrypt until the password is updated. Selecting the option doesn't remove bcrypt hashes from a cluster.

## Not covered

The proxy's existing credential cache still uses `SHA256(password + username)`, and `CredentialInfo.Sha256Password` is included in the `AlterUser` message body. It is not persisted to the catalog, and this change does not alter it.
