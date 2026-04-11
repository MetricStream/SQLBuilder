# Publishing to Maven Central

This document describes the steps to publish SQLBuilder artifacts to Maven Central via the Sonatype Central Portal.

## Prerequisites

- GnuPG (GPG) installed — verify with `gpg --version`
- Access to [central.sonatype.com](https://central.sonatype.com) with the `com.metricstream` namespace

---

## One-Time Setup

### 1. Generate a GPG Key

```bash
gpg --full-gen-key
```

When prompted:
- Kind: `1` (RSA and RSA)
- Size: `4096`
- Expiry: `0` (never expires)
- Real name and email: use your MetricStream identity
- Passphrase: choose a strong passphrase and store it securely

### 2. Find Your Key ID

```bash
gpg --list-secret-keys --keyid-format=long
```

Example output:
```
sec   rsa4096/ABCD1234EFGH5678 2026-04-11
uid   Prasadu Babu Dandu <prasadbabu@metricstream.com>
```

The key ID is the part after `rsa4096/` — e.g. `ABCD1234EFGH5678`.

### 3. Export the Private Key

```bash
gpg --armor --export-secret-keys ABCD1234EFGH5678 > C:/Users/<you>/private-key.asc
```

Store this file outside the repository. Never commit it.

### 4. Publish the Public Key to Keyservers

Sonatype validates signatures against public keyservers during release.

```bash
gpg --keyserver keyserver.ubuntu.com --send-keys ABCD1234EFGH5678
gpg --keyserver keys.openpgp.org --send-keys ABCD1234EFGH5678
gpg --keyserver pgp.mit.edu --send-keys ABCD1234EFGH5678
```

Verify the key is live:
```bash
gpg --keyserver keyserver.ubuntu.com --recv-keys ABCD1234EFGH5678
```

### 5. Generate a Sonatype Central Token

1. Log in to [central.sonatype.com](https://central.sonatype.com)
2. Click your profile → **Generate User Token**
3. Save the **token username** and **token password** securely — these are used as `sonatypeUsername` and `sonatypePassword`

---

## Publishing a New Version

### 1. Bump the Version

Update the version in [build.gradle.kts](build.gradle.kts):

```kotlin
version = "4.0.1"  // increment as appropriate
```

### 2. Verify the Build

```bash
./gradlew clean build
./gradlew ktlintCheck
./gradlew detekt
```

### 3. Publish to Central Portal

```bash
./gradlew publishAggregationToCentralPortal \
  -PsonatypeUsername=<token-username> \
  -PsonatypePassword=<token-password> \
  -PsigningKeyFile="C:/Users/<you>/private-key.asc" \
  -PsigningPassword=<gpg-passphrase>
```

### 4. Release on the Portal

Since `publishingType = "USER_MANAGED"` in [settings.gradle.kts](settings.gradle.kts):

1. Log in to [central.sonatype.com](https://central.sonatype.com)
2. Go to **Publish** → **Deployments**
3. Verify the deployment contents
4. Click **Publish** to release to Maven Central

Artifacts are available on Maven Central within ~30 minutes.

### 5. Tag the Release

```bash
git tag v4.0.1
git push origin v4.0.1
```

---

## Automation Option

To skip the manual portal release step, change in [settings.gradle.kts](settings.gradle.kts):

```kotlin
publishingType = "AUTOMATIC"
```

This will automatically release to Maven Central after a successful upload.

---

## Coordinates on Maven Central

| Module | Artifact |
|--------|---------|
| Core   | `com.metricstream.jdbc:sqlbuilder-core:<version>` |
| Mock   | `com.metricstream.jdbc:sqlbuilder-mock:<version>` |
