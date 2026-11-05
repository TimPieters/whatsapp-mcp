# Linux service operations

The included systemd unit runs one Go process under a dedicated account. It keeps private state separate from an administrator-owned configuration and gives an MCP client only authenticated HTTP access.

## Install

Build the executable for the destination architecture. From the repository root, install it and create the account and directories:

```sh
sudo install -o root -g root -m 0755 whatsapp-bridge/whatsapp-bridge /usr/local/bin/whatsapp-bridge
sudo useradd --system --user-group --home-dir /var/lib/whatsapp-archive \
  --shell /usr/sbin/nologin whatsapp-archive
sudo install -d -o whatsapp-archive -g whatsapp-archive -m 0700 /var/lib/whatsapp-archive
sudo install -d -o root -g whatsapp-archive -m 0750 /etc/whatsapp-archive
sudo /usr/local/bin/whatsapp-bridge init \
  --config /etc/whatsapp-archive/config.json \
  --data-dir /var/lib/whatsapp-archive \
  --token-file /var/lib/whatsapp-archive/mcp.token
sudo chown root:whatsapp-archive /etc/whatsapp-archive/config.json
sudo chmod 0640 /etc/whatsapp-archive/config.json
sudo chown whatsapp-archive:whatsapp-archive /var/lib/whatsapp-archive/mcp.token
sudo chmod 0600 /var/lib/whatsapp-archive/mcp.token
```

Skip account creation if the dedicated account already exists. Do not substitute the agent's account. The token is created by `init`; the example configuration does not contain a usable credential. Keep config and state outside the repository.

Approve the intended chat identifiers before startup:

```sh
sudo /usr/local/bin/whatsapp-bridge allow \
  --config /etc/whatsapp-archive/config.json \
  --jid '15551234567@s.whatsapp.net' --label 'Example contact'
sudo chown root:whatsapp-archive /etc/whatsapp-archive/config.json
sudo chmod 0640 /etc/whatsapp-archive/config.json
sudo install -o root -g root -m 0644 deploy/whatsapp-archive.service /etc/systemd/system/whatsapp-archive.service
sudo systemctl daemon-reload
sudo systemctl enable --now whatsapp-archive.service
```

Preserve the configuration's administrator ownership, service-group readability, and lack of service write permission after every edit. Do not grant the agent membership in that group or read access to the private state directory.

## Pair privately

While the unpaired daemon is running, copy its current QR as the service account:

```sh
sudo -u whatsapp-archive /usr/local/bin/whatsapp-bridge qr \
  --config /etc/whatsapp-archive/config.json \
  --output /var/lib/whatsapp-archive/pairing-view.png
```

View that image through a trusted administrator-only display or transfer it privately for scanning. Run `qr` as the service account, rather than root, so an exported image has the intended owner. The live `pairing.png` is temporary and removed after pairing. A copied `pairing-view.png` is a separate secret and must be removed after use. Neither file belongs in an MCP client's accessible filesystem. If the code expires, remove the old copy before running `qr` again; it refuses to overwrite an existing output file.

A paired session reconnects using `session.db`. Do not copy another client's session database into this directory. To unlink a device, use WhatsApp's linked-device controls. If pairing must be reset locally, stop the service before an administrator removes or archives state. Preserve the searchable archive separately if it is still wanted.

## Operate and change approval

The endpoint defaults to `http://127.0.0.1:8787/mcp`. Provision only the token contents to the client through its secret mechanism. An operator can inspect generic service health without reading messages:

```sh
sudo systemctl status whatsapp-archive.service
sudo journalctl -u whatsapp-archive.service --since '15 minutes ago'
sudo systemctl show whatsapp-archive.service \
  -p MemoryCurrent -p MemoryPeak -p MemoryHigh -p MemoryMax -p MemorySwapCurrent
```

Use `allow` or `deny` as an administrator to update `/etc/whatsapp-archive/config.json`, and preserve `root:whatsapp-archive` ownership with mode `0640`. The daemon checks for changes every five seconds. A rejected or unreadable config suspends access until a valid configuration can be applied. Changing listen, data-dir, or token-file settings requires a restart. Removing approval purges that chat's live archive, but not its exports, backups, or client copies.

For token rotation, stop the service, replace the owner-only token through a trusted administrative procedure, update each client's secret, and restart. Do not paste the token into a command-line argument or log it. Do not use `init` to overwrite an existing installation.

Import requires a stopped service and the same dedicated account. See [migration](migration.md). Keep only one collector owner per account. The local directory lock prevents two processes sharing its state directory; it cannot detect a second collector using a different directory or host.

## Resource limits

The sample unit sets these starting limits:

| Setting | Value | Effect |
| --- | --- | --- |
| `GOMEMLIMIT` | `256MiB` | Go's soft runtime memory target |
| `MemoryHigh` | `384M` | cgroup memory-pressure threshold |
| `MemoryMax` | `512M` | Hard cgroup ceiling that can terminate the process |
| `MemorySwapMax` | `0` | Disallows swap for the service cgroup |

These are configurable ceilings, not measured consumption or proof of spare system memory. Go's soft target excludes some process and operating-system costs. SQLite caches, cryptographic state, and bursts of incoming traffic can still raise resident memory. A hard-limit kill can interrupt collection. The unit limits repeated restarts so an unsuitable ceiling does not create a rapid loop.

Account-wide initial history download is disabled, and approved archive writes are bounded. This reduces one source of large history bursts without proving a maximum working set. Observe process and cgroup memory on the actual host under representative traffic, then adjust a unit override and restart if needed. Check that the host supports the configured cgroup controls.

Approved attachments are prefetched by one background worker. The default `max_attachment_bytes` is 33,554,432 bytes and `attachment_cache_bytes` is 1,073,741,824 bytes. Both settings live in the administrator-owned JSON configuration and reload with the other archive policy. The maximum supported individual file is 128 MiB. Allow space for the archive/session databases and filesystem overhead in addition to the attachment cache. Disk capacity is not a RAM reservation; downloads and complete HTTP reads stream from files.

Cached attachments follow parent-message approval, retention, and count limits. Cache pressure evicts least-recently-used files while keeping eligible message metadata. A later `get_attachment` can request another download. Source files can expire, so cache eviction can make a historical attachment unavailable permanently. Use a deliberately sized cache and protected backups if long-term file retention is needed. The cache is not a permanent document-management system.

CI also records a synthetic archive-import resource check on native Linux AMD64. It compiles an archive test binary without race instrumentation, imports 20,000 synthetic 1 KiB messages across two chats in batches of 100, and runs the benchmark once under `/usr/bin/time -v`:

```sh
cd whatsapp-bridge
CGO_ENABLED=0 go test -c -o /tmp/archive.test ./internal/archive
/usr/bin/time -v /tmp/archive.test -test.run '^$' \
  -test.bench '^BenchmarkBoundedImport$' -test.benchtime=1x
```

The resulting maximum resident set size and elapsed time appear in CI logs. This measures the synthetic archive operation on that runner, including the Go test process. It excludes the live linked-device connection, transport bursts, and target-host behavior, so it cannot establish deployment headroom. No result is claimed before the workflow runs.

CI separately measures `BenchmarkStreamingAttachment` using a generated 32 MiB file streamed through the private cache and read back. This checks the file-storage path without allocating a file-sized input buffer. It excludes a real WhatsApp download, TLS, decryption, and any client-side file analysis, and does not establish a live-process peak.

## Recover failed attachments

An administrator can ask the connected owner's phone to refresh failed attachment download paths:

```sh
sudo -u whatsapp-archive /usr/local/bin/whatsapp-bridge recover-attachments \
  --config /etc/whatsapp-archive/config.json
```

Run this as the service account while the paired daemon is connected. The command creates a private request marker; the daemon processes at most 32 eligible failed attachments and records one recovery attempt per file. Requests are paced and sent only to the account owner's linked phone through the media-retry protocol. They do not post chat messages or mark messages read. The phone must still have the file and be able to respond. Unknown legacy group participants are omitted rather than fabricated, and the phone may reject an incomplete old envelope.

Only currently approved, retained messages are eligible. Responses must match the pending message and private reference, and cannot revive removed chats. A refreshed path is downloaded through the same size, cache, and integrity checks as other attachments. Recovery is not an MCP tool and cannot be triggered by the bearer credential alone. It is not a promise that deleted or unavailable phone media can be reconstructed. Leave the private attempt records intact to avoid repeated reupload requests.

If refreshed paths remain unusable, an administrator can request copies of the exact failed messages from the owner's phone:

```sh
sudo -u whatsapp-archive /usr/local/bin/whatsapp-bridge recover-message-copies \
  --config /etc/whatsapp-archive/config.json
```

This uses a separate once-per-file attempt record and the same bounded, private request mechanism. It requests individual messages from the owner's primary device without posting messages to chats or downloading account-wide history. Each response must match the saved peer request and approved message. It can update encryption credentials and a download path only when the attachment type, original plaintext SHA-256 and size remain identical. Missing or incomplete historical metadata can prevent recovery. Keep the owner phone online while the service processes the request.

If an administrator already has an original file, restore that one attachment while the service is stopped:

```sh
sudo systemctl stop whatsapp-archive.service
sudo -u whatsapp-archive /usr/local/bin/whatsapp-bridge restore-attachment \
  --config /etc/whatsapp-archive/config.json \
  --jid '120363000000000000@g.us' --message-id 'EXAMPLE_MESSAGE_ID' \
  --source /var/lib/whatsapp-archive/private-original.bin
sudo systemctl start whatsapp-archive.service
```

The source must be readable by the service account and match the selected message's existing size and SHA-256 exactly. A similar filename or a newer document revision is insufficient. This command does not change the message, expected hash, encryption credentials or allowlist. It uses the same bounded cache path and is not available over MCP. Remove temporary transfer copies after verification; preserve the original source separately.

## Backups and upgrades

Stop the service before copying SQLite files, or use a proper SQLite backup tool that includes committed WAL data. Back up the token and linked-device session only to storage with equivalent private access controls. Treat a backup of protocol keys as account credentials. Archive policy changes cannot purge older backups.

For an upgrade, stop the service, create a consistent private backup, install the new executable, and start the service. Recheck authenticated status, approved chat scope, and new-message collection. Do not infer live transport compatibility from compilation alone. To recover from a failed upgrade, restore a matching executable and consistent state backup together.
