# WhatsApp archive MCP

A read-only MCP server for a local archive of explicitly approved WhatsApp chats. One Go process maintains a linked-device connection, filters incoming chat envelopes, stores approved text in SQLite, caches approved attachments on disk, and serves authenticated HTTP tools.

The allowlist starts empty. The server provides six tools:

| Tool | Purpose |
| --- | --- |
| `whatsapp_status` | Check collector and archive status |
| `list_chats` | List approved chats with configured labels |
| `list_messages` | Read retained messages from an approved chat |
| `search_messages` | Search retained text in approved chats |
| `get_attachment` | Get attachment status, retrieval links, and small inline images/audio |
| `read_attachment` | Read cached file bytes in bounded chunks |

There are no MCP tools for sending, reacting, managing contacts, pairing, changing the allowlist, or importing history. The archive keeps text, captions, and approved attachment metadata. A single background worker downloads images, videos, audio, stickers, and documents, including spreadsheets, into a bounded private disk cache. View-once messages and unsupported structured payloads are skipped.

Cached JPEG, PNG, GIF, WebP images and supported audio up to 4 MiB can be returned inline through MCP. Original files are available through authenticated HTTP, MCP binary resources up to 8 MiB, or bounded byte chunks. File access does not itself transcribe speech, parse spreadsheets, or analyze video; the client supplies those capabilities. See [attachment access](docs/client.md#attachments).

## Build

Install Go 1.27.x, then build from the module directory:

```sh
cd whatsapp-bridge
CGO_ENABLED=0 go build -trimpath -o whatsapp-bridge .
```

The runtime needs the resulting executable and its private state directory. It does not need Python, a C compiler, a separate SQLite installation, or a second bridge process. SQLite uses the pure Go `modernc.org/sqlite` driver.

Cross-compile a Linux ARM64 executable with:

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -o whatsapp-bridge-linux-arm64 .
```

CI runs tests and the race detector on Linux, then builds Linux AMD64, Linux ARM64, and a Windows AMD64 executable suitable for migration work. A cross-build does not verify a live linked-device session on the target system.

## Local setup

Use an owner-controlled directory outside any repository or shared folder. Run these commands as that owner:

```sh
umask 077
mkdir -p private-archive
./whatsapp-bridge init \
  --config "$PWD/private-archive/config.json" \
  --data-dir "$PWD/private-archive/data" \
  --token-file "$PWD/private-archive/mcp.token"
```

`init` creates a random bearer token with owner-only file permissions and an empty allowlist. Approve exact canonical chat identifiers before collecting:

```sh
./whatsapp-bridge allow \
  --config "$PWD/private-archive/config.json" \
  --jid '15551234567@s.whatsapp.net' --label 'Example contact'
./whatsapp-bridge allow \
  --config "$PWD/private-archive/config.json" \
  --jid '120363000000000000@g.us' --label 'Example group'
./whatsapp-bridge serve --config "$PWD/private-archive/config.json"
```

The identifiers above are examples. Copy the exact identifier for each intended chat from a trusted existing source. Phone-number JIDs and linked-identity JIDs are distinct identifiers; approving one does not silently approve the other. Contact search and account-wide chat discovery are not exposed.

During first pairing, the daemon temporarily writes an owner-only `pairing.png` inside its private data directory. In a separate trusted administrator session, copy it for viewing:

```sh
./whatsapp-bridge qr \
  --config "$PWD/private-archive/config.json" \
  --output "$PWD/private-archive/pairing-view.png"
```

Open that private image and scan it with WhatsApp's linked-device interface. If its QR expires, remove the copied file before running `qr` again; the command does not overwrite existing files. Remove the copied image after pairing. Pairing images are never served through HTTP or MCP. For a dedicated service installation, run `qr` as the service account, as described in [operations](docs/operations.md).

## Connect an MCP client

Configure a client that supports Streamable HTTP MCP with:

```text
URL: http://127.0.0.1:8787/mcp
Authorization: Bearer <contents of the generated token file>
```

Provision the token through the client's secret configuration. See [client setup](docs/client.md) for an environment-based connection check. Do not put the token in a URL, source file, shell history, public configuration, or issue report. Give the agent HTTP access and the token only. Do not give it a shell, the service account, the config file, the SQLite files, or the pairing directory. A client with those privileges can bypass the application boundary.

The daemon accepts only literal loopback listen addresses. For access from another host, use an authenticated encrypted tunnel or a carefully configured private HTTPS proxy to the loopback backend. A bearer token over plain remote HTTP is exposed to the network path. Keep the service itself behind that boundary.

## Configuration and retention

[deploy/config.example.json](deploy/config.example.json) lists the supported configuration fields. Defaults retain 90 days and at most 10,000 messages per chat, with at most 100 results per query. Adjust them for the intended archive size.

Attachments download automatically after collection, subject to `max_attachment_bytes` (default 32 MiB per file) and `attachment_cache_bytes` (default 1 GiB of cached file content). These are disk limits, not RAM allocations. Oversized files remain identifiable but are not downloaded. Least-recently-used cached files can be evicted; `get_attachment` can queue a fresh download if the source remains available. Cache eviction is distinct from message deletion. Removing or expiring a message removes its attachment reference and cached file. Downloads use streaming I/O and one worker, with temporary space needed for the in-flight file.

While serving, the daemon checks configuration every five seconds. A validated allowlist change applies to ingestion and reads. Removing approval also purges that chat's archived rows. Invalid configuration suspends archive access. Changing the listen address, data directory, or token-file path requires a restart; a live change to those values suspends access until then.

Remove approval through the owner CLI:

```sh
./whatsapp-bridge deny \
  --config "$PWD/private-archive/config.json" \
  --jid '15551234567@s.whatsapp.net'
```

The retention policy applies to ordinary disappearing-message text too. This archive does not automatically mirror WhatsApp's message expiration or revocation. Backups, exported files, and copies made by a client have their own lifetimes.

## Existing history

Initial account-wide history downloads are disabled. The collector archives new approved messages while connected. Older history can be migrated once from a stopped upstream SQLite archive, with approval enforced during both export and import:

```sh
./whatsapp-bridge export-upstream --config export-config.json \
  --source stopped-original-messages.db --output allowed-history.jsonl
./whatsapp-bridge import --config config.json --source allowed-history.jsonl
```

Run export on the trusted source machine. Transfer only the allowlisted JSONL file. Stop the destination service before importing. See [migration](docs/migration.md) for snapshot handling, retention, and verification.

For failed downloads whose files may still exist on the linked owner's phone, the administrator-only `recover-attachments` command queues a bounded media-recovery request. It never posts chat messages and is not exposed over MCP. See [operations](docs/operations.md) for scope and availability limits.

## Privacy limits and deployment

The archive allowlist controls which chat envelopes may persist message text. It cannot stop the linked-device library from receiving and decrypting other account traffic in memory. The separate `session.db` retains necessary cryptographic sessions, app-state synchronization metadata, and phone-number/linked-identity mappings, including identifiers outside the allowlist. Contact-name and plaintext-event persistence are disabled.

Approval is a chat boundary, not a sender-origin boundary. A message in an approved chat may quote, forward, or repeat information about someone else. Retrieved text is untrusted content and may include instructions addressed to an agent. The receiving client must treat it as data.

Read the [security model](docs/security.md) and [Linux service guide](docs/operations.md) before deployment. The included systemd unit supplies starting memory ceilings. No resident-memory measurement or headroom guarantee is claimed.

## Attribution and license

This fork is based on [Luke Harries' WhatsApp MCP](https://github.com/lharries/whatsapp-mcp), with a different collection, storage, and tool-access model. The upstream copyright notice and [MIT license](LICENSE) are retained. The linked-device integration uses [whatsmeow](https://github.com/tulir/whatsmeow), under its own license. This project is independent of WhatsApp and does not provide an official WhatsApp API integration.
