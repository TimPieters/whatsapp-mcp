# Security model

## Access boundary

The daemon exposes six read-only MCP tools and attachment resources over `/mcp`, plus streamed attachment reads through `/attachments`. Every HTTP request requires the configured bearer token. Pairing, allowlist changes, imports, exports, and process administration are local owner operations. There is no HTTP route for pairing images or raw database access.

Deploy the daemon under a dedicated service account. That account owns the private state directory with mode `0700`; generated secrets and pairing images use mode `0600`. The configuration should be owned by the administrator, readable by the service group, and unwritable by the service account. The agent receives only its token and access to the HTTP endpoint. Do not add its operating-system account to the service group or give it shell/sudo access as the service owner.

The token authorizes all retained content from all approved chats. It does not implement per-client or per-chat roles. Use separate instances and credentials when clients need different approval sets. Anyone who obtains the token can read through these tools; anyone who obtains the private service account or its files has broader access.

Loopback HTTP assumes a trusted local connection. Remote connections need encryption, such as a private authenticated tunnel or HTTPS termination. Keep authentication intact at the backend, redact authorization headers from proxy logs, and do not publish the endpoint or token through a URL.

## What the allowlist controls

Approval uses exact canonical chat JIDs, including their server suffix. No wildcard, contact-name, partial-number, or automatic phone-number/linked-identity expansion is performed. Empty approval means no messages can be archived or returned.

The live collector checks the envelope's chat before extracting bodies or captions. The archive checks policy again when writing and reading. Defensive history-event handling checks each conversation and rejects a message whose embedded chat contradicts the conversation envelope. No sender-name or contact-directory lookup is needed. Display labels are configured by the administrator.

The boundary is the approved chat. Text in that chat can include copied, quoted, or forwarded information from another source. Approval does not prove that every person mentioned or represented in its contents was independently approved. Quote metadata and contact cards are not separately indexed.

The daemon polls configuration every five seconds. Once a valid update is applied, removed chats are purged and no longer readable or ingestible. An invalid update suspends archive access. Changes to the listening address, state directory, or token path suspend access until restart. File edits are not an instantaneous policy switch; allow for the polling interval and verify the resulting status.

## What is stored

| Location | Contents | Access |
| --- | --- | --- |
| Searchable archive database | Approved message IDs, chat/sender JIDs, text/captions, timestamps, direction and media type | Private service account; mediated MCP reads |
| Attachment references in archive database | Approved filenames, MIME types, sizes, download paths, media decryption keys and integrity hashes | Private service account; public responses omit download paths, keys and hashes |
| Private attachment cache | Downloaded approved file content and temporary in-flight downloads | Private service account; mediated authenticated reads |
| `session.db` | Linked-device identity, encryption keys/sessions, app-state synchronization state, phone-number/linked-identity mappings | Private service account only |
| Token file | Random bearer credential | Private service owner and intentionally provisioned clients |
| Temporary `pairing.png` | Current pairing QR | Private service owner only; removed after pairing |
| Migration JSONL | Selected approved historical records and optional private media references | Trusted administrator; outside agent-accessible storage |

The linked-device session is account-wide. The library may receive, decrypt, and handle nonapproved traffic in process memory before the application sees an event. Protocol state can contain identifiers and cryptographic records for nonapproved peers. This implementation does not promise that unapproved traffic never enters memory or that protocol state is restricted to approved peers.

Contact-name, chat-setting, message-secret, privacy-token, and decrypted-event stores are replaced by nonpersistent stores. Persistent retry-message caching is disabled. Library logs are suppressed; application logs use generic operational messages without names, JIDs, message IDs, bodies, captions, raw library errors, or QR values. Start with a fresh session database. Reusing one from another client can preserve older records that this application did not create and does not scrub.

## Message and retention behavior

There are no application-level user-message send, reaction, presence, or read-receipt operations. Approved attachment downloads and necessary linked-device authentication, cryptographic, delivery acknowledgment, and synchronization traffic still occur. This is read-only application access, not a receive-only network protocol.

Initial account-wide history downloads are disabled. New approved messages are archived while connected. A stopped upstream database can supply selected older history through the local migration commands. The application does not guarantee complete history or recovery of every message missed while offline.

View-once messages are skipped. Approved ordinary disappearing-message wrappers are unwrapped, and their text and attachments follow local archive retention. The archive does not mirror source expiration or revocation. Attachment references contain only the fields needed to identify and retrieve the approved file; quoted media and thumbnails are not extracted. Private references never appear in public message results, resources, or retrieval links.

Downloads run one at a time with bounded streaming writes and integrity validation. Temporary files are private. Attachment reads, cache commits, and policy changes share the archive's authorization boundary. Retention and approval removal purge references and cached files. An already-running bounded response can finish before the policy update acquires the archive lock; downloaded client copies are outside the service's control. Cache limits can evict files without deleting their parent messages. Oversized or unavailable files produce status rather than an unlimited download.

Defaults retain 90 days and at most 10,000 messages per chat. Policy application removes disallowed rows and compacts the archive. Secure deletion within this SQLite file does not erase copies from backups, filesystem snapshots, SSD remapping, migration exports, client responses, or operating-system diagnostics. Protect those separately. The example service disables swap for its cgroup and requests no core dumps; verify host policy and cgroup support before relying on either setting.

## Agent use

Retrieved WhatsApp text is untrusted evidence. It may contain instructions, credential requests, malicious links, or text impersonating a tool response. The client must not treat message content as authorization to run commands, disclose secrets, change the allowlist, or contact others.

A bearer token grants read access, not authority to perform external actions. Keep the agent's write-capable tools and filesystem access separate from this service. The MCP tool list alone cannot constrain an agent that also controls the daemon's operating-system account.
