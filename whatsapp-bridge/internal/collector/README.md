# Collector privacy boundary

Only explicitly approved chat envelopes reach application content extraction and the searchable archive. Policy is checked again by the archive while writing. Names, contact lists, message IDs, chat identifiers, message bodies, captions, and raw library errors are never logged. Chat labels come from local configuration.

Initial account-wide history downloading is disabled. New approved messages are archived while connected. Defensive history-event handling also applies the chat allowlist and writes at most 100 messages per batch. This is not a promise that the server supplies complete historical coverage.

The application never sends user messages, reactions, read receipts, presence updates, or automatic replies. It does not download media. The linked-device library still exchanges authentication, cryptographic, delivery-acknowledgment, and synchronization protocol traffic. "Read-only" refers to the application's user-message capabilities, not a network connection that transmits zero bytes.

View-once messages are skipped, including payloads already unwrapped by the library. Ordinary disappearing-message wrappers are unwrapped and approved text is retained under the local archive retention policy. Media records contain only a type and optional caption. No filenames, download URLs, keys, thumbnails, or quoted-message bodies are copied into the archive. Unsupported structured payloads, such as contact cards, are skipped.

`session.db` is private protocol state, separate from the searchable archive. It retains linked-account identity, cryptographic sessions and keys, app-state synchronization hashes, and phone-number/linked-identity mappings needed by the library. Those protocol records can refer to peers outside the archive allowlist. The allowlist cannot prevent the linked device from receiving and decrypting other account traffic in memory.

Contact-name, chat-setting, message-secret, privacy-token, and decrypted-event stores are replaced with no-op stores, including immediately after first pairing initializes the library SQL stores. Persistent retry-message storage is disabled. These choices intentionally reduce ancillary features. The underlying schema still contains empty tables for these disabled stores. Reusing a preexisting session database from another client is unsupported; old records are not scrubbed by these wrappers.

Pairing images are generated in process memory until their QR lifetime expires. The owner process may write the current image to an owner-only ephemeral `pairing.png` in the private state directory and remove it after pairing. No pairing image is served through HTTP or MCP. Administrator-made copies have their own lifetime and must be removed after use. On Windows, POSIX file modes alone do not replace user-profile ACLs.
