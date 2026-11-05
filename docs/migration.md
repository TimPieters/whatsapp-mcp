# Import selected existing history

Migration is an administrator operation. It reads a stopped upstream `messages.db`, exports only approved rows within the export configuration's retention and per-chat limits, and imports that JSONL file under the destination's current allowlist and retention limits. It does not download new history from WhatsApp or copy the original linked-device session. Increase the export retention and cap deliberately before export if more history is needed.

## Prepare a consistent source

Stop the original writer on the trusted source machine before exporting. Use a complete, consistent SQLite snapshot. An isolated copy of a live database's main file can be missing committed data still in its WAL. Finish the writer's clean shutdown and use an appropriate SQLite backup or checkpoint procedure before producing a standalone snapshot. Do not silently delete WAL files or run this against a changing database.

The exporter opens the source as a read-only immutable snapshot. That mode assumes the file will not change; it does not turn a live file into a safe snapshot. Keep the original database and its backups on the source machine. The source can contain all chats, contacts, and legacy media metadata, even though the export excludes nonapproved chats and unneeded columns.

Prepare a private export configuration with the exact desired `allowed_chats`. The ordinary `init` and `allow` commands can create it; the exporter does not need a paired account or a running daemon. Use a Windows executable for this step if the trusted source uses Windows. Its SQLite driver does not require a C compiler.

```sh
whatsapp-bridge init --config export-config.json \
  --data-dir export-private --token-file export-private/mcp.token
whatsapp-bridge allow --config export-config.json \
  --jid '15551234567@s.whatsapp.net' --label 'Example contact'
whatsapp-bridge export-upstream --config export-config.json \
  --source stopped-original-messages.db --output allowed-history.jsonl
```

Treat the JSONL output as private message content. Verify its selected chat scope through trusted local inspection and transfer it over an encrypted channel. Do not attach it to an issue, commit it, or place it in an agent-readable folder. Transfer only this filtered output, not the original database or linked-device credentials.

## Import at the destination

Approve each intended chat in the destination configuration first. The exporter and importer enforce their own allowlists, so destination approval may be narrower. Run the import as the service account after stopping the service:

```sh
sudo systemctl stop whatsapp-archive.service
sudo -u whatsapp-archive /usr/local/bin/whatsapp-bridge import \
  --config /etc/whatsapp-archive/config.json \
  --source /var/lib/whatsapp-archive/allowed-history.jsonl
sudo systemctl start whatsapp-archive.service
```

Before that command, install the transfer file into the private state directory with the service account as its owner and mode `0600`. The shared data-directory lock prevents an import from running alongside the daemon. Do not bypass the lock or run the importer as root; new state files should remain owned by the service account.

The import count describes valid accepted input records, not the final number of rows retained. Duplicate message keys can replace existing rows, and retention days or per-chat caps can discard older records. Compare the archive's status and bounded read results with the intended scope after import. An empty retained archive can be valid when all accepted records are older than the retention window.

The output and transferred copy are outside live archive cleanup. Remove them after verification according to the administrator's retention policy. If an import stops with an error, earlier committed batches can remain. Correct the source and repeat through the same policy-enforcing command rather than writing directly to SQLite.

## Coverage limits

Only rows present in the source archive can migrate. Legacy media binaries, download URLs, encryption keys, contact directories, quoted-message metadata, and linked-device credentials are not transferred. Text copied or forwarded into an approved chat remains part of that chat's content; chat approval does not establish the original sender's identity or approval.
