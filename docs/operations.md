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

CI also records a synthetic archive-import resource check on native Linux AMD64. It compiles an archive test binary without race instrumentation, imports 20,000 synthetic 1 KiB messages across two chats in batches of 100, and runs the benchmark once under `/usr/bin/time -v`:

```sh
cd whatsapp-bridge
CGO_ENABLED=0 go test -c -o /tmp/archive.test ./internal/archive
/usr/bin/time -v /tmp/archive.test -test.run '^$' \
  -test.bench '^BenchmarkBoundedImport$' -test.benchtime=1x
```

The resulting maximum resident set size and elapsed time appear in CI logs. This measures the synthetic archive operation on that runner, including the Go test process. It excludes the live linked-device connection, transport bursts, and target-host behavior, so it cannot establish deployment headroom. No result is claimed before the workflow runs.

## Backups and upgrades

Stop the service before copying SQLite files, or use a proper SQLite backup tool that includes committed WAL data. Back up the token and linked-device session only to storage with equivalent private access controls. Treat a backup of protocol keys as account credentials. Archive policy changes cannot purge older backups.

For an upgrade, stop the service, create a consistent private backup, install the new executable, and start the service. Recheck authenticated status, approved chat scope, and new-message collection. Do not infer live transport compatibility from compilation alone. To recover from a failed upgrade, restore a matching executable and consistent state backup together.
