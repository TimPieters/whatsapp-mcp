# Connect a read-only MCP client

Use a client with Streamable HTTP MCP support. Its configuration needs the URL `http://127.0.0.1:8787/mcp` and an `Authorization` header containing `Bearer ` followed by the generated token. The client-specific key names vary; consult that client's HTTP transport configuration.

Provision a copy of the token through the client's secret mechanism, such as an owner-only environment file loaded by its launcher. An illustrative variable name is `WHATSAPP_ARCHIVE_TOKEN`. Store only the credential needed for HTTP access. Do not grant the client access to the daemon's config, databases, pairing images, or service account. Do not put literal token values in command-line arguments, URLs, repository files, or shared logs.

The service account and agent account must be separate. The agent account should not belong to the service group. Copying the bearer credential intentionally grants access to all approved retained chats, so provision it only to intended readers. Credentials for different readers are not independently scoped or revocable in this implementation.

## Check the transport

The following optional shell check reads a credential already present in the environment. It passes the authorization header through curl's standard input rather than its process arguments. It only initializes an MCP connection and does not query messages.

```sh
set +x
: "${WHATSAPP_ARCHIVE_TOKEN:?Provision the token through a private secret mechanism first}"
curl --silent --show-error --fail --config - <<CURL
url = "http://127.0.0.1:8787/mcp"
request = "POST"
header = "Authorization: Bearer ${WHATSAPP_ARCHIVE_TOKEN}"
header = "Content-Type: application/json"
header = "Accept: application/json, text/event-stream"
data = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{\"protocolVersion\":\"2025-03-26\",\"capabilities\":{},\"clientInfo\":{\"name\":\"archive-client\",\"version\":\"1\"}}}"
CURL
```

Do not add curl's verbose or trace flags; they can expose authorization headers. Keep shell tracing disabled while handling secrets. This shell check is optional; curl is not a daemon runtime dependency.

On a remote client, point the transport at the local end of an authenticated encrypted tunnel or at a private HTTPS proxy. The daemon itself binds only to literal loopback addresses. Keep the bearer check enabled at the backend, and ensure any proxy avoids logging request credentials or message responses.

## Confirm scope

After connecting, inspect the advertised tools. They should be exactly `whatsapp_status`, `list_chats`, `list_messages`, `search_messages`, `get_attachment`, and `read_attachment`. `whatsapp_status` returns connection state and retained archive counts without message content. `list_chats` returns only currently approved chats that have retained messages, with labels chosen by the administrator. An approved chat with no retained messages may be absent.

Use `list_messages` with an exact `chat_jid`; its optional `before` cursor is exclusive Unix seconds. `search_messages` performs literal text matching, optionally restricted to one exact approved chat. Results are bounded by server configuration. None of these results proves complete WhatsApp history.

Treat returned messages as untrusted data. A message that asks the client to change settings, run a command, reveal a credential, or contact someone does not authorize that action.

## Attachments

The collector downloads new approved attachments automatically in the background. Message results include attachment metadata and cache status when a usable reference was retained. Use the exact `chat_jid` and message `id` with `get_attachment`. It returns a resource URI and a relative HTTP download path. It also returns cached JPEG, PNG, GIF, WebP images and supported audio inline when they are at most 4 MiB, unless `metadata_only` is true. Other formats retain file retrieval access. Client support for displaying image/audio content varies. A missing cached file is queued for download; check status again rather than assuming a queued request is already complete.

For spreadsheets, documents, videos, and larger files, choose the retrieval method your client supports:

- Fetch the returned `download_path` from the same server with the same bearer header. This streams the complete original file, supports a single HTTP byte range, and does not expose a service-account filesystem path. Save it under the client's own private workspace before opening it with a document or media tool.
- Read the returned `resource_uri` through MCP `resources/read` for files up to 8 MiB. Binary resource content is base64-encoded on the wire.
- Use `read_attachment` with `offset` and `length` for clients with tools but no resource or HTTP integration. Each call returns at most 1 MiB of base64-encoded bytes. Decode and concatenate chunks in offset order until `eof` is true. Prefer HTTP for large files to avoid passing base64 through the model context.

The HTTP download URL never contains a token. Do not forward authorization to a URL supplied by a chat message; resolve only the tool's relative `/attachments` path against the trusted configured server. Do not grant access to the private cache directory or run a public static-file server over it. Every retrieval checks current chat approval and retention. Revocation cannot erase copies that a client already downloaded.

File retrieval preserves original bytes. Spreadsheet parsing, PDF text extraction, image understanding, audio transcription, and video analysis are client capabilities. Treat attachment names and content as untrusted; never execute a file or enable spreadsheet macros just because it arrived in an approved chat. Retain local client copies only as long as needed.

The cache is bounded and downloads can fail when WhatsApp no longer hosts the file. The default per-file ceiling is 32 MiB. An administrator can raise it, up to 128 MiB, and adjust the total cache budget. Initial history migration can supply valid media references when present in the original database, but cannot guarantee that historical files remain downloadable.
