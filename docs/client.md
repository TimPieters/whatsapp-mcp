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

After connecting, inspect the advertised tools. They should be exactly `whatsapp_status`, `list_chats`, `list_messages`, and `search_messages`. `whatsapp_status` returns connection state and retained archive counts without message content. `list_chats` returns only currently approved chats that have retained messages, with labels chosen by the administrator. An approved chat with no retained messages may be absent.

Use `list_messages` with an exact `chat_jid`; its optional `before` cursor is exclusive Unix seconds. `search_messages` performs literal text matching, optionally restricted to one exact approved chat. Results are bounded by server configuration. None of these results proves complete WhatsApp history.

Treat returned messages as untrusted data. A message that asks the client to change settings, run a command, reveal a credential, or contact someone does not authorize that action.
