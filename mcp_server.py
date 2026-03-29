import os
from typing import Any

import httpx
from fastmcp import FastMCP


API_BASE_URL = os.getenv("MOUSESEARCH_API_BASE_URL", "http://127.0.0.1:5000/api/v1").rstrip("/")
API_TOKEN = str(os.getenv("MOUSESEARCH_API_TOKEN", "") or "").strip()
MCP_HOST = os.getenv("MOUSESEARCH_MCP_HOST", "0.0.0.0")
MCP_PORT = int(os.getenv("MOUSESEARCH_MCP_PORT", "8765"))

INSTRUCTIONS = """
MouseSearch MCP exposes a private, agent-friendly wrapper around the MouseSearch HTTP API.

Recommended workflow:
1. Call `server_info`, `mam_status`, and `client_status` first.
2. Use `search_torrents` to find ranked MAM results.
3. Take a result's `download_link` and `id`, then call `add_torrent`.
4. If add returns no hash yet, call `resolve_mid`.
5. Use `torrent_info` or `torrent_info_batch` to monitor the torrent client.

Notes:
- This server is intended for trusted private access, typically over Tailscale.
- `search_torrents` accepts the same filter names as the HTTP API in its `filters` object.
- `add_torrent` only needs a MAM `download_link` to work, but title/author/id make follow-up tracking much better.
- `custom_relative_path` and `custom_destination_path` are only useful if organization is configured in MouseSearch.
""".strip()

mcp = FastMCP("MouseSearch MCP", instructions=INSTRUCTIONS)


async def api_request(method: str, path: str, *, params: dict[str, Any] | None = None, json_body: dict[str, Any] | None = None) -> dict[str, Any]:
    headers = {"Accept": "application/json"}
    if API_TOKEN:
        headers["Authorization"] = f"Bearer {API_TOKEN}"

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.request(
            method=method,
            url=f"{API_BASE_URL}{path}",
            params=params,
            json=json_body,
            headers=headers,
        )

    content_type = response.headers.get("content-type", "")
    if "application/json" in content_type.lower():
        payload = response.json()
    else:
        payload = {"raw_text": response.text}

    if response.is_error:
        return {
            "ok": False,
            "status_code": response.status_code,
            "path": path,
            "payload": payload,
        }

    return {
        "ok": True,
        "status_code": response.status_code,
        "path": path,
        "payload": payload,
    }


@mcp.tool
async def server_info() -> dict[str, Any]:
    """Return API discovery info and recommended agent workflow."""
    return await api_request("GET", "/info")


@mcp.tool
async def mam_status() -> dict[str, Any]:
    """Check whether MouseSearch can talk to MyAnonamouse."""
    return await api_request("GET", "/mam/status")


@mcp.tool
async def mam_user_data() -> dict[str, Any]:
    """Fetch current MyAnonamouse user stats such as ratio, uploaded, downloaded, and bonus points."""
    return await api_request("GET", "/mam/user_data")


@mcp.tool
async def client_status() -> dict[str, Any]:
    """Check whether MouseSearch can talk to the configured torrent client."""
    return await api_request("GET", "/client/status")


@mcp.tool
async def client_categories() -> dict[str, Any]:
    """List torrent-client categories currently available for add operations."""
    return await api_request("GET", "/client/categories")


@mcp.tool
async def search_torrents(query: str, filters: dict[str, Any] | None = None) -> dict[str, Any]:
    """
    Search MyAnonamouse and return ranked JSON results.

    Put any supported HTTP query params in `filters`, for example:
    `{"hide_downloaded": True, "main_cat": ["13"], "language_ids": ["1"]}`.
    """
    params = dict(filters or {})
    params["query"] = query
    return await api_request("GET", "/mam/search", params=params)


@mcp.tool
async def add_torrent(
    torrent_url: str,
    title: str = "",
    author: str = "",
    torrent_id: str = "",
    category: str = "",
    size: str = "",
    main_cat: str = "",
    free: int = 0,
    download_link: str = "",
    custom_relative_path: str = "",
    custom_destination_path: str = "",
) -> dict[str, Any]:
    """
    Add a torrent to the configured torrent client.

    `torrent_url` should usually be the `download_link` returned by `search_torrents`.
    """
    payload = {
        "torrent_url": torrent_url,
        "title": title,
        "author": author,
        "id": torrent_id,
        "category": category,
        "size": size,
        "main_cat": main_cat,
        "free": free,
        "download_link": download_link or torrent_url,
    }
    if custom_relative_path:
        payload["custom_relative_path"] = custom_relative_path
    if custom_destination_path:
        payload["custom_destination_path"] = custom_destination_path

    return await api_request("POST", "/client/add", json_body=payload)


@mcp.tool
async def resolve_mid(mid: str) -> dict[str, Any]:
    """Resolve a MyAnonamouse torrent id to a torrent-client hash when add_torrent returns before the hash is known."""
    return await api_request("POST", "/client/resolve_mid", json_body={"mid": mid})


@mcp.tool
async def torrent_info(info_hash: str) -> dict[str, Any]:
    """Fetch torrent-client status for a single info hash."""
    return await api_request("GET", f"/client/info/{info_hash}")


@mcp.tool
async def torrent_info_batch(hashes: list[str]) -> dict[str, Any]:
    """Fetch torrent-client status for multiple info hashes in one request."""
    return await api_request("POST", "/client/info/batch", json_body={"hashes": hashes})


if __name__ == "__main__":
    mcp.run(transport="http", host=MCP_HOST, port=MCP_PORT)
