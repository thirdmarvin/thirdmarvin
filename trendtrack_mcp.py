#!/usr/bin/env python3
"""TrendTrack MCP & REST Search CLI.

Connects to TrendTrack's remote Streamable HTTP MCP server
(https://api.trendtrack.io/v1/mcp) and REST API (https://api.trendtrack.io)
using Python's standard library so it works in restricted/proxy environments
with zero external dependencies.
"""

from __future__ import annotations

import argparse
import json
import os
import stat
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

DEFAULT_MCP_URL = "https://api.trendtrack.io/v1/mcp"
DEFAULT_API_BASE = "https://api.trendtrack.io"
DEFAULT_KEY_FILE = Path.home() / ".gemini" / "trendtrack_api_key"
MCP_PROTOCOL_VERSION = "2026-07-28"


class TrendTrackError(RuntimeError):
    """Raised when a TrendTrack MCP or REST call fails."""


def load_dotenv_value(dotenv_path: Path, key_name: str = "TRENDTRACK_API_KEY") -> str | None:
    if not dotenv_path.is_file():
        return None
    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        if k.strip() == key_name:
            val = v.strip().strip('"').strip("'")
            if val:
                return val
    return None


def resolve_api_key(explicit_key: str | None = None) -> str | None:
    if explicit_key and explicit_key.strip():
        return explicit_key.strip()
    env_key = os.environ.get("TRENDTRACK_API_KEY", "").strip()
    if env_key:
        return env_key
    for candidate in (Path.cwd() / ".env", Path(__file__).resolve().parent / ".env"):
        val = load_dotenv_value(candidate)
        if val:
            return val
    if DEFAULT_KEY_FILE.is_file():
        val = DEFAULT_KEY_FILE.read_text(encoding="utf-8").strip()
        if val:
            return val
    return None


def save_api_key(api_key: str, key_file: Path = DEFAULT_KEY_FILE) -> Path:
    cleaned = api_key.strip()
    if not cleaned:
        raise TrendTrackError("API key cannot be empty.")
    key_file.parent.mkdir(parents=True, exist_ok=True)
    key_file.write_text(cleaned + "\n", encoding="utf-8")
    try:
        os.chmod(key_file, stat.S_IRUSR | stat.S_IWUSR)
    except OSError:
        # Shared mounts (e.g., ~/.gemini -> /shared/user/.gemini) may disallow chmod.
        pass
    return key_file


def mask_key(api_key: str) -> str:
    if len(api_key) <= 8:
        return "*" * len(api_key)
    return f"{api_key[:4]}...{api_key[-4:]}"


def parse_sse_json_messages(raw_text: str) -> list[dict[str, Any]]:
    """Parse JSON-RPC payloads from a text/event-stream (SSE) body."""
    messages: list[dict[str, Any]] = []
    data_lines: list[str] = []
    for line in raw_text.splitlines():
        if line.startswith("data:"):
            data_lines.append(line[5:].lstrip())
        elif line.strip() == "" and data_lines:
            payload = "\n".join(data_lines).strip()
            data_lines.clear()
            if payload and payload != "[DONE]":
                messages.append(json.loads(payload))
    if data_lines:
        payload = "\n".join(data_lines).strip()
        if payload and payload != "[DONE]":
            messages.append(json.loads(payload))
    return messages


def decode_mcp_response(body_bytes: bytes, content_type: str, request_id: int | None) -> dict[str, Any] | None:
    text = body_bytes.decode("utf-8", errors="replace").strip()
    if not text:
        return None
    ctype = (content_type or "").lower()
    if "text/event-stream" in ctype or text.startswith("event:") or text.startswith("data:"):
        messages = parse_sse_json_messages(text)
        if request_id is not None:
            for msg in messages:
                if msg.get("id") == request_id:
                    return msg
        return messages[-1] if messages else None
    return json.loads(text)


class TrendTrackClient:
    """Minimal Streamable HTTP MCP + REST client for TrendTrack."""

    def __init__(
        self,
        api_key: str | None = None,
        mcp_url: str = DEFAULT_MCP_URL,
        api_base: str = DEFAULT_API_BASE,
        timeout: int = 60,
    ) -> None:
        self.api_key = api_key
        self.mcp_url = mcp_url.rstrip("/")
        self.api_base = api_base.rstrip("/")
        self.timeout = timeout
        self.session_id: str | None = None
        self._next_id = 1
        self._initialized = False

    def _require_key(self) -> str:
        if not self.api_key:
            raise TrendTrackError(
                "Missing TrendTrack API key. Set TRENDTRACK_API_KEY, add it to .env, "
                f"or run: {Path(sys.argv[0]).name} auth set"
            )
        return self.api_key

    def _http_request(
        self,
        url: str,
        method: str = "GET",
        payload: dict[str, Any] | None = None,
        auth_required: bool = True,
        extra_headers: dict[str, str] | None = None,
    ) -> tuple[int, dict[str, str], bytes]:
        headers: dict[str, str] = {
            "Accept": "application/json, text/event-stream",
            "User-Agent": "thirdmarvin-trendtrack-mcp/1.0",
        }
        if auth_required:
            headers["Authorization"] = f"Bearer {self._require_key()}"
        if extra_headers:
            headers.update(extra_headers)
        data: bytes | None = None
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"

        req = urllib.request.Request(url=url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                resp_headers = {k.lower(): v for k, v in resp.headers.items()}
                return resp.status, resp_headers, resp.read()
        except urllib.error.HTTPError as exc:
            err_body = exc.read().decode("utf-8", errors="replace")
            raise TrendTrackError(f"HTTP {exc.code} from {url}: {err_body}") from exc
        except urllib.error.URLError as exc:
            raise TrendTrackError(f"Network error calling {url}: {exc.reason}") from exc

    def _mcp_rpc(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        is_notification: bool = False,
    ) -> dict[str, Any] | None:
        rpc_id: int | None = None
        body: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
        if not is_notification:
            rpc_id = self._next_id
            self._next_id += 1
            body["id"] = rpc_id
        rpc_params = dict(params or {})
        meta = dict(rpc_params.get("_meta") or {})
        meta["io.modelcontextprotocol/protocolVersion"] = MCP_PROTOCOL_VERSION
        meta.setdefault("io.modelcontextprotocol/clientCapabilities", {})
        rpc_params["_meta"] = meta
        body["params"] = rpc_params

        extra_headers: dict[str, str] = {
            "MCP-Protocol-Version": MCP_PROTOCOL_VERSION,
            "Mcp-Method": method,
        }
        if method == "tools/call" and "name" in rpc_params:
            extra_headers["Mcp-Name"] = str(rpc_params["name"])
        if self.session_id:
            extra_headers["Mcp-Session-Id"] = self.session_id

        _, resp_headers, raw = self._http_request(
            self.mcp_url,
            method="POST",
            payload=body,
            auth_required=True,
            extra_headers=extra_headers,
        )
        if "mcp-session-id" in resp_headers:
            self.session_id = resp_headers["mcp-session-id"]

        if is_notification:
            return None
        parsed = decode_mcp_response(raw, resp_headers.get("content-type", ""), rpc_id)
        if parsed is None:
            raise TrendTrackError(f"Empty MCP response for method {method}")
        if "error" in parsed:
            raise TrendTrackError(f"MCP error ({method}): {json.dumps(parsed['error'])}")
        return parsed.get("result")

    def initialize(self) -> dict[str, Any]:
        if self._initialized:
            return {}
        if MCP_PROTOCOL_VERSION == "2026-07-28":
            # MCP 2026-07-28 stateless transport sends per-request metadata without initialize.
            self._initialized = True
            return {}
        try:
            result = self._mcp_rpc(
                "initialize",
                {
                    "protocolVersion": MCP_PROTOCOL_VERSION,
                    "capabilities": {},
                    "clientInfo": {"name": "thirdmarvin-trendtrack-mcp", "version": "1.0.0"},
                },
            )
            try:
                self._mcp_rpc("notifications/initialized", {}, is_notification=True)
            except TrendTrackError:
                pass
            self._initialized = True
            return result or {}
        except TrendTrackError as exc:
            if "not supported" in str(exc) or "-32601" in str(exc):
                self._initialized = True
                return {}
            raise

    def list_tools(self) -> list[dict[str, Any]]:
        self.initialize()
        result = self._mcp_rpc("tools/list", {}) or {}
        return result.get("tools", [])

    def call_tool(self, name: str, arguments: dict[str, Any] | None = None) -> dict[str, Any]:
        self.initialize()
        return self._mcp_rpc(
            "tools/call",
            {"name": name, "arguments": arguments or {}},
        ) or {}

    def rest_get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        auth_required: bool = True,
    ) -> dict[str, Any]:
        url = f"{self.api_base}{path}"
        if params:
            clean_params = {k: v for k, v in params.items() if v is not None}
            if clean_params:
                url = f"{url}?{urllib.parse.urlencode(clean_params, doseq=True)}"
        _, _, raw = self._http_request(url, method="GET", auth_required=auth_required)
        return json.loads(raw.decode("utf-8"))

    def rest_post(
        self,
        path: str,
        payload: dict[str, Any],
        auth_required: bool = True,
    ) -> dict[str, Any]:
        url = f"{self.api_base}{path}"
        _, _, raw = self._http_request(url, method="POST", payload=payload, auth_required=auth_required)
        return json.loads(raw.decode("utf-8"))


def parse_kv_args(items: list[str]) -> dict[str, Any]:
    parsed: dict[str, Any] = {}
    for item in items:
        if "=" not in item:
            raise TrendTrackError(f"Invalid --arg '{item}'. Expected key=value.")
        k, raw_v = item.split("=", 1)
        k = k.strip()
        raw_v = raw_v.strip()
        if raw_v.lower() in {"true", "false"}:
            val: Any = raw_v.lower() == "true"
        elif raw_v.isdigit():
            val = int(raw_v)
        else:
            try:
                val = json.loads(raw_v)
            except json.JSONDecodeError:
                val = raw_v
        parsed[k] = val
    return parsed


def format_tool_result(result: dict[str, Any], raw_json: bool = False) -> str:
    if raw_json:
        return json.dumps(result, indent=2, ensure_ascii=False)
    content = result.get("content")
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text":
                text = item.get("text", "")
                try:
                    parsed = json.loads(text)
                    parts.append(json.dumps(parsed, indent=2, ensure_ascii=False))
                except json.JSONDecodeError:
                    parts.append(text)
            else:
                parts.append(json.dumps(item, indent=2, ensure_ascii=False))
        return "\n".join(parts)
    return json.dumps(result, indent=2, ensure_ascii=False)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="trendtrack",
        description="Search & query TrendTrack via Streamable HTTP MCP and REST API.",
    )
    parser.add_argument("--api-key", help="Override TrendTrack API key (otherwise reads env / .env / ~/.gemini/trendtrack_api_key).")
    parser.add_argument("--mcp-url", default=DEFAULT_MCP_URL, help=f"MCP endpoint URL (default: {DEFAULT_MCP_URL}).")
    parser.add_argument("--raw", action="store_true", help="Output raw JSON envelope.")

    sub = parser.add_subparsers(dest="command", required=True)

    # auth
    auth_p = sub.add_parser("auth", help="Manage local TrendTrack API key.")
    auth_sub = auth_p.add_subparsers(dest="auth_cmd", required=True)
    auth_set = auth_sub.add_parser("set", help="Save API key to ~/.gemini/trendtrack_api_key (reads stdin if --stdin or omitted).")
    auth_set.add_argument("--key", help="API key string (prefer TRENDTRACK_API_KEY env or stdin to avoid shell history).")
    auth_set.add_argument("--stdin", action="store_true", help="Read API key from stdin.")
    auth_sub.add_parser("status", help="Show whether an API key is configured (masked).")

    # freshness
    sub.add_parser("freshness", help="Check TrendTrack data freshness (zero credits, no auth required).")

    # tools
    tools_p = sub.add_parser("tools", help="List live MCP tools from https://api.trendtrack.io/v1/mcp.")
    tools_p.add_argument("--schema", metavar="TOOL_NAME", help="Show full JSON inputSchema for one tool.")

    # call
    call_p = sub.add_parser("call", help="Call any TrendTrack MCP tool by name.")
    call_p.add_argument("tool_name", help="Exact MCP tool name (e.g., search_shops, find_winning_products).")
    call_p.add_argument("--json", dest="json_args", help="JSON object string of arguments.")
    call_p.add_argument("--arg", action="append", default=[], help="Key=value argument (repeatable).")

    # Convenience MCP search wrappers
    shops_p = sub.add_parser("search-shops", help="Search Shopify stores via MCP tool `search_shops`.")
    shops_p.add_argument("query", help="Search query (e.g., 'sleep aids', 'collagen gummies').")
    shops_p.add_argument("--limit", type=int, default=10, help="Max results (default: 10).")
    shops_p.add_argument("--arg", action="append", default=[], help="Extra key=value MCP arguments.")

    prod_p = sub.add_parser("search-products", help="Search Shopify products via MCP tool `search_products`.")
    prod_p.add_argument("query", help="Product search query.")
    prod_p.add_argument("--limit", type=int, default=10, help="Max results (default: 10).")
    prod_p.add_argument("--arg", action="append", default=[], help="Extra key=value MCP arguments.")

    lp_p = sub.add_parser("search-landing-pages", help="Search landing pages via MCP tool `search_landing_pages`.")
    lp_p.add_argument("query", help="Landing page search query.")
    lp_p.add_argument("--limit", type=int, default=10, help="Max results (default: 10).")
    lp_p.add_argument("--arg", action="append", default=[], help="Extra key=value MCP arguments.")

    win_p = sub.add_parser("winning-products", help="Find winning products via MCP tool `find_winning_products`.")
    win_p.add_argument("query", help="Niche or category query (e.g., 'pet supplies').")
    win_p.add_argument("--limit", type=int, help="Max results (maps to max_results).")
    win_p.add_argument("--arg", action="append", default=[], help="Extra key=value MCP arguments.")

    radar_p = sub.add_parser("product-radar", help="Find newly launched breakout products via MCP tool `product_launch_radar`.")
    radar_p.add_argument("query", nargs="?", default="", help="Optional product query.")
    radar_p.add_argument("--limit", type=int, default=10, help="Max results (default: 10).")
    radar_p.add_argument("--arg", action="append", default=[], help="Extra key=value MCP arguments.")

    sim_p = sub.add_parser("similar-shops", help="Find lookalike stores via MCP tool `find_similar_shops`.")
    sim_p.add_argument("domain", help="Reference domain (e.g., 'glossier.com').")
    sim_p.add_argument("--limit", type=int, default=10, help="Max results (default: 10).")
    sim_p.add_argument("--arg", action="append", default=[], help="Extra key=value MCP arguments.")

    pack_p = sub.add_parser("creative-pack", help="Pull hooks & LP patterns via MCP tool `creative_inspiration_pack`.")
    pack_p.add_argument("niche", help="Niche or vertical (e.g., 'skincare').")
    pack_p.add_argument("--arg", action="append", default=[], help="Extra key=value MCP arguments.")

    brief_p = sub.add_parser("brief", help="Generate competitor brief via MCP tool `brief_competitor`.")
    brief_p.add_argument("domain", help="Competitor domain (e.g., 'gymshark.com').")
    brief_p.add_argument("--arg", action="append", default=[], help="Extra key=value MCP arguments.")

    scan_p = sub.add_parser("scan-ad", help="Inspect a Meta ad via MCP tool `scan_ad`.")
    scan_p.add_argument("target", help="Meta Ad URL or ID.")
    scan_p.add_argument("--arg", action="append", default=[], help="Extra key=value MCP arguments.")

    sub.add_parser("credits", help="Check remaining credits via MCP tool `check_credits`.")
    sub.add_parser("radar", help="Check tracked brand updates via MCP tool `daily_radar`.")
    sub.add_parser("tracked", help="List tracked brands via MCP tool `list_tracked_brands`.")

    lookup_p = sub.add_parser("lookup", help="Zero-credit brand/domain/page lookup via MCP tool `lookup`.")
    lookup_p.add_argument("q", help="Brand name, domain, Facebook page ID, or Instagram handle.")
    lookup_p.add_argument("--limit", type=int, default=10, help="Max results (default: 10).")

    ads_p = sub.add_parser("search-ads", help="Search Meta ads via MCP tool `search_ads`.")
    ads_p.add_argument("search", help="Search keywords.")
    ads_p.add_argument("--limit", type=int, default=10, help="Results per page (default: 10).")
    ads_p.add_argument("--arg", action="append", default=[], help="Extra key=value MCP arguments.")

    tt_p = sub.add_parser("search-tiktok", help="Search TikTok library via MCP tool `search_tiktok_library`.")
    tt_p.add_argument("search", help="Search keywords.")
    tt_p.add_argument("--type", default="all", choices=["all", "ad", "organic"], help="Item type.")
    tt_p.add_argument("--limit", type=int, default=10, help="Results per page (default: 10).")
    tt_p.add_argument("--arg", action="append", default=[], help="Extra key=value MCP arguments.")

    ga_p = sub.add_parser("search-google-ads", help="Search Google Ads via MCP tool `search_google_ads_library`.")
    ga_p.add_argument("search", help="Search keywords.")
    ga_p.add_argument("--limit", type=int, default=10, help="Results per page (default: 10).")
    ga_p.add_argument("--status", default="active", help="Ad status (default: active).")
    ga_p.add_argument("--arg", action="append", default=[], help="Extra key=value MCP arguments.")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "auth":
        if args.auth_cmd == "set":
            key_val = args.key
            if not key_val:
                if sys.stdin.isatty() and not args.stdin:
                    print("Paste TRENDTRACK_API_KEY and press Enter:", file=sys.stderr)
                key_val = sys.stdin.readline().strip()
            path = save_api_key(key_val)
            print(f"Saved TrendTrack API key ({mask_key(key_val)}) to {path}")
            return 0
        if args.auth_cmd == "status":
            resolved = resolve_api_key(args.api_key)
            if resolved:
                print(f"Configured: yes ({mask_key(resolved)})")
                return 0
            print("Configured: no (set TRENDTRACK_API_KEY or run `trendtrack auth set`)")
            return 1

    api_key = resolve_api_key(args.api_key)
    client = TrendTrackClient(api_key=api_key, mcp_url=args.mcp_url)

    try:
        if args.command == "freshness":
            res = client.rest_get("/v1/system/freshness", auth_required=False)
            print(json.dumps(res, indent=2, ensure_ascii=False))
            return 0

        if args.command == "tools":
            tools = client.list_tools()
            if args.schema:
                matched = [t for t in tools if t.get("name") == args.schema]
                if not matched:
                    raise TrendTrackError(f"Tool '{args.schema}' not found.")
                print(json.dumps(matched[0], indent=2, ensure_ascii=False))
                return 0
            if args.raw:
                print(json.dumps(tools, indent=2, ensure_ascii=False))
                return 0
            for tool in tools:
                name = tool.get("name", "")
                desc = (tool.get("description") or "").strip().splitlines()[0] if tool.get("description") else ""
                props = list(tool.get("inputSchema", {}).get("properties", {}).keys())
                print(f"- {name} ({', '.join(props)}): {desc}")
            return 0

        if args.command == "call":
            payload: dict[str, Any] = {}
            if args.json_args:
                payload.update(json.loads(args.json_args))
            if args.arg:
                payload.update(parse_kv_args(args.arg))
            res = client.call_tool(args.tool_name, payload)
            print(format_tool_result(res, raw_json=args.raw))
            return 0

        # Direct MCP tool bindings verified against https://api.trendtrack.io/v1/mcp
        mcp_shortcuts: dict[str, tuple[str, dict[str, Any]]] = {
            "search-shops": ("search_shops", {"query": getattr(args, "query", ""), "limit": getattr(args, "limit", None)}),
            "search-products": ("search_products", {"query": getattr(args, "query", ""), "limit": getattr(args, "limit", None)}),
            "search-landing-pages": ("search_landing_pages", {"query": getattr(args, "query", ""), "limit": getattr(args, "limit", None)}),
            "winning-products": ("find_winning_products", {"niche": getattr(args, "query", ""), "max_results": getattr(args, "limit", None)}),
            "product-radar": ("product_launch_radar", {"query": getattr(args, "query", "") or None, "limit": getattr(args, "limit", None)}),
            "similar-shops": ("find_similar_shops", {"shop": getattr(args, "domain", ""), "limit": getattr(args, "limit", None)}),
            "creative-pack": ("creative_inspiration_pack", {"niche": getattr(args, "niche", "")}),
            "brief": ("brief_competitor", {"competitor": getattr(args, "domain", "")}),
            "scan-ad": ("scan_ad", {"ad_identifier": getattr(args, "target", "")}),
            "lookup": ("lookup", {"q": getattr(args, "q", ""), "limit": getattr(args, "limit", None)}),
            "search-ads": ("search_ads", {"query": getattr(args, "search", ""), "limit": getattr(args, "limit", None)}),
            "search-tiktok": ("search_tiktok_library", {"query": getattr(args, "search", ""), "type": getattr(args, "type", "all"), "limit": getattr(args, "limit", None)}),
            "search-google-ads": ("search_google_ads_library", {"query": getattr(args, "search", ""), "status": getattr(args, "status", "active"), "limit": getattr(args, "limit", None)}),
        }

        if args.command in mcp_shortcuts:
            tool_name, base_payload = mcp_shortcuts[args.command]
            payload = {k: v for k, v in base_payload.items() if v is not None}
            if getattr(args, "arg", None):
                payload.update(parse_kv_args(args.arg))
            res = client.call_tool(tool_name, payload)
            print(format_tool_result(res, raw_json=args.raw))
            return 0

        if args.command == "credits":
            print(format_tool_result(client.call_tool("check_credits", {}), raw_json=args.raw))
            return 0
        if args.command == "radar":
            print(format_tool_result(client.call_tool("daily_radar", {}), raw_json=args.raw))
            return 0
        if args.command == "tracked":
            print(format_tool_result(client.call_tool("list_tracked_brands", {}), raw_json=args.raw))
            return 0

    except TrendTrackError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
