import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import trendtrack_mcp as tt


class TestTrendTrackMCP(unittest.TestCase):
    def test_parse_sse_json_messages(self) -> None:
        raw = (
            "event: message\n"
            'data: {"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2026-07-28"}}\n\n'
            "event: message\n"
            'data: {"jsonrpc":"2.0","id":2,"result":{"tools":[{"name":"search_shops"}]}}\n\n'
        )
        msgs = tt.parse_sse_json_messages(raw)
        self.assertEqual(len(msgs), 2)
        decoded = tt.decode_mcp_response(raw.encode("utf-8"), "text/event-stream", request_id=2)
        self.assertEqual(decoded["result"]["tools"][0]["name"], "search_shops")

    def test_save_and_resolve_api_key(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            key_path = Path(tmp) / "trendtrack_api_key"
            tt.save_api_key("tt_live_1234567890", key_file=key_path)
            self.assertEqual(key_path.read_text(encoding="utf-8").strip(), "tt_live_1234567890")
            self.assertEqual(tt.mask_key("tt_live_1234567890"), "tt_l...7890")

    def test_mcp_stateless_tools_list_and_call_tool(self) -> None:
        client = tt.TrendTrackClient(api_key="tt_test_key")
        responses = [
            (
                200,
                {"content-type": "application/json"},
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "result": {
                            "tools": [
                                {
                                    "name": "search_shops",
                                    "inputSchema": {"properties": {"query": {"type": "string"}}},
                                }
                            ]
                        },
                    }
                ).encode(),
            ),
            (
                200,
                {"content-type": "application/json"},
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": 2,
                        "result": {"content": [{"type": "text", "text": '{"shops": ["demo.myshopify.com"]}'}]},
                    }
                ).encode(),
            ),
        ]

        with mock.patch.object(client, "_http_request", side_effect=responses) as mocked:
            tools = client.list_tools()
            self.assertEqual(tools[0]["name"], "search_shops")
            res = client.call_tool("search_shops", {"query": "sleep aids"})
            self.assertIn("demo.myshopify.com", tt.format_tool_result(res))
            self.assertEqual(mocked.call_count, 2)

            first_headers = mocked.call_args_list[0].kwargs["extra_headers"]
            self.assertEqual(first_headers["MCP-Protocol-Version"], "2026-07-28")
            self.assertEqual(first_headers["Mcp-Method"], "tools/list")

            second_headers = mocked.call_args_list[1].kwargs["extra_headers"]
            self.assertEqual(second_headers["Mcp-Method"], "tools/call")
            self.assertEqual(second_headers["Mcp-Name"], "search_shops")
            second_payload = mocked.call_args_list[1].kwargs["payload"]
            self.assertEqual(
                second_payload["params"]["_meta"]["io.modelcontextprotocol/protocolVersion"],
                "2026-07-28",
            )

    def test_parse_kv_args(self) -> None:
        parsed = tt.parse_kv_args(["query=sleep aids", "limit=10", "active=true"])
        self.assertEqual(parsed, {"query": "sleep aids", "limit": 10, "active": True})


if __name__ == "__main__":
    unittest.main()
