import importlib.util
import json
import os
import pathlib
import unittest
from unittest import mock


MGIT_PATH = pathlib.Path(__file__).with_name("mgit.py")
spec = importlib.util.spec_from_file_location("mgit", MGIT_PATH)
mgit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mgit)


class MiniMaxProviderTest(unittest.TestCase):
    def test_serialized_requests_cover_regions_protocols_and_model_selection(self):
        cases = [
            (
                "https://api.minimax.io/v1",
                None,
                "MiniMax-M3.1-Flash",
                "https://api.minimax.io/v1/chat/completions",
                False,
            ),
            (
                "https://api.minimaxi.com/v1",
                "MiniMax-M3",
                "MiniMax-M3",
                "https://api.minimaxi.com/v1/chat/completions",
                False,
            ),
            (
                "https://api.minimax.io/anthropic",
                "saved-custom-model",
                "saved-custom-model",
                "https://api.minimax.io/anthropic/v1/messages",
                True,
            ),
            (
                "https://api.minimaxi.com/anthropic",
                "abab6.5s-chat",
                "abab6.5s-chat",
                "https://api.minimaxi.com/anthropic/v1/messages",
                True,
            ),
        ]

        for api_base_url, configured_model, expected_model, expected_url, uses_anthropic_api in cases:
            with self.subTest(api_base_url=api_base_url, model=configured_model):
                env = {
                    "MINIMAX_API_KEY": "test-key",
                    "MINIMAX_API_BASE_URL": api_base_url,
                }
                if configured_model is not None:
                    env["MINIMAX_MODEL"] = configured_model

                with mock.patch.dict(os.environ, env, clear=True), mock.patch.object(
                    mgit.AIService, "_check_claude_cli", return_value=False
                ), mock.patch.object(mgit.urllib.request, "urlopen") as urlopen:
                    if uses_anthropic_api:
                        result = {
                            "content": [
                                {"type": "thinking", "thinking": "internal"},
                                {"type": "text", "text": '{"valid": true}'},
                            ]
                        }
                    else:
                        result = {
                            "choices": [
                                {"message": {"content": '{"valid": true}'}}
                            ]
                        }

                    response = mock.MagicMock()
                    response.__enter__.return_value.read.return_value = json.dumps(
                        result
                    ).encode("utf-8")
                    urlopen.return_value = response

                    service = mgit.AIService()
                    content = service._call_minimax_api(
                        "prompt", temperature=0.3, timeout=30
                    )

                request = urlopen.call_args.args[0]
                request_body = json.loads(request.data.decode("utf-8"))
                self.assertEqual(request.full_url, expected_url)
                self.assertEqual(request_body["model"], expected_model)
                self.assertEqual(
                    request_body["messages"], [{"role": "user", "content": "prompt"}]
                )
                self.assertEqual(content, '{"valid": true}')
                if uses_anthropic_api:
                    self.assertEqual(request.full_url.count("/v1/messages"), 1)
                    self.assertNotIn("reasoning_split", request_body)
                else:
                    self.assertTrue(request_body["reasoning_split"])
                    self.assertNotIn("service_tier", request_body)
                    self.assertNotIn("reasoning_effort", request_body)


if __name__ == "__main__":
    unittest.main()
