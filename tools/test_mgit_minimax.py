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
    def test_official_api_bases_and_target_models(self):
        cases = [
            (
                "https://api.minimax.io/v1",
                "MiniMax-M3",
                "https://api.minimax.io/v1/chat/completions",
                False,
            ),
            (
                "https://api.minimaxi.com/v1",
                "MiniMax-M2.7",
                "https://api.minimaxi.com/v1/chat/completions",
                False,
            ),
            (
                "https://api.minimax.io/anthropic/v1",
                "MiniMax-M3",
                "https://api.minimax.io/anthropic/v1/messages",
                True,
            ),
            (
                "https://api.minimaxi.com/anthropic/v1",
                "MiniMax-M2.7",
                "https://api.minimaxi.com/anthropic/v1/messages",
                True,
            ),
        ]

        for api_base_url, model, expected_url, uses_anthropic_api in cases:
            with self.subTest(api_base_url=api_base_url, model=model):
                env = {
                    "MINIMAX_API_KEY": "test-key",
                    "MINIMAX_API_BASE_URL": api_base_url,
                    "MINIMAX_MODEL": model,
                }
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
                self.assertEqual(request_body["model"], model)
                self.assertEqual(content, '{"valid": true}')
                if uses_anthropic_api:
                    self.assertEqual(request.full_url.count("/v1/messages"), 1)


if __name__ == "__main__":
    unittest.main()
