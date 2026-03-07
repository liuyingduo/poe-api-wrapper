import importlib.util
from pathlib import Path


UTILS_PATH = Path(__file__).resolve().parents[1] / "poe_api_wrapper" / "reverse" / "utils.py"
spec = importlib.util.spec_from_file_location("reverse_utils", UTILS_PATH)
reverse_utils = importlib.util.module_from_spec(spec)
assert spec is not None and spec.loader is not None
spec.loader.exec_module(reverse_utils)
ensure_model_parameters = reverse_utils.ensure_model_parameters


def test_nano_banana_adds_image_only_when_missing():
    params = ensure_model_parameters("Nano-Banana-2", None)
    assert params == {"image_only": True}


def test_nano_banana_forces_image_only_true_for_dict():
    params = ensure_model_parameters("nano-banana-2", {"image_only": False, "web_search": False})
    assert params["image_only"] is True
    assert params["web_search"] is False


def test_nano_banana_forces_image_only_true_for_json_string():
    params = ensure_model_parameters("Nano-Banana-2", '{"web_search":false,"image_only":false}')
    assert isinstance(params, dict)
    assert params["image_only"] is True
    assert params["web_search"] is False


def test_non_nano_banana_keeps_original_parameters():
    original = {"web_search": False}
    params = ensure_model_parameters("GPT-4o", original)
    assert params == original
