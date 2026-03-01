from httpx import Client
from bs4 import BeautifulSoup
from loguru import logger
import quickjs
import re

try:
    import execjs
except ImportError:
    execjs = None

class PoeBundle:
    form_key_pattern = r"window\.([a-zA-Z0-9_]+)=function\(\)\{return window"
    seeded_call_pattern = r'window\.([a-zA-Z0-9_]+)\(\s*["\']([^"\']{16,})["\']\s*\)'
    window_secret_pattern = r'let useFormkeyDecode=[\s\S]*?(window\.[\w]+="[^"]+")'
    static_pattern = r'static[^"]*\.js'
    js_bootstrap = (
        "var window = {document:{hack:1},navigator:{userAgent:'Mozilla/5.0 "
        "(Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36'}};"
        "var process = undefined;"
        "var QuickJS = undefined;"
    )

    def __init__(self, document: str):
        self._window = self.js_bootstrap
        self._src_scripts = []
        self._webpack_script: str = None

        self.init_window(document)

    def init_window(self, document: str):
        # initialize the window object with document scripts
        logger.info("Initializing web data")

        scripts = BeautifulSoup(document, "html.parser").find_all('script')
        for script in scripts:
            if (src := script.attrs.get("src")) and (src not in self._src_scripts):
                if "_app" in src:
                    self.init_app(src)
                if "buildManifest" in src:
                    self.extend_src_scripts(src)
                elif "webpack" in src:
                    self._webpack_script = src
                    self.extend_src_scripts(src)
                else:
                    self._src_scripts.append(src)
            elif ("document." in script.text) or ("function" not in script.text):
                continue
            elif script.attrs.get("type") == "application/json":
                continue
            self._window += script.text

        logger.info("Web data initialized")

    def init_app(self, src: str):
        script = self.load_src_script(src)
        if not (window_secret_match := re.search(self.window_secret_pattern, script)):
            raise RuntimeError("Failed to find window secret in js scripts")
        
        self._window += window_secret_match.group(1) + ';'

    def extend_src_scripts(self, manifest_src: str):
        # extend src scripts list with static scripts from manifest
        static_main_url = self.get_base_url(manifest_src)
        manifest = self.load_src_script(manifest_src)

        matches = re.findall(self.static_pattern, manifest)
        scr_list = [f"{static_main_url}{match}" for match in matches]

        self._src_scripts.extend(scr_list)

    @staticmethod
    def load_src_script(src: str) -> str:
        with Client() as client:
            resp = client.get(src)
        if resp.status_code != 200:
            logger.warning(f"Failed to load script {src}, status code: {resp.status_code}")
        return resp.text

    @staticmethod
    def get_base_url(src: str) -> str:
        return src.split("static/")[0]

    def _resolve_formkey_expression(self) -> str:
        seeded_match = re.search(self.seeded_call_pattern, self._window)
        if seeded_match:
            function_name, seed = seeded_match.groups()
            return f'window.{function_name}("{seed}")'

        match = re.search(self.form_key_pattern, self._window)
        if match:
            function_name = match.group(1)
            return f"window.{function_name}()"

        raise RuntimeError("Failed to parse form-key function in Poe document")

    def _get_form_key_by_execjs(self, expression: str) -> str:
        if execjs is None:
            raise RuntimeError("PyExecJS is not installed")

        script = (
            self._window
            + f"\nfunction __poe_formkey__(){{return String({expression}).slice(0, 32);}}"
        )
        context = execjs.compile(script)
        return str(context.call("__poe_formkey__"))

    def _get_form_key_by_quickjs(self, expression: str) -> str:
        script = self._window + f"\nString({expression}).slice(0, 32);"
        context = quickjs.Context()
        return str(context.eval(script))

    def get_form_key(self) -> str:
        expression = self._resolve_formkey_expression()

        try:
            formkey = self._get_form_key_by_execjs(expression)
            logger.info(f"Retrieved formkey successfully via execjs: {formkey}")
            return formkey
        except Exception as execjs_error:
            logger.warning(f"ExecJS formkey extraction failed: {execjs_error}")

        formkey = self._get_form_key_by_quickjs(expression)
        logger.info(f"Retrieved formkey successfully via quickjs fallback: {formkey}")
        return formkey
