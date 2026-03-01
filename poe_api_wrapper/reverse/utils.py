import os, string, secrets, base64, json, re
from urllib.parse import urlparse
from httpx import Client
from loguru import logger
from typing import Any, Optional

BASE_URL = 'https://poe.com'
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 Edg/115.0.1901.203",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.5",
    "Sec-Ch-Ua": '"Microsoft Edge";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Upgrade-Insecure-Requests": "1",
    "Origin": "https://poe.com",
    "Referer": "https://poe.com/",
}

SubscriptionsMutation = {
    "subscriptions":[
        {"subscriptionName":"chatMarkedAsUnseen","query":None,"queryHash":"490c2f0667243e085e01c8318618cb978a16840929fff630272a44e1a2d748e2"},
        {"subscriptionName":"messageAdded","query":None,"queryHash":"9bc4af8951e2f446fdd00399a4823b9b73c96a767b86a92078269e64725aa934"},
        {"subscriptionName":"messageCancelled","query":None,"queryHash":"14647e90e5960ec81fa83ae53d270462c3743199fbb6c4f26f40f4c83116d2ff"},
        {"subscriptionName":"messageDeleted","query":None,"queryHash":"da44e3fdd05dfde85d1c035471858e7201b0ef7ea7728df75b71707ffb2a5aa0"},
        {"subscriptionName":"messageDeletedV2","query":None,"queryHash":"6d0901c4738df2ee3b6cf737a244af7a2b3b26535149a1128a08b4cca2465eab"},
        {"subscriptionName":"messageReadV2","query":None,"queryHash":"35cd1f2905e26147effb38ad67280c54a8f13fbb67a36bbe6080619e70207aab"},
        {"subscriptionName":"messageReadByViewer","query":None,"queryHash":"984ea258538cf29e706807856070b5e95e25870089cdac7619d4499a441ef3dd"},
        {"subscriptionName":"messageEdited","query":None,"queryHash":"dad338006f002ede73ac0bf594ad22e104a1cd8bf43b6e605a0d79c97a28578e"},
        {"subscriptionName":"messageCreated","query":None,"queryHash":"7e94f09b61e69c5621c3af8de849d7a87cfa469a577ea0a63a2430cda9968f36"},
        {"subscriptionName":"messageStateUpdated","query":None,"queryHash":"117a49c685b4343e7e50b097b10a13b9555fedd61d3bf4030c450dccbeef5676"},
        {"subscriptionName":"messageAttachmentAdded","query":None,"queryHash":"65798bb2f409d9457fc84698479f3f04186d47558c3d7e75b3223b6799b6788d"},
        {"subscriptionName":"messageFollowupActionAdded","query":None,"queryHash":"d2e770beae7c217c77db4918ed93e848ae77df668603bc84146c161db149a2c7"},
        {"subscriptionName":"messageMetadataUpdated","query":None,"queryHash":"71c247d997d73fb0911089c1a77d5d8b8503289bc3701f9fb93c9b13df95aaa6"},
        {"subscriptionName":"messageReactionsUpdated","query":None,"queryHash":"975515f0096656b4f98508da9f015b24cfe63b1e44f06ab0e27fae4fa23ecae3"},
        {"subscriptionName":"messageTextUpdated","query":None,"queryHash":"800eea48edc9c3a81aece34f5f1ff40dc8daa71dead9aec28f2b55523fe61231"},
        {"subscriptionName":"jobStarted","query":None,"queryHash":"c8b0a1fa651db0384cb8bc56bbe4f1d6c0cef28bd1e8176c0d0be2a20bb75fc7"},
        {"subscriptionName":"jobUpdated","query":None,"queryHash":"961c92a9b49fa30e67ee1a9c6a276a92829aecf9135000d6ba69efaf15df91a3"},
        {"subscriptionName":"jobCostUpdated","query":None,"queryHash":"a72e7590abe1c18fcb9dfcaf43103a35d86eef966197c28db6f5876c00c0a74c"},
        {"subscriptionName":"viewerStateUpdated","query":None,"queryHash":"0c9cbf7513f7af97f419eec1ca6d848fa6425d2715e03318110a41f9a08fb0b6"},
        {"subscriptionName":"canvasTabClosed","query":None,"queryHash":"7b26a0f049c7742c4484ea0cbd98dcdd97f31638fd01c0ee988b8cbf9e967328"},
        {"subscriptionName":"canvasTabOpened","query":None,"queryHash":"49ad669f3eb1f83366b42947f2704a1ce3157062f963b54a535a9e9b06242a15"},
        {"subscriptionName":"canvasTabBackgrounded","query":None,"queryHash":"08158bb1faedc52a0c6f42ecfbc3afe7ffaec22dcd92731a6ce3d870273c6553"},
        {"subscriptionName":"onDataToForwardToCanvasTab","query":None,"queryHash":"3306309cb5a1d7e19867ced094f779a22d28c5c6fc617dfa136d11f51c7cee0c"},
        {"subscriptionName":"chatTitleUpdated","query":None,"queryHash":"ee062b1f269ecd02ea4c2a3f1e4b2f222f7574c43634a2da4ebeb616d8647e06"},
        {"subscriptionName":"chatDeletedV2","query":None,"queryHash":"847388a799b3c8ce51ecf0b9aa9163242f409cd5432b4e49c0f71014c66b04eb"},
        {"subscriptionName":"chatDeletedV3","query":None,"queryHash":"51e2f60da61b7ef9b8a5bd85e95e3b5ce17c1a6651cf692014a3aaae3fd3e458"},
        {"subscriptionName":"chatAddedV2","query":None,"queryHash":"a5206e59f0e7fbd603921ee111b7a48f7dbbd55270ce1f69cdd3d1039911ca67"},
        {"subscriptionName":"knowledgeSourceUpdated","query":None,"queryHash":"7de63f89277bcf54f2323008850573809595dcef687f26a78561910cfd4f6c37"},
        {"subscriptionName":"chatMemberAddedWithContext","query":None,"queryHash":"e9c6a41bf62e876fa4cb735f31c54ca75eeac0af170019df6ab1d6396ed94726"},
        {"subscriptionName":"chatMemberRemoved","query":None,"queryHash":"fc640db462e081168e20c8194fe6e7e219228b6fa2bd19735158eb066507e212"},
        {"subscriptionName":"userRemovedFromChat","query":None,"queryHash":"ac46d057797562a5d0f1d2c05410cd5d42da3e6f54e07cc56629d307d0df0944"},
        {"subscriptionName":"userRemovedFromChatV2","query":None,"queryHash":"bbefe65bc9ebcf6d053ec46ce054abd2bb95e3b77c25136b0ac1b79a77b061b0"},
        {"subscriptionName":"chatSettingsUpdated","query":None,"queryHash":"76806efbd2d584ba9e2e515f79045a8fb2015ecb5b8e9a25bade2843fcf5fee7"},
        {"subscriptionName":"chatModalStateChanged","query":None,"queryHash":"679b09b1c45951a4d7e6a4b72bb248819de70c94b8043a0f5b81fd267abf2269"},
        {"subscriptionName":"defaultBotOfChatChanged","query":None,"queryHash":"2bacc089fc4432fae0fa9952b2dbb5150af9be1fc67a04438b21547af5093173"},
        {"subscriptionName":"messageFollowupActionUpdated","query":None,"queryHash":"be87f2ee5ba5cef5980ec863ad6bc217f921f16032c0cb028ee8af522cf2136b"},
        {"subscriptionName":"chatMuteStatusUpdated","query":None,"queryHash":"97563dd19a520bacc607de851c84df0c75bc882ecff92aa37ac2c0b3b49e8ed3"},
        {"subscriptionName":"chatPinStatusUpdated","query":None,"queryHash":"45ad3d66ca2d705b26a0c6a1da34955dac057ae9c2ceacb149fc9f5428e5d753"},
        {"subscriptionName":"allChatsDeleted","query":None,"queryHash":"cdb3791da2adab8bb7d756b79dd2d4231df6409f4da2d90002ef5fe3f21aa175"},
        {"subscriptionName":"multiplayerNuxCompleted","query":None,"queryHash":"8e171fe2c2ae61897874d04417344ca9c6313547e077dfec6a772ab1b5290f4f"},
        {"subscriptionName":"userFollowersUpdated","query":None,"queryHash":"4adbb81a4652b86232dabe5c63297dc0d62a909001bc33d4d9f508a5b7b9b66d"},
        {"subscriptionName":"userFollowingUpdated","query":None,"queryHash":"976349f61a530a41b94c29e64423885beec722fcbc8635d659e83430600756bb"}]
}


BOTS_LIST = {
    'Assistant': 'capybara',
    'Claude-3.5-Sonnet': 'claude_3_igloo',
    'Claude-3-Opus': 'claude_2_1_cedar',
    'Claude-3-Sonnet': 'claude_2_1_bamboo',
    'Claude-3-Haiku': 'claude_3_haiku',
    'Claude-3-Opus-200k': 'claude_3_opus_200k',
    'Claude-3.5-Sonnet-200k': 'claude_3_igloo_200k',
    'Claude-3-Sonnet-200k': 'claude_3_sonnet_200k',
    'Claude-3-Haiku-200k': 'claude_3_haiku_200k',
    'Claude-2': 'claude_2_short',
    'Claude-2-100k': 'a2_2',
    'Claude-instant': 'a2',
    'Claude-instant-100k': 'a2_100k',
    'GPT-3.5-Turbo': 'chinchilla',
    'GPT-3.5-Turbo-Raw': 'gpt3_5',
    'GPT-3.5-Turbo-Instruct': 'chinchilla_instruct',
    'ChatGPT-16k': 'agouti',
    'GPT-4-Classic': 'gpt4_classic',
    'GPT-4-Turbo': 'beaver',
    'GPT-4-Turbo-128k': 'vizcacha',
    'GPT-4o': 'gpt4_o',
    'GPT-4o-128k': 'gpt4_o_128k',
    'GPT-4o-Mini': 'gpt4_o_mini',
    'GPT-4o-Mini-128k': 'gpt4_o_mini_128k',
    'Google-PaLM': 'acouchy',
    'Code-Llama-13b': 'code_llama_13b_instruct',
    'Code-Llama-34b': 'code_llama_34b_instruct',
    'Solar-Mini':'upstage_solar_0_70b_16bit',
    'Gemini-1.5-Flash-Search': 'gemini_pro_search',
    'Gemini-1.5-Pro-2M': 'gemini_1_5_pro_1m',
}

REVERSE_BOTS_LIST = {v: k for k, v in BOTS_LIST.items()}

EXTENSIONS = {
    '.md': 'application/octet-stream',
    '.lua': 'application/octet-stream',
    '.rs': 'application/octet-stream',
    '.rb': 'application/octet-stream',
    '.go': 'application/octet-stream',
    '.java': 'application/octet-stream',
    '.pdf': 'application/pdf',
    '.docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    '.txt': 'text/plain',
    '.py': 'text/x-python',
    '.js': 'text/javascript',
    '.ts': 'text/plain',
    '.html': 'text/html',
    '.css': 'text/css',
    '.csv': 'text/csv',
    '.c' : 'text/plain',
    '.cs': 'text/plain',
    '.cpp': 'text/plain',
}

MEDIA_EXTENSIONS = {
    '.png': 'image/png',
    '.jpg': 'image/jpeg',
    '.jpeg': 'image/jpeg',
    '.gif': 'image/gif',
    '.mp4': 'video/mp4',
    '.mov': 'video/quicktime',
    '.mp3': 'audio/mpeg',
    '.wav': 'audio/wav',
}

def bot_map(bot):
    if bot in BOTS_LIST:
        return BOTS_LIST[bot]
    return bot.lower().replace(' ', '')

def generate_nonce(length:int=16):
      return "".join(secrets.choice(string.ascii_letters + string.digits) for i in range(length))

def is_valid_url(url):
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False
    
def generate_file(file_path: list, proxy: dict=None):
    files = []   
    file_size = 0
    for file in file_path:
        if isinstance(file, str) and file.startswith("data:image"):
            file_extension = file.split(";")[0].split("/")[-1]
            content_type = MEDIA_EXTENSIONS.get(f".{file_extension}", "image/png")
            file_data = base64.b64decode(file.split(",")[1])
            file_name = f"{generate_nonce(8)}.{file_extension}"
            files.append((file_name, file_data, content_type))
            file_size += len(file_data)
            
        elif is_valid_url(file):
            # Handle URL files
            file_name = file.split('/')[-1]
            file_extension = os.path.splitext(file_name)[1].lower()
            content_type = MEDIA_EXTENSIONS.get(file_extension, EXTENSIONS.get(file_extension, None))
            if not content_type:
                raise RuntimeError("This file type is not supported. Please try again with a different file.")
            logger.info(f"Downloading file from {file}")
            # httpx changed proxy kwarg across versions (proxy vs proxies).
            # Build client in a version-compatible way.
            client_kwargs = {"http2": True}
            if proxy:
                client_kwargs["proxy"] = proxy
            try:
                fetcher = Client(**client_kwargs)
            except TypeError:
                if "proxy" in client_kwargs:
                    legacy_kwargs = {"http2": True, "proxies": client_kwargs["proxy"]}
                    fetcher = Client(**legacy_kwargs)
                else:
                    raise
            with fetcher:
                response = fetcher.get(file)
                file_data = response.content
            files.append((file_name, file_data, content_type))
            file_size += len(file_data)
        else:
            # Handle local files
            file_extension = os.path.splitext(file)[1].lower()
            content_type = MEDIA_EXTENSIONS.get(file_extension, EXTENSIONS.get(file_extension, None))
            if not content_type:
                raise RuntimeError("This file type is not supported. Please try again with a different file.")
            file_name = os.path.basename(file)
            with open(file, 'rb') as f:
                file_data = f.read()
                files.append((file_name, file_data, content_type))
                file_size += len(file_data)
    return files, file_size


_NEXT_DATA_SCRIPT_RE = re.compile(
    r'<script[^>]*id=["\']__NEXT_DATA__["\'][^>]*>(?P<json>.*?)</script>',
    re.IGNORECASE | re.DOTALL,
)


def _deep_find_tchannel_data(obj: Any) -> Optional[dict]:
    if isinstance(obj, dict):
        candidate = obj.get("tchannelData")
        if isinstance(candidate, dict):
            required = {"minSeq", "channel", "channelHash", "boxName", "baseHost"}
            if required.issubset(candidate.keys()):
                return candidate
        for value in obj.values():
            found = _deep_find_tchannel_data(value)
            if found:
                return found
    elif isinstance(obj, list):
        for value in obj:
            found = _deep_find_tchannel_data(value)
            if found:
                return found
    return None


def _extract_balanced_json_object(source: str, start_idx: int) -> str:
    depth = 0
    in_string = False
    escaped = False
    buf: list[str] = []

    for idx in range(start_idx, len(source)):
        ch = source[idx]
        buf.append(ch)

        if in_string:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_string = False
            continue

        if ch == '"':
            in_string = True
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return "".join(buf)

    raise RuntimeError("Failed to extract balanced JSON object for tchannelData")


def extract_tchannel_data_from_html(html: str) -> dict:
    if not html or not html.strip():
        raise RuntimeError("Empty HTML response while extracting tchannelData")

    match = _NEXT_DATA_SCRIPT_RE.search(html)
    if match:
        raw_json = match.group("json").strip()
        try:
            payload = json.loads(raw_json)
            tchannel_data = _deep_find_tchannel_data(payload)
            if tchannel_data:
                return tchannel_data
        except Exception as exc:
            logger.warning("Failed parsing __NEXT_DATA__ JSON: {}", exc)

    key = '"tchannelData"'
    key_idx = html.find(key)
    if key_idx < 0:
        raise RuntimeError("tchannelData not found in Poe HTML response")

    brace_start = html.find("{", key_idx)
    if brace_start < 0:
        raise RuntimeError("tchannelData key found but JSON object start missing")

    raw_obj = _extract_balanced_json_object(html, brace_start)
    try:
        tchannel_data = json.loads(raw_obj)
    except Exception as exc:
        raise RuntimeError(f"Failed to parse tchannelData JSON object: {exc}") from exc

    required = {"minSeq", "channel", "channelHash", "boxName", "baseHost"}
    if not required.issubset(tchannel_data.keys()):
        raise RuntimeError(
            f"Incomplete tchannelData fields: expected {sorted(required)}, got {sorted(tchannel_data.keys())}"
        )
    return tchannel_data
