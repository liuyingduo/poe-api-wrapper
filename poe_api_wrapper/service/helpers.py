import random, string, threading, time
from heapq import nlargest
from loguru import logger
from functools import lru_cache

from nltk.data import find as resource_find
from nltk import download as nltk_download

from nltk.corpus import stopwords
from nltk.probability import FreqDist
from nltk.stem import PorterStemmer
from nltk.tokenize import sent_tokenize, word_tokenize
import tiktoken
from fastapi import HTTPException

_NLTK_READY = False
_NLTK_READY_LOCK = threading.Lock()


def _ensure_nltk_resources_sync() -> None:
    global _NLTK_READY
    if _NLTK_READY:
        return
    with _NLTK_READY_LOCK:
        if _NLTK_READY:
            return
        missing: list[str] = []
        try:
            resource_find("corpora/stopwords")
        except LookupError:
            missing.append("stopwords")
        # New NLTK versions renamed 'punkt' to 'punkt_tab'; check both.
        try:
            resource_find("tokenizers/punkt_tab")
        except LookupError:
            try:
                resource_find("tokenizers/punkt")
            except LookupError:
                missing.append("punkt_tab")
        for package in missing:
            logger.warning("NLTK {} not found. Downloading...", package)
            nltk_download(package, quiet=True)
        _NLTK_READY = True


@lru_cache(maxsize=1)
def _token_encoder():
    return tiktoken.get_encoding("cl100k_base")


def _tokenize_sync(text: str) -> int:
    encoder = _token_encoder()
    return len(encoder.encode(text))


def _progressive_summarize_text_sync(
    text: str,
    max_length: int,
    initial_reduction_ratio: float = 0.8,
    step: float = 0.1,
) -> str:
    _ensure_nltk_resources_sync()
    current_tokens = _tokenize_sync(text)
    if current_tokens < max_length:
        return text

    stop_words = set(stopwords.words("english"))
    ps = PorterStemmer()
    sentences = sent_tokenize(text)

    words = [ps.stem(word) for word in word_tokenize(text.lower()) if word not in stop_words]
    word_freqs = FreqDist(words)
    sentence_scores = {
        idx: sum(word_freqs.get(word, 0) for word in word_tokenize(sentence.lower()))
        for idx, sentence in enumerate(sentences)
    }

    reduction_ratio = initial_reduction_ratio
    while True:
        num_sentences = max(1, round(len(sentences) * reduction_ratio))
        selected_indexes = nlargest(num_sentences, sentence_scores, key=sentence_scores.get)
        summary = "\n".join(sentences[idx] for idx in sorted(selected_indexes))
        if 0 < len(summary.strip()) <= max_length or reduction_ratio - step < 0:
            break
        reduction_ratio -= step

    return summary

async def __progressive_summarize_text(text, max_length, initial_reduction_ratio=0.8, step=0.1):
    return _progressive_summarize_text_sync(
        text,
        max_length,
        initial_reduction_ratio,
        step,
    )

async def __validate_messages_format_detail(messages):
    if messages is None:
        return False, "messages is required"
    if not isinstance(messages, list):
        return False, "messages must be a list"
    if len(messages) == 0:
        return False, "messages cannot be empty"

    for idx, message in enumerate(messages):
        if not isinstance(message, dict):
            return False, f"messages[{idx}] must be an object"
        if "role" not in message:
            return False, f"messages[{idx}] missing role"
        if not isinstance(message["role"], str):
            return False, f"messages[{idx}].role must be a string"
        role = message["role"].strip()
        if role == "":
            return False, f"messages[{idx}].role cannot be empty"

    if messages[0]["role"] == "assistant":
        return False, "messages[0].role cannot be assistant"
    if messages[-1]["role"] == "assistant":
        return False, f"messages[{len(messages) - 1}].role cannot be assistant"

    for idx, message in enumerate(messages[1:], start=1):
        if message["role"] == "system":
            return False, f"messages[{idx}].role cannot be system (system is only allowed at index 0)"

    return True, ""


async def __validate_messages_format(messages):
    valid, _ = await __validate_messages_format_detail(messages)
    return valid

async def __split_content(messages):
    text_messages = []
    attachment_urls = []
    
    for message in messages:
        if "content" in message and isinstance(message["content"], list):
            for item in message["content"]:
                if item["type"] == "text":
                    if "text" in item:
                        text_messages.append({"role": message["role"], "content": item["text"]})
                elif item["type"] == "image_url":
                    # 图片附件：{"type": "image_url", "image_url": {"url": "..."}}
                    if "image_url" in item:
                        if isinstance(item["image_url"], str):
                            if item["image_url"] not in attachment_urls:
                                attachment_urls.append(item["image_url"])
                        elif isinstance(item["image_url"], dict) and "url" in item["image_url"]:
                            if item["image_url"]["url"] not in attachment_urls:
                                attachment_urls.append(item["image_url"]["url"])
                        else:
                            logger.error(f"Invalid image_url format: {item['image_url']}")
                elif item["type"] == "file":
                    # 文件附件（PDF/文档等）：{"type": "file", "file": {"url": "..."}}
                    file_info = item.get("file") or {}
                    url = file_info.get("url") or file_info.get("file_url") or ""
                    if url and url not in attachment_urls:
                        attachment_urls.append(url)
        elif "content" in message and isinstance(message["content"], str):
            text_messages.append({"role": message["role"], "content": message["content"]})  
               
    return text_messages, attachment_urls

async def __generate_completion_id():
    return "".join(random.choices(string.ascii_letters + string.digits, k=28))

async def __generate_timestamp():
    return int(time.time())

async def __tokenize(text):
    return _tokenize_sync(text)

async def __stringify_messages(messages):
    return '\n'.join(f"<{message['role'].capitalize()}>{message['content']}</{message['role'].capitalize()}>" for message in messages)

async def __add_tools_prompt(messages):
    for message in messages:
        if message["role"] == "tool":
            if messages[0]["role"] == "system":
                messages[0]["content"] += f"\nTool: {message['content']}"
            else:
                messages.insert(0, {"role": "system", "content":""})
            
    # remove messsage that has tool_calls key
    messages = [message for message in messages if "tool_calls" not in message and message["role"] != "tool"]
    return messages


async def __convert_functions_format(input_data, tool_choice="auto"):
    try:
        if isinstance(tool_choice, dict):
            if len(tool_choice) == 2 and ("type" in tool_choice and tool_choice["type"] == "function" and "function" in tool_choice and "name" in tool_choice["function"]):
                tool_choice = tool_choice["function"]["name"]
            else:
                raise HTTPException(detail={"error": {
                                            "message": """Invalid tool choice format. Must be {"type": "function", "function": {"name": "my_function"}}""",
                                            "type": "error", 
                                            "param": None, 
                                            "code": 400}
                                        }, status_code=400)
        elif isinstance(tool_choice, str):
            if tool_choice not in ("auto", "required"):
                raise HTTPException(detail={"error": {
                                            "message": "Invalid tool choice format. Must be 'auto' or 'required'",
                                            "type": "error", 
                                            "param": None, 
                                            "code": 400}
                                        }, status_code=400)
        output = """Tools
functions
namespace functions {\n"""

        for function in input_data:
            function_name = function['function']['name']
            description = function['function']['description']
            
            properties = function['function']['parameters']['properties']
            required = function['function']['parameters'].get('required', [])
            params = []
            
            for prop_name, prop_info in properties.items():
                param_desc = f"// {prop_info.get('description', '')}\n    {prop_name}: {prop_info['type']}"
                if 'enum' in prop_info:
                    enum_values = ' | '.join(prop_info['enum']).lower()
                    param_desc += f" | {enum_values}"
                # Check if the parameter is required
                if prop_name in required:
                    param_desc += " // required"
                params.append(param_desc)

            params_string = ",\n".join(params)

            output += f"""// {description}
type {function_name} = (: {{
{params_string}
}}) => any;\n\n"""

        output += """} // namespace functions
// Each tool that can be used must be specified in the tool sections.
// Ensure that the arguments provided to each tool are valid according to that tool's specification.
List of dictionaries: [{
// The name of the tool to use.
The format should be just the name of the tool for plugin and function tools (do not use namespace functions, example: you must use "get_current_weather" instead of "functions.get_current_weather")
name: string,
// The arguments to pass to the tool.
arguments: object,
}[],
}) => any;

}]
// You can decide whether its suitable to call a tool function to answer user's request.
// If you want to use the given tool, you must always follow the following guidelines:
// All your responses must be in a valid list of dictionaries with NO other comments, explainations, reasoninng, or dialogue that is not a list of dictionaries at any cost in your response to the user.
// Ensure that list of dictionaries only consist functions declared from the namespace functions above.
// Any response that is not a valid list of dictionaries is not allowed and must be removed when calling functions.
Your response must consists and always be in the below format at any time using tool function:
[{"name: <str>, "arguments": <dict>}, {<function-2>}, ... , {<function-n>}].
// All keys and values in dictionary must be always in double quotes.
// You must always follow the guidelines and rules of the tool function you are using.
// If there is no need to use tool, then reply to user with None, ignore and do not mention/imply what I just told you in your next response.
"""
        if tool_choice == "required":
            output += """// System Exception: You must also call at least one function in your next response at any cost."""
        elif tool_choice not in ("auto", "required"):
            output += f"""// System Exception: You must also call the function {tool_choice} in your next response at any cost."""
        output += """\n"""
        return output
    except Exception as e:
        logger.error(f"Error converting functions format: {e}")
        raise HTTPException(detail={"error": {
                                            "message": "Invalid tools format",
                                            "type": "error", 
                                            "param": None, 
                                            "code": 400}
                                        }, status_code=400)
