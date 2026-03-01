from pydantic import BaseModel, Extra
from typing import Any, Optional, List, Literal, Dict, Union

class ChatData(BaseModel, extra=Extra.allow):
    model: Any
    messages: Any
    stream: Optional[bool] = False
    max_tokens: Optional[int] = None
    max_completion_tokens: Optional[int] = None
    frequency_penalty: Optional[float] = 0.0
    presence_penalty: Optional[float] = 1.0
    temperature: Optional[float] = 1.0
    top_p: Optional[float] = 1.0
    stream_options: Optional[dict[str, Any]] = None
    tools: Optional[List[dict]] = None
    tool_choice: Optional[Union[str, dict]] = None
    metadata: Optional[dict[str, Any]] = None
    user: Optional[str] = None
    n: Optional[int] = 1
    extra_body: Optional[dict[str, Any]] = None
    
class ImagesGenData(BaseModel, extra=Extra.allow):
    prompt: Any
    model: Any
    n: Optional[int] = 1
    size: Optional[str] = '1024x1024'
    
class ImagesEditData(BaseModel, extra=Extra.allow):
    image: Any
    prompt: Any
    model: Any
    n: Optional[int] = 1
    size: Optional[str] = '1024x1024'


class ResponsesData(BaseModel, extra=Extra.allow):
    model: Any
    input: Any
    stream: Optional[bool] = False
    max_output_tokens: Optional[int] = None
    metadata: Optional[dict[str, Any]] = None
    user: Optional[str] = None


class AccountUpsertData(BaseModel):
    email: str
    poe_p_b: str
    poe_cf_clearance: str
    poe_cf_bm: Optional[str] = None
    p_lat: Optional[str] = None
    formkey: Optional[str] = None
    poe_revision: Optional[str] = None
    user_agent: Optional[str] = None


# OpenAI typing
class FunctionCall(BaseModel):
    name: Optional[str] = None
    arguments: Optional[str] = None
    
class ChoiceDeltaToolCallFunction(BaseModel):
    name: Optional[str] = None
    arguments: Optional[str] = None

class ChatCompletionMessageToolCall(BaseModel, extra=Extra.allow):
    id: Optional[str] = None
    function: FunctionCall
    type: Optional[Literal["function"]] = 'function'
    
class ChoiceDeltaToolCall(BaseModel):
    index: Optional[int] = 0
    id: Optional[str] = None
    function: ChoiceDeltaToolCallFunction
    type: Optional[Literal["function"]] = 'function'

class MessageResponse(BaseModel):
    role: Optional[Literal["user", "system", "assistant", "tool"]] = None
    content: Optional[str] = None
    function_call: Optional[ChoiceDeltaToolCallFunction] = None
    tool_calls: Optional[List[Union[ChatCompletionMessageToolCall, ChoiceDeltaToolCall]]] = None

class ChatCompletionUsage(BaseModel):
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    
# Non-Streaming
class ChatCompletionResponseChoice(BaseModel):
    index: int
    message: MessageResponse
    finish_reason: Optional[Literal["stop", "length", "content_filter", "tool_calls", "function_call"]] = None
    
class ChatCompletionResponse(BaseModel):
    id: str
    choices: List[ChatCompletionResponseChoice]
    created: int  # Unix timestamp (in seconds)
    model: str
    system_fingerprint: Optional[str] = None
    object: Literal["chat.completion"] = "chat.completion"
    usage: ChatCompletionUsage

# Streaming
class ChatCompletionChunkChoice(BaseModel):
    index: int
    delta: MessageResponse
    finish_reason: Optional[Literal["stop", "length", "content_filter", "tool_calls", "function_call"]] = None
    
class ChatCompletionChunk(BaseModel, extra=Extra.allow):
    id: str
    choices: List[ChatCompletionChunkChoice]
    object: Literal["chat.completion.chunk"] = "chat.completion.chunk"
    created: int 
    model: str
