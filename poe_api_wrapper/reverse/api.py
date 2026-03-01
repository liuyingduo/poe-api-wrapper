from time import sleep
from httpx import Client
from requests_toolbelt import MultipartEncoder
import os, secrets, string, random, websocket, threading, queue, ssl, hashlib, re, uuid
from loguru import logger
from typing import Generator, Optional

try:
    import orjson
except ImportError:
    import json as _json

    class _OrjsonCompat:
        OPT_INDENT_2 = 2

        @staticmethod
        def dumps(obj, option=None):
            indent = 2 if option == 2 else None
            return _json.dumps(obj, ensure_ascii=False, indent=indent).encode("utf-8")

        @staticmethod
        def loads(data):
            if isinstance(data, (bytes, bytearray)):
                data = data.decode("utf-8")
            return _json.loads(data)

    orjson = _OrjsonCompat()
from .utils import (
                    BASE_URL,
                    HEADERS,
                    SubscriptionsMutation,
                    BOTS_LIST, 
                    REVERSE_BOTS_LIST, 
                    bot_map, 
                    generate_nonce, 
                    generate_file,
                    extract_tchannel_data_from_html,
                    )
from .queries import generate_payload, resolve_query_name
from .bundles import PoeBundle

"""
This API is modified and maintained by @snowby666
Credit to @ading2210 for the GraphQL queries
"""
class PoeApi:
    BASE_URL = BASE_URL
    HEADERS = HEADERS
    MAX_CONCURRENT_MESSAGES = 3

    def __init__(
        self,
        tokens: Optional[dict] = None,
        proxy: Optional[list] = None,
        auto_proxy: bool = False,
        headers: Optional[dict] = None,
    ):
        self.client = None
        tokens = tokens or {}
        proxy = proxy or []
        if 'p-b' not in tokens:
            raise ValueError("Please provide a valid p-b cookie")
    
        self.tokens: dict = tokens
        self.formkey: str = ""
        self.ws_connecting: bool = False
        self.ws_connected: bool = False
        self.ws_error: bool = False
        self.active_messages: dict[int, str] = {}
        self.message_queues: dict[int, queue.Queue] = {}
        self.current_thread: dict[str, list] = {}
        self.retry_attempts: int = 3
        self.ws_refresh: int = 3
        self.ws_heartbeat_interval: int = 25
        self.groups: dict = {}
        self.proxies: dict = {}
        self.bundle: PoeBundle = None
        self._ws_heartbeat_stop = threading.Event()
        self._ws_heartbeat_thread: Optional[threading.Thread] = None
        
        self.client = Client(headers=self.HEADERS.copy(), timeout=None, http2=True)
        if headers:
            self.client.headers.update(headers)
        self.client.cookies.update({
                                'p-b': self.tokens['p-b'],
                                })
        if 'p-lat' in self.tokens and self.tokens['p-lat']:
            self.client.cookies.update({
                'p-lat': self.tokens['p-lat']
            })
        
        cloudflare_cookies = {}
        if '__cf_bm' in tokens and tokens['__cf_bm']:
            cloudflare_cookies['__cf_bm'] = tokens['__cf_bm']
        if 'cf_clearance' in tokens and tokens['cf_clearance']:
            cloudflare_cookies['cf_clearance'] = tokens['cf_clearance']
        if cloudflare_cookies:
            self.client.cookies.update(cloudflare_cookies)
        
        if 'formkey' in tokens:
            self.formkey = tokens['formkey']
            self.client.headers.update({
                'Poe-Formkey': self.formkey,
            })
        if 'poe-revision' in tokens and tokens['poe-revision']:
            self.client.headers.update({
                'Poe-Revision': tokens['poe-revision'],
            })
        
        if self.formkey == "":
            self.load_bundle()
        self.connect_ws() 
        
    def __del__(self):
        if self.client:
            self.client.close()
        
    def load_bundle(self):
        try:
            webData = self.client.get(self.BASE_URL)
            self.bundle = PoeBundle(webData.text)
            self.formkey = self.bundle.get_form_key()
            self.client.headers.update({
                'Poe-Formkey': self.formkey,
            })
        except Exception as e:
            logger.error(f"Failed to load bundle. Reason: {e}")
            logger.warning("Failed to get formkey from bundle. Please provide a valid formkey manually." if self.formkey == "" else "Continuing with provided formkey")
            
    def _extract_file_hash_jwt(self, raw_text: str):
        token_pattern = re.compile(r"^[A-Za-z0-9_\-=]+\.[A-Za-z0-9_\-=]+\.[A-Za-z0-9_\-=]+$")

        def find_token(obj):
            if isinstance(obj, str):
                value = obj.strip().strip('"')
                if token_pattern.match(value):
                    return value
                return None
            if isinstance(obj, dict):
                for key in ("fileHashJwt", "file_hash_jwt", "jwt", "token"):
                    if key in obj:
                        token = find_token(obj[key])
                        if token:
                            return token
                for value in obj.values():
                    token = find_token(value)
                    if token:
                        return token
            if isinstance(obj, list):
                for value in obj:
                    token = find_token(value)
                    if token:
                        return token
            return None

        stripped = raw_text.strip()
        if token_pattern.match(stripped.strip('"')):
            return stripped.strip('"')

        try:
            parsed = orjson.loads(stripped)
        except Exception:
            return None

        return find_token(parsed)

    def finish_upload(self, file_form: list) -> list:
        file_hash_jwts = []
        for file in file_form:
            response = self.client.post(
                f"{self.BASE_URL}/api/finish_upload_POST",
                files={"file": file},
                follow_redirects=True,
            )
            if response.status_code != 200:
                preview = response.text[:300].replace("\n", " ")
                raise RuntimeError(
                    f"finish_upload_POST failed. status_code:{response.status_code}, body_preview:{preview!r}"
                )
            file_hash_jwt = self._extract_file_hash_jwt(response.text)
            if not file_hash_jwt:
                preview = response.text[:300].replace("\n", " ")
                raise RuntimeError(
                    f"Failed to parse fileHashJwt from finish_upload_POST response. body_preview:{preview!r}"
                )
            file_hash_jwts.append(file_hash_jwt)
        return file_hash_jwts
    
    def send_request(
        self,
        path: str,
        query_name: str = "",
        variables: Optional[dict] = None,
        file_form: Optional[list] = None,
        knowledge: bool = False,
    ):
        variables = variables or {}
        file_form = file_form or []
        status_code = 0
        resolved_query_name = resolve_query_name(query_name)
        
        try:
            payload = generate_payload(query_name, variables)
            base_string = payload + self.formkey + "4LxgHM6KpFqokX0Ox"
            if file_form == []:
                headers = {'Content-Type': 'application/json'}
            else:
                fields = {'queryInfo': payload}
                if not knowledge:
                    for i in range(len(file_form)):
                        fields[f'file{i}'] = file_form[i]
                else:
                    fields['file'] = file_form[0]
                payload = MultipartEncoder(
                    fields=fields
                    )
                headers = {'Content-Type': payload.content_type}
                payload = payload.to_string()
            
            headers.update({
                "poe-tag-id": hashlib.md5(base_string.encode()).hexdigest(),
                "poe-queryname": resolved_query_name,
                "poegraphql": "1",
            })
            response = self.client.post(f'{self.BASE_URL}/api/{path}', data=payload, headers=headers, follow_redirects=True)
            
            status_code = response.status_code
            response_text = response.text
            content_type = response.headers.get("content-type", "")
            try:
                json_data = orjson.loads(response_text)
            except Exception as json_error:
                body_preview = response_text[:300].replace("\n", " ")
                raise RuntimeError(
                    f"Non-JSON response. status_code:{status_code}, content_type:{content_type}, body_preview:{body_preview!r}"
                ) from json_error

            if (
                "success" in json_data.keys()
                and not json_data["success"]
                or (json_data and json_data["data"] is None)
            ):
                err_msg: str = json_data["errors"][0]["message"]
                if err_msg == "Server Error":
                    raise RuntimeError(f"Server Error. Raw response data: {json_data}")
                else:
                    logger.error(response.status_code)
                    logger.error(response.text)
                    raise Exception(response.text)
                
            if status_code == 200:
                for file in file_form:
                    try:
                        if hasattr(file[1], 'closed') and not file[1].closed:
                            file[1].close()
                    except IOError as e:
                        logger.warning(f"Failed to close file: {file[0]}. Reason: {e}")
                return json_data
            
        except Exception as e:
            error_code = f"status_code:{status_code}, " if status_code else ""
            raise Exception(
                f"Sending request {query_name} failed. {error_code} Error log: {repr(e)}"
            )
    
    def get_channel_settings(self):
        response = self.client.get(self.BASE_URL, headers=self.HEADERS, follow_redirects=True)
        if response.status_code != 200:
            raise RuntimeError(
                f"Failed to load Poe homepage for channel settings. status_code:{response.status_code}"
            )
        response_json = extract_tchannel_data_from_html(response.text)
        self.ws_domain = f"tch{random.randint(1, int(1e6))}"[:11]
        self.tchannel_data = response_json
        self.client.headers["Poe-Tchannel"] = self.tchannel_data["channel"]
        self.channel_url = f'wss://{self.ws_domain}.tch.{self.tchannel_data["baseHost"]}/up/{self.tchannel_data["boxName"]}/updates?min_seq={self.tchannel_data["minSeq"]}&channel={self.tchannel_data["channel"]}&hash={self.tchannel_data["channelHash"]}'
        self.subscribe()
    
    def subscribe(self):
        response_json = self.send_request('gql_POST', "SubscriptionsMutation", SubscriptionsMutation)
        if response_json['data'] == None and response_json["errors"]:
            raise RuntimeError(f'Failed to subscribe by sending SubscriptionsMutation. Raw response data: {response_json}')
            
    def ws_run_thread(self):
        if self.ws and not self.ws.sock:
            kwargs = {
                "sslopt": {"cert_reqs": ssl.CERT_NONE},
                "ping_interval": 20,
                "ping_timeout": 10,
            }
            self.ws.run_forever(**kwargs)
             
    def connect_ws(self):
        
        if self.ws_connected:
            return

        if self.ws_connecting:
            while not self.ws_connected:
                sleep(0.01)
            return

        self.ws_connecting = True
        self.ws_connected = False
        self.ws_refresh = 3
        
        while True:
            self.ws_refresh -= 1
            if self.ws_refresh == 0:
                self.ws_refresh = 3
                raise RuntimeError("Rate limit exceeded for sending requests to poe.com. Please try again later.")
            try:
                self.get_channel_settings()
                break
            except Exception as e:
                logger.error(f"Failed to get channel settings. Reason: {e}")
                sleep(1)
                continue

        self.ws = websocket.WebSocketApp(self.channel_url, 
                                         header={
                                             "Origin": f"{self.BASE_URL}",
                                             "Pragma": "no-cache",
                                             "Cache-Control": "no-cache",
                                         },
                                         on_message=lambda ws, msg: self.on_message(ws, msg), 
                                         on_open=lambda ws: self.on_ws_connect(ws), 
                                         on_error=lambda ws, error: self.on_ws_error(ws, error), 
                                         on_close=lambda ws, close_status_code, close_message: self.on_ws_close(ws, close_status_code, close_message))

        t = threading.Thread(target=self.ws_run_thread, daemon=True)
        t.start()

        while not self.ws_connected:
            sleep(0.01)
        self._start_ws_heartbeat()

    def disconnect_ws(self):
        self.ws_connecting = False
        self.ws_connected = False
        self._stop_ws_heartbeat()
        if self.ws:
            self.ws.close()
            logger.info("Websocket connection closed.")

    def on_ws_connect(self, ws):
        self.ws_connecting = False
        self.ws_connected = True
        self._start_ws_heartbeat()

    def on_ws_close(self, ws, close_status_code, close_message):
        self.ws_connecting = False
        self.ws_connected = False
        self._stop_ws_heartbeat()
        if self.ws_error:
            logger.warning("Connection to remote host was lost. Reconnecting...")
            self.ws_error = False
            self.connect_ws()

    def on_ws_error(self, ws, error):
        self.ws_connecting = False
        self.ws_connected = False
        self.ws_error = True
        self._stop_ws_heartbeat()

    def _start_ws_heartbeat(self):
        if self._ws_heartbeat_thread and self._ws_heartbeat_thread.is_alive():
            return
        self._ws_heartbeat_stop.clear()
        self._ws_heartbeat_thread = threading.Thread(target=self._ws_heartbeat_loop, daemon=True)
        self._ws_heartbeat_thread.start()

    def _stop_ws_heartbeat(self):
        self._ws_heartbeat_stop.set()
        self._ws_heartbeat_thread = None

    def _ws_heartbeat_loop(self):
        while not self._ws_heartbeat_stop.wait(self.ws_heartbeat_interval):
            if not self.ws_connected or self.ws_error:
                continue
            ws = getattr(self, "ws", None)
            if not ws or not ws.sock:
                continue
            try:
                ws.send('{"type":"ping"}')
            except Exception as exc:
                logger.debug(f"Failed to send websocket heartbeat ping. Reason: {exc}")
                break
        
    def on_message(self, ws, msg):
        try:
            ws_data = orjson.loads(msg)

            if isinstance(ws_data, dict):
                message_type = ws_data.get("type")
                if message_type == "ping":
                    try:
                        ws.send('{"type":"pong"}')
                    except Exception as exc:
                        logger.debug(f"Failed to reply websocket pong. Reason: {exc}")
                    return
                if message_type == "pong":
                    return

            if "error" in ws_data.keys() and ws_data["error"] == "missed_messages":
                self.connect_ws()
                return
            
            if not "messages" in ws_data:
                return
            
            for data in ws_data["messages"]:
                data = orjson.loads(data)
                message_type = data.get("message_type")
                if message_type == "refetchChannel":
                    self.connect_ws()
                    return

                payload = data.get("payload", {})
                
                subscriptionName = payload.get("subscription_name")
                
                if subscriptionName not in ("messageAdded", "messageCancelled", "chatTitleUpdated"):
                    continue

                data = (payload.get("data", {}))
                
                if not data:
                    continue
                
                if subscriptionName == "messageAdded" and data["messageAdded"]["author"] == "human":
                    continue
                        
                chat_id: int = int(payload.get("unique_id")[(len(subscriptionName) + 1):])
                
                if chat_id not in self.message_queues:
                    continue
                
                if chat_id in self.message_queues:
                    self.message_queues[chat_id].put(
                        {
                            "data": data,
                            "subscription": subscriptionName,
                        }
                    )
                    if subscriptionName == "messageAdded":
                        self.active_messages[chat_id] = data["messageAdded"]["messageId"]          
                    continue
                    
        except Exception:
            logger.exception(f"Failed to parse message: {ws_data}")
            self.disconnect_ws()
            self.connect_ws()
            
    def delete_queues(self, chatId: int):
        if chatId in self.message_queues:
            del self.message_queues[chatId]
        if chatId in self.active_messages:
            del self.active_messages[chatId]
        
    def delete_pending_messages(self, prompt_md5: str):
        if prompt_md5 in self.active_messages:
            del self.active_messages[prompt_md5]
            
    def get_settings(self):
        response_json = self.send_request('gql_POST', 'SettingsPageQuery', {})
        if response_json['data'] == None and response_json["errors"]:
            raise RuntimeError(f'Failed to get settings. Raw response data: {response_json}')
        viewer = response_json.get("data", {}).get("viewer", {})
        message_info = dict(viewer.get("messagePointInfo") or {})
        message_info["subscriptionPointBalance"] = int(message_info.get("subscriptionPointBalance", 0) or 0)
        message_info["addonPointBalance"] = int(message_info.get("addonPointBalance", 0) or 0)
        message_info["totalPointBalance"] = (
            message_info["subscriptionPointBalance"] + message_info["addonPointBalance"]
        )
        return {
            "subscription": viewer.get("subscription"),
            "messagePointInfo": message_info
        }
    
    def get_available_bots(self, count: int=25, get_all: bool=False):
        self.bots = {}
        if not (get_all or count):
            raise TypeError("Please provide at least one of the following parameters: get_all=<bool>, count=<int>")
        query_variables = {
            "botSelectorFilter": "all",
            "cursor": None,
            "filterCanvasOnlyBots": False,
            "includeHomePageBotSelectorFragment": False,
            "limit": 10,
        }
        response = self.send_request('gql_POST', "AvailableBotsSelectorModalPaginationQuery", query_variables)
        bots = [
            each["node"]
            for each in response["data"]["viewer"]["availableBotsConnection"]["edges"]
            if each["node"]["deletionState"] == "not_deleted"
        ]
        cursor = response["data"]["viewer"]["availableBotsConnection"]["pageInfo"]["endCursor"]
        if len(bots) >= count and not get_all:
            self.bots.update({bot["handle"]: {"bot": bot} for bot in bots})
            return self.bots
        while len(bots) < count or get_all:
            query_variables["cursor"] = cursor
            response = self.send_request("gql_POST", "AvailableBotsSelectorModalPaginationQuery", query_variables)
            new_bots = [
                each["node"]
                for each in response["data"]["viewer"]["availableBotsConnection"]["edges"]
                if each["node"]["deletionState"] == "not_deleted"
            ]
            cursor = response["data"]["viewer"]["availableBotsConnection"]["pageInfo"]["endCursor"]
            bots += new_bots
            if len(new_bots) == 0:
                if not get_all:
                    logger.warning(f"Only {len(bots)} bots found on this account")
                else:
                    logger.info(f"Total {len(bots)} bots found on this account")
                self.bots.update({bot["handle"]: {"bot": bot} for bot in bots})
                return self.bots
            
        logger.info("Succeed to get available bots")
        self.bots.update({bot["handle"]: {"bot": bot} for bot in bots})
        return self.bots
    
    def get_chat_history(self, bot: str=None, count: int=None, interval: int=50, cursor: str=None):

        chat_bots = {'data': {}, 'cursor': None}
        
        if count != None:
            interval = count
        
        if bot == None:
            response_json = self.send_request('gql_POST', 'ChatHistoryListPaginationQuery', {'count': interval, 'cursor': cursor})
            if response_json['data']['chats']['pageInfo']['hasNextPage']:
                cursor = response_json['data']['chats']['pageInfo']['endCursor']
                chat_bots['cursor'] = cursor  
            else:
                chat_bots['cursor'] = None
            edges = response_json['data']['chats']['edges']
       
            for edge in edges:
                chat = edge['node']
                model = bot_map(chat["defaultBotObject"]["displayName"])
              
                if model in chat_bots['data']:
                    chat_bots['data'][model].append({"chatId": chat["chatId"],"chatCode": chat["chatCode"], "id": chat["id"], "title": chat["title"]})
                else:
                    chat_bots['data'][model] = [{"chatId": chat["chatId"], "chatCode": chat["chatCode"], "id": chat["id"], "title": chat["title"]}]
            # Fetch more chats
            if count == None:
                while response_json['data']['chats']['pageInfo']['hasNextPage']:
                    response_json = self.send_request('gql_POST', 'ChatHistoryListPaginationQuery', {'count': interval, 'cursor': cursor})
                    edges = response_json['data']['chats']['edges']
                    for edge in edges:
                        chat = edge['node']
                        model = bot_map(chat["defaultBotObject"]["displayName"])
                      
                        if model in chat_bots['data']:
                            chat_bots['data'][model].append({"chatId": chat["chatId"],"chatCode": chat["chatCode"], "id": chat["id"], "title": chat["title"]})
                        else:
                            chat_bots['data'][model] = [{"chatId": chat["chatId"], "chatCode": chat["chatCode"], "id": chat["id"], "title": chat["title"]}]    
                    cursor = response_json['data']['chats']['pageInfo']['endCursor']  
                    chat_bots['cursor'] = cursor      
                if not response_json['data']['chats']['pageInfo']['hasNextPage']:
                    chat_bots['cursor'] = None  
        else:
            model = bot.lower().replace(' ', '')
            handle = model
            for key, value in BOTS_LIST.items():
                if model == value:
                    handle = key
                    break
            response_json = self.send_request('gql_POST', 'ChatHistoryFilteredListPaginationQuery', {'count': interval, 'handle': handle, 'cursor': cursor})
            if response_json['data'] == None and response_json["errors"]:
                raise ValueError(
                    f"Bot {bot} not found. Make sure the bot exists before creating new chat."
                )
            if response_json['data']['filteredChats']['pageInfo']['hasNextPage']:
                cursor = response_json['data']['filteredChats']['pageInfo']['endCursor']
                chat_bots['cursor'] = cursor  
            else:
                chat_bots['cursor'] = None
            edges = response_json['data']['filteredChats']['edges']
            for edge in edges:
                chat = edge['node']
                try:
                    if model in chat_bots['data']:
                        chat_bots['data'][model].append({"chatId": chat["chatId"],"chatCode": chat["chatCode"], "id": chat["id"], "title": chat["title"]})
                    else:
                        chat_bots['data'][model] = [{"chatId": chat["chatId"], "chatCode": chat["chatCode"], "id": chat["id"], "title": chat["title"]}]
                except Exception as e:
                    logger.debug(str(e))
                    pass 
            # Fetch more chats
            if count == None:
                while response_json['data']['filteredChats']['pageInfo']['hasNextPage']:
                    response_json = self.send_request('gql_POST', 'ChatHistoryFilteredListPaginationQuery', {'count': interval, 'handle': handle, 'cursor': cursor})
                    edges = response_json['data']['filteredChats']['edges']
                    for edge in edges:
                        chat = edge['node']
                        try:
                            if model in chat_bots['data']:
                                chat_bots['data'][model].append({"chatId": chat["chatId"],"chatCode": chat["chatCode"], "id": chat["id"], "title": chat["title"]})
                            else:
                                chat_bots['data'][model] = [{"chatId": chat["chatId"], "chatCode": chat["chatCode"], "id": chat["id"], "title": chat["title"]}]
                        except Exception as e:
                            logger.debug(str(e))
                            pass      
                    cursor = response_json['data']['filteredChats']['pageInfo']['endCursor']  
                    chat_bots['cursor'] = cursor  
                if not response_json['data']['filteredChats']['pageInfo']['hasNextPage']:
                    chat_bots['cursor'] = None
        return chat_bots
    
    def get_threadData(self, bot: str="", chatCode: str=None, chatId: int=None):
        id = None
        title = None
        if bot not in self.current_thread or len(self.current_thread[bot]) <= 1:
            self.current_thread[bot] = self.get_chat_history(bot=bot)['data'][bot]
        if chatCode != None:
            for chat in self.current_thread[bot]:
                if chat['chatCode'] == chatCode:
                    chatId = chat['chatId']
                    id = chat['id']
                    title = chat['title']
                    break
        elif chatId != None:
            for chat in self.current_thread[bot]:
                if chat['chatId'] == chatId:
                    chatCode = chat['chatCode']
                    id = chat['id']
                    title = chat['title']
                    break
        return {'chatCode': chatCode, 'chatId': chatId, 'id': id, 'title': title}
    
    def get_botInfo(self, handle: str):
        if handle in REVERSE_BOTS_LIST:
            handle = REVERSE_BOTS_LIST[handle]
        else:
            handle = handle.lower().replace(' ', '')
        response_json = self.send_request('gql_POST', 'HandleBotLandingPageQuery', {'botHandle': handle})
        if response_json['data'] == None and response_json["errors"]:
            raise ValueError(
                f"Bot {handle} not found. Make sure the bot exists before creating new chat."
            )
        root_data = response_json["data"]
        botData = root_data["bot"]
        viewer = root_data["viewer"]

        custom_ui_raw = botData.get("customUIDefinition")
        custom_ui = orjson.loads(custom_ui_raw) if isinstance(custom_ui_raw, str) and custom_ui_raw else None

        file_upload_limits_raw = viewer.get("fileUploadSizeLimits")
        file_upload_limits = (
            orjson.loads(file_upload_limits_raw)
            if isinstance(file_upload_limits_raw, str) and file_upload_limits_raw
            else None
        )

        data = {
                'handle': botData["handle"],
                'displayName': botData["displayName"],
                'model': botData["model"],
                'supportsFileUpload': bool(botData["supportsFileUpload"]),
                'allowsImageAttachments': bool(botData["allowsImageAttachments"]),
                'uploadFileSizeLimit': botData["uploadFileSizeLimit"],
                'messageTimeoutSecs': botData["messageTimeoutSecs"],
                'canUserAccessBot': bool(botData["canUserAccessBot"]),
                'noAccessMessage': botData.get("noAccessMessage"),
                'limitedAccessType': botData.get("limitedAccessType"),
                'isApiBot': bool(botData["isApiBot"]),
                'isOfficialBot': bool(botData["isOfficialBot"]),
                'isDown': bool(botData["isDown"]),
                'isServerBot': bool(botData.get("isServerBot", False)),
                'viewerIsCreator': bool(botData["viewerIsCreator"]),
                'id': botData["id"],
                'botId': botData["botId"],
                'description': botData.get("description"),
                'customUIDefinition': custom_ui,
                'customUIDefinitionRaw': custom_ui_raw,
                'submitMessageParamsAsDict': bool(viewer["submitMessageParamsAsDict"]),
                'shouldUseFinishUploadEndpoint': bool(viewer["shouldUseFinishUploadEndpoint"]),
                'shouldUsePresignedUrl': bool(viewer["shouldUsePresignedUrl"]),
                'fileUploadSizeLimits': file_upload_limits,
                'supportedPreviewsContentTypes': root_data.get("supportedPreviewsContentTypes", []),
                'supportedExecutableLanguages': root_data.get("supportedExecutableLanguages", []),
                }
        return data
        
    def retry_message(self, chatCode: str, suggest_replies: bool=False, timeout: int=5):
        self.retry_attempts = 3
        while None in self.active_messages.values() and len(self.active_messages) > self.MAX_CONCURRENT_MESSAGES:
            sleep(0.01)
            
        prompt_md5 = hashlib.md5((chatCode + generate_nonce()).encode()).hexdigest()
        self.active_messages[prompt_md5] = None
        
        while self.ws_error:
            sleep(0.01)
        self.connect_ws()
        
        variables = {"chatCode": chatCode}
        response_json = self.send_request('gql_POST', 'ChatPageQuery', variables)
        
        if response_json['data'] == None and response_json["errors"]:
            raise RuntimeError(f"An unknown error occurred. Raw response data: {response_json}")
    
        edges = response_json['data']['chatOfCode']['messagesConnection']['edges']
        edges.reverse()
        
        chatId = response_json['data']['chatOfCode']['chatId']
        title = response_json['data']['chatOfCode']['title']
        last_message = edges[0]['node']
        
        if last_message['author'] == 'human':
            raise RuntimeError(f"Last message is not from bot. Raw response data: {response_json}")
        
        bot = bot_map(last_message['author'])
        
        status = last_message['state']
        if status == 'error_user_message_too_long':
            raise RuntimeError(f"Last message is too long. Raw response data: {response_json}")
        while status != 'complete':
            sleep(0.5)
            response_json = self.send_request('gql_POST', 'ChatPageQuery', variables)
            if response_json['data'] == None and response_json["errors"]:
                raise RuntimeError(f"An unknown error occurred. Raw response data: {response_json}")
            edges = response_json['data']['chatOfCode']['messagesConnection']['edges']
            edges.reverse()
            last_message = edges[0]['node']
            status = last_message['state']
            if status == 'error_user_message_too_long':
                raise RuntimeError(f"Last message is too long. Raw response data: {response_json}")
        
        bot_message_id = last_message['messageId']
        self.delete_pending_messages(prompt_md5)
        
        response_json = self.send_request('gql_POST', 'RegenerateMessageMutation', {'messageId': bot_message_id})
        if response_json['data'] == None and response_json["errors"]:
            logger.error(f"Failed to retry message {bot_message_id} of Thread {chatCode}. Raw response data: {response_json}")
        else:
            logger.info(f"Message {bot_message_id} of Thread {chatCode} has been retried.")
            
        self.active_messages[chatId] = None
        self.message_queues[chatId] = queue.Queue()

        last_text = ""
        stateChange = False
        suggest_attempts = 6
        response = {}
        suggestedReplies = []
        
        while True:
            try:
                ws_data = self.message_queues[chatId].get()
            except KeyError:
                sleep(1)
                continue
            
            if ws_data["subscription"] == "messageCancelled":
                break
            
            if ws_data["subscription"] == "chatTitleUpdated":
                title = ws_data["data"]["chatTitleUpdated"]["title"]

            if ws_data["subscription"] == "messageAdded" or title:
                if ws_data["subscription"] == "messageAdded":
                    response = ws_data["data"]["messageAdded"]
                
                response["chatCode"] = chatCode
                response["chatId"] = chatId
                response["title"] = title
                # response["msgPrice"] = msgPrice
                response["response"] = ""
                response["suggestedReplies"] = suggestedReplies

                if response["state"] == "error_user_message_too_long":
                    response["response"]  = "Message too long. Please try again!"
                    yield response
                    break
                
                if (response["author"] == "pacarana" and response["text"].strip() == last_text.strip()):
                    response["response"] = ""
                elif response["author"] == "pacarana" and (last_text == "" or bot != "web-search"):
                    response["response"] = f'{response["text"]}\n'
                else:
                    if stateChange == False:
                        response["response"] = response["text"]
                        stateChange = True
                    else:
                        response["response"] = response["text"][len(last_text):]
                
                if response["state"] == "complete":
                    if suggest_replies:
                        if suggest_attempts > 0 and len(response["followupActions"]) <= 6:
                            actions = response["followupActions"]
                            suggestedReplies = [action["bodyText"] for action in actions]
                            suggest_attempts -= 1     
                            sleep(1)
                            continue
                                   
                    yield response
                    break
                
                yield response
                
                last_text = response["text"]
        
        self.delete_queues(chatId)
        self.retry_attempts = 3
        
    def send_message(
        self,
        bot: str,
        message: str,
        chatId: int = None,
        chatCode: str = None,
        msgPrice: int = 20,
        file_path: Optional[list] = None,
        suggest_replies: bool = False,
        timeout: int = 5,
    ) -> Generator[dict, None, None]:
        file_path = file_path or []
        self.retry_attempts = 3
        while None in self.active_messages.values() and (len(self.active_messages) > self.MAX_CONCURRENT_MESSAGES):
            sleep(0.01)

        prompt_md5 = hashlib.md5((message + generate_nonce()).encode()).hexdigest() 
        self.active_messages[prompt_md5] = None
        
        while self.ws_error:
            sleep(0.01)
        self.connect_ws()
        
        bot_input = bot
        bot = bot_map(bot_input)
        bot_candidates = [bot]
        if bot_input != bot:
            bot_candidates.append(bot_input)
        attachments = []
        file_hash_jwts = []
        
        if file_path == []:
            apiPath = 'gql_POST'
            file_form = []
        else:
            apiPath = 'gql_POST'
            file_form, file_size = generate_file(file_path, self.proxies)
            if file_size > 350000000:
                raise RuntimeError("File size too large. Please try again with a smaller file.")
            for i in range(len(file_form)):
                attachments.append(f'file{i}')
            file_hash_jwts = self.finish_upload(file_form)
        
        msgPrice = None
            
        if (chatId == None and chatCode == None):
            try:
                variables = {
                                "chatId": None, 
                                "query":message, 
                                "shouldFetchChat": True, 
                                "source":{"sourceType":"chat_input","chatInputMetadata":{"useVoiceRecord":False, "newChatContext": "home_page_input"}}, 
                                "clientNonce": generate_nonce(),
                                "sdid": str(uuid.uuid4()),
                                "attachments":attachments, 
                                "existingMessageAttachmentsIds":[],
                                "chatNonce": generate_nonce(),
                                "referencedMessageId": None,
                                "parameters": None,
                                "fileHashJwts": file_hash_jwts
                            }
                message_data = None
                send_error = None
                for bot_candidate in bot_candidates:
                    try:
                        variables["bot"] = bot_candidate
                        message_data = self.send_request(apiPath, 'SendMessageMutation', variables)
                        bot = bot_candidate
                        break
                    except Exception as e:
                        send_error = e
                if message_data is None:
                    raise send_error
        
                if message_data["data"] == None and message_data["errors"]:
                    raise ValueError(
                        f"Bot {bot} not found. Make sure the bot exists before creating new chat."
                    )
                else:
                    status = message_data['data']['messageEdgeCreate']['status']
                    if status == 'success' and file_path != []:
                        for file in file_form:
                            logger.info(f"File '{file[0]}' uploaded successfully")
                    elif status == 'unsupported_file_type' and file_path != []:
                        logger.warning("This file type is not supported. Please try again with a different file.")
                    elif status == 'reached_limit':
                        raise RuntimeError(f"Daily limit reached for {bot}.")
                    elif status == 'too_many_tokens':
                        raise RuntimeError(f"{message_data['data']['messageEdgeCreate']['statusMessage']}")
                    elif status == 'no_access':
                        raise RuntimeError(f"{message_data['data']['messageEdgeCreate']['statusMessage']}")
                    elif status in ('rate_limit_exceeded', 'concurrent_messages'):
                        self.delete_pending_messages(prompt_md5)
                        raise RuntimeError(f"{message_data['data']['messageEdgeCreate']['statusMessage']}")

                    chat_data = message_data['data']['messageEdgeCreate'].get('chat')
                    if not chat_data:
                        raise RuntimeError(f"Failed to create chat: {message_data['data']['messageEdgeCreate'].get('statusMessage', 'Unknown error')}")

                    logger.info(f"New Thread created | {message_data['data']['messageEdgeCreate']['chat']['chatCode']}")
                
                message_data = message_data['data']['messageEdgeCreate']['chat']
                chatCode = message_data['chatCode']
                chatId = message_data['chatId']
                title = message_data['title']
                if bot not in self.current_thread:
                    self.current_thread[bot] = [{'chatId': chatId, 'chatCode': chatCode, 'id': message_data['id'], 'title': message_data['title']}]
                elif self.current_thread[bot] == []:
                    self.current_thread[bot] = [{'chatId': chatId, 'chatCode': chatCode, 'id': message_data['id'], 'title': message_data['title']}]
                else:
                    self.current_thread[bot].append({'chatId': chatId, 'chatCode': chatCode, 'id': message_data['id'], 'title': message_data['title']})
                self.delete_pending_messages(prompt_md5)
            except Exception as e:
                self.delete_pending_messages(prompt_md5)
                raise e
        else:
            chatdata = self.get_threadData(bot, chatCode, chatId)
            chatCode = chatdata['chatCode']
            chatId = chatdata['chatId']
            title = chatdata['title']
            variables = {
                            'chatId': chatId, 
                            'query': message, 
                            'shouldFetchChat': False, 
                            'source': { "sourceType": "chat_input", "chatInputMetadata": {"useVoiceRecord": False}}, 
                            "clientNonce": generate_nonce(), 
                            'sdid': str(uuid.uuid4()), 
                            'attachments': attachments, 
                            "existingMessageAttachmentsIds":[],
                            "chatNonce": generate_nonce(),
                            "referencedMessageId": None,
                            "parameters": None,
                            "fileHashJwts": file_hash_jwts
                        }
            
            try:
                message_data = None
                send_error = None
                for bot_candidate in bot_candidates:
                    try:
                        variables["bot"] = bot_candidate
                        message_data = self.send_request(apiPath, 'SendMessageMutation', variables)
                        bot = bot_candidate
                        break
                    except Exception as e:
                        send_error = e
                if message_data is None:
                    raise send_error
                    
                if message_data["data"] == None and message_data["errors"]:
                    raise RuntimeError(f"An unknown error occurred. Raw response data: {message_data}")
                else:
                    status = message_data['data']['messageEdgeCreate']['status']
                    if status == 'success' and file_path != []:
                        for file in file_form:
                            logger.info(f"File '{file[0]}' uploaded successfully")
                    elif status == 'unsupported_file_type' and file_path != []:
                        logger.warning("This file type is not supported. Please try again with a different file.")
                    elif status == 'reached_limit':
                        raise RuntimeError(f"Daily limit reached for {bot}.")
                    elif status == 'too_many_tokens':
                        raise RuntimeError(f"{message_data['data']['messageEdgeCreate']['statusMessage']}")
                    elif status == 'no_access':
                        raise RuntimeError(f"{message_data['data']['messageEdgeCreate']['statusMessage']}")
                    elif status in ('rate_limit_exceeded', 'concurrent_messages'):
                        self.delete_pending_messages(prompt_md5)
                        raise RuntimeError(f"{message_data['data']['messageEdgeCreate']['statusMessage']}")
                        
                self.delete_pending_messages(prompt_md5)
            except Exception as e:
                self.delete_pending_messages(prompt_md5)
                raise e
        
        self.active_messages[chatId] = None
        self.message_queues[chatId] = queue.Queue()

        last_text = ""
        stateChange = False
        suggest_attempts = 6
        response = {}
        suggestedReplies = []
        
        while True:
            try:
                ws_data = self.message_queues[chatId].get()
            except KeyError:
                sleep(1)
                continue
            
            if ws_data["subscription"] == "messageCancelled":
                break
            
            if ws_data["subscription"] == "chatTitleUpdated":
                title = ws_data["data"]["chatTitleUpdated"]["title"]

            if ws_data["subscription"] == "messageAdded" or title:
                if ws_data["subscription"] == "messageAdded":
                    response = ws_data["data"]["messageAdded"]
                
                response["chatCode"] = chatCode
                response["chatId"] = chatId
                response["title"] = title
                response["msgPrice"] = msgPrice
                response["response"] = ""
                response["suggestedReplies"] = suggestedReplies

                if response["state"] == "error_user_message_too_long":
                    response["response"]  = "Message too long. Please try again!"
                    yield response
                    break
                
                if (response["author"] == "pacarana" and response["text"].strip() == last_text.strip()):
                    response["response"] = ""
                elif response["author"] == "pacarana" and (last_text == "" or bot != "web-search"):
                    response["response"] = f'{response["text"]}\n'
                else:
                    if stateChange == False:
                        response["response"] = response["text"]
                        stateChange = True
                    else:
                        response["response"] = response["text"][len(last_text):]
                        
                if response["state"] == "complete":
                    if suggest_replies:
                        if suggest_attempts > 0 and len(response["followupActions"]) <= 6:
                            actions = response["followupActions"]
                            suggestedReplies = [action["bodyText"] for action in actions]
                            suggest_attempts -= 1     
                            sleep(1)
                            continue

                    yield response
                    break
                
                yield response
                
                last_text = response["text"]
                          
        self.delete_queues(chatId)
        self.retry_attempts = 3
        
    def cancel_message(self, chunk: dict):
        if chunk.get("chatId") is not None:
            variables = {"chatId": chunk["chatId"]}
            self.send_request('gql_POST', 'cancelViewerActiveJobs_cancelViewerActiveJobs_Mutation', variables)
        else:
            variables = {"messageId": chunk["messageId"], "textLength": len(chunk["text"])}
            self.send_request('gql_POST', 'StopMessage_messageCancel_Mutation', variables)
        
    def chat_break(self, bot: str, chatId: int=None, chatCode: str=None):
        bot = bot_map(bot)
        chatdata = self.get_threadData(bot, chatCode, chatId)
        chatId = chatdata['chatId']
        variables = {'chatId': chatId, 'clientNonce': generate_nonce()}
        self.send_request('gql_POST', 'SendChatBreakMutation', variables)
            
    def delete_message(self, message_ids):
        variables = {'messageIds': message_ids}
        self.send_request('gql_POST', 'DeleteMessageMutation', variables)
    
    def purge_conversation(self, bot: str, chatId: int=None, chatCode: str=None, count: int=50, del_all: bool=False):
        bot = bot_map(bot)
        if chatId != None and chatCode == None:
            chatdata = self.get_threadData(bot, chatCode, chatId)
            chatCode = chatdata['chatCode']
        variables = {'chatCode': chatCode}
        response_json = self.send_request('gql_POST', 'ChatPageQuery', variables)
        if response_json['data'] == None and response_json["errors"]:
            raise RuntimeError(f"An unknown error occurred. Raw response data: {response_json}")
        edges = response_json['data']['chatOfCode']['messagesConnection']['edges']
        
        if del_all == True:
            while True:
                if len(edges) == 0:
                    break
                message_ids = []
                for edge in edges:
                    message_ids.append(edge['node']['messageId'])
                self.delete_message(message_ids)
                sleep(0.5)
                response_json = self.send_request('gql_POST', 'ChatPageQuery', variables)
                edges = response_json['data']['chatOfCode']['messagesConnection']['edges']
            logger.info(f"Deleted {len(message_ids)} messages of {chatCode}")
        else:
            num = count
            while True:
                if len(edges) == 0 or num == 0:
                    break
                message_ids = []
                for edge in edges:
                    message_ids.append(edge['node']['messageId'])
                self.delete_message(message_ids)
                sleep(0.5)
                num -= len(message_ids)
                if len(edges) < num:
                    response_json = self.send_request('gql_POST', 'ChatPageQuery', variables)
                    edges = response_json['data']['chatOfCode']['messagesConnection']['edges']
            logger.info(f"Deleted {count-num} messages of {chatCode}")
            
    def purge_all_conversations(self):
        self.current_thread = {}
        self.send_request('gql_POST', 'DeleteUserMessagesMutation', {})
    
    def delete_chat(self, bot: str, chatId: any=None, chatCode: any=None, del_all: bool=False):
        bot = bot_map(bot)
        try:
            chatdata = self.get_chat_history(bot=bot)['data'][bot]
        except:
            raise RuntimeError(f"No chat found for {bot}. Make sure the bot has a chat history before deleting.")
        if chatId != None and not isinstance(chatId, list):
            if bot in self.current_thread:
                for thread in range(len(self.current_thread[bot])):
                    if self.current_thread[bot][thread]['chatId'] == chatId:
                        del self.current_thread[bot][thread]
                        break
            self.send_request('gql_POST', 'DeleteChat', {'chatId': chatId})
            logger.info(f"Chat {chatId} deleted")
        if del_all == True:
            if bot in self.current_thread:
                del self.current_thread[bot]
            for chat in chatdata:
                self.send_request('gql_POST', 'DeleteChat', {'chatId': chat['chatId']})
                logger.info(f"Chat {chat['chatId']} deleted")
        if chatCode != None:
                for chat in chatdata:
                    if isinstance(chatCode, list):
                        if chat['chatCode'] in chatCode:
                            chatId = chat['chatId']
                            if bot in self.current_thread:
                                for thread in range(len(self.current_thread[bot])):
                                    if self.current_thread[bot][thread]['chatId'] == chatId:
                                        del self.current_thread[bot][thread]
                                        break
                            self.send_request('gql_POST', 'DeleteChat', {'chatId': chatId})
                            logger.info(f"Chat {chatId} deleted")
                    else:
                        if chat['chatCode'] == chatCode:
                            chatId = chat['chatId']
                            if bot in self.current_thread:
                                for thread in range(len(self.current_thread[bot])):
                                    if self.current_thread[bot][thread]['chatId'] == chatId:
                                        del self.current_thread[bot][thread]
                                        break
                            self.send_request('gql_POST', 'DeleteChat', {'chatId': chatId})
                            logger.info(f"Chat {chatId} deleted")
                            break               
        elif chatId != None and isinstance(chatId, list):
            for chat in chatId:
                if bot in self.current_thread:
                    if self.current_thread[bot]:
                        for thread in range(len(self.current_thread[bot])):
                            if self.current_thread[bot][thread]['chatId'] == chat:
                                del self.current_thread[bot][thread]
                                break
                self.send_request('gql_POST', 'DeleteChat', {'chatId': chat})
                logger.info(f"Chat {chat} deleted")  
                
    def get_previous_messages(self, bot: str, chatId: int = None, chatCode: str = None, count: int = 50, get_all: bool = False, include_extended: bool = False):
        bot = bot_map(bot)
        try:
            getchatdata = self.get_threadData(bot, chatCode, chatId)
        except:
            raise RuntimeError(f"Thread not found. Make sure the thread exists before getting messages.")
        chatCode = getchatdata['chatCode']
        id = getchatdata['id']
        messages = []
        cursor = None
        edges = True

        if get_all:
            while edges:
                variables = {'count': 100, 'cursor': cursor, 'id': id}
                response_json = self.send_request('gql_POST', 'ChatListPaginationQuery', variables)
                chatdata = response_json['data']['node']
                edges = chatdata['messagesConnection']['edges'][::-1]
                for edge in edges:
                    message = {
                        'author': edge['node']['author'],
                        'text': edge['node']['text'],
                        'messageId': edge['node']['messageId'],
                        'contentType': edge['node']['contentType']
                    }
                    if include_extended:
                        message['state'] = edge['node'].get('state')
                        message['creationTime'] = edge['node'].get('creationTime')
                    messages.append(message)
                cursor = chatdata['messagesConnection']['pageInfo']['startCursor']
        else:
            num = count
            while edges and num > 0:
                variables = {'count': 100, 'cursor': cursor, 'id': id}
                response_json = self.send_request('gql_POST', 'ChatListPaginationQuery', variables)
                chatdata = response_json['data']['node']
                edges = chatdata['messagesConnection']['edges'][::-1]
                for edge in edges:
                    message = {
                        'author': edge['node']['author'],
                        'text': edge['node']['text'],
                        'messageId': edge['node']['messageId'],
                        'contentType': edge['node']['contentType']
                    }
                    if include_extended:
                        message['state'] = edge['node'].get('state')
                        message['creationTime'] = edge['node'].get('creationTime')
                    messages.append(message)
                    num -= 1
                    if len(messages) == count:
                        break
                cursor = chatdata['messagesConnection']['pageInfo']['startCursor']

        logger.info(f"Found {len(messages)} messages of {chatCode}")
        return messages[::-1]
    
    def get_user_bots(self, user: str):
        variables = {'handle': user}
        response_json = self.send_request('gql_POST', 'HandleProfilePageQuery', variables)
        if response_json['data'] == None and response_json["errors"]:
            raise RuntimeError(f"User {user} not found. Make sure the user exists before getting bots.")
        userData = response_json['data']['user']
        logger.info(f"Found {userData['createdBotCount']} bots of {user}")
        botsData = userData['createdBots']
        bots = [each['handle'] for each in botsData]
        return bots
        
    def complete_profile(self, handle: str=None):
        if handle == None:
            handle = ''.join(secrets.choice(string.ascii_letters + string.digits) for i in range(10))
        variables = {"handle" : handle}
        self.send_request('gql_POST', 'NuxInitialModal_poeSetHandle_Mutation', variables)
        self.send_request('gql_POST', 'MarkMultiplayerNuxCompleted', {})
    
    def get_available_knowledge(self, botName: str, count: int=10, get_all: bool=False):
        response = self.get_botInfo(botName)
        if response['viewerIsCreator'] == False:
            raise RuntimeError(f"You are not the creator of {botName}.")
        id = response["id"]
        sources_ids = {}
        new_variables = {"after": "5", "first": count, "id": id}
        response = self.send_request('gql_POST', 'BotKnowledgeSourcesModalPaginationQuery', new_variables)
        edges = response['data']['node']['knowledgeSourceConnection']['edges']
        total_sources = 0
        
        if edges:
            for edge in edges:
                if edge['node']['title'] not in sources_ids:
                    sources_ids[edge['node']['title']] = [edge['node']['knowledgeSourceId']]
                else:
                    sources_ids[edge['node']['title']].append(edge['node']['knowledgeSourceId'])
                total_sources = edge['cursor']
            while (len(sources_ids) < count or get_all):
                if response['data']['node']['knowledgeSourceConnection']['pageInfo']['hasNextPage']:
                    cursor = response['data']['node']['knowledgeSourceConnection']['pageInfo']['endCursor']
                else:
                    break
                new_variables = {"after": cursor, "first": count, "id": id }
                response = self.send_request('gql_POST', 'BotKnowledgeSourcesModalPaginationQuery', new_variables)
                if edges:
                    edges = response['data']['node']['knowledgeSourceConnection']['edges']
                    for edge in edges:
                        if edge['node']['title'] not in sources_ids:
                            sources_ids[edge['node']['title']] = [edge['node']['knowledgeSourceId']]
                        else:
                            sources_ids[edge['node']['title']].append(edge['node']['knowledgeSourceId'])
                        total_sources = edge['cursor']
        logger.info(f"Found {len(sources_ids)} unique knowledge sources out of {int(total_sources)+1} sources from {botName}")
        return sources_ids

    def upload_knowledge(self, file_path: Optional[list] = None, text_knowledge: Optional[list] = None):
        file_path = file_path or []
        text_knowledge = text_knowledge or []
        ids = {}
        if text_knowledge != []:
            for text in text_knowledge:
                if text != {} and "title" not in text and "content" not in text:
                    error_msg = f"Invalid text knowledge {text}. \nPlease make sure the text knowledge is in the format of " + "{'title': <str>, 'content': <str>}"
                    raise ValueError(error_msg)
                else:
                    response = self.send_request('gql_POST', 'Knowledge_CreateKnowledgeSourceMutation', {"sourceInput":{"text_input":{"title":text["title"],"content":text["content"]}}})
                    if response['data']['knowledgeSourceCreate']['status'] != 'success':
                        raise RuntimeError(f"Failed to upload text '{text['title']}'. \nRaw response data: {response}")
                    title = response['data']['knowledgeSourceCreate']['source']['title']
                    sourceid = response['data']['knowledgeSourceCreate']['source']['knowledgeSourceId']
                    if title not in ids:
                        ids[title] = [sourceid]
                    else:
                        ids[title].append(sourceid)
                    logger.info(f"Text '{text['title']}' uploaded successfully")
                    sleep(2)        
        if file_path != []:
            for path in file_path:
                file_form, file_size = generate_file([path], self.proxies)
                if file_size > 350000000:
                    raise RuntimeError("File size too large. Please try again with a smaller file.")
                response = self.send_request('gql_upload_POST', 'Knowledge_CreateKnowledgeSourceMutation', {"sourceInput":{"file_upload":{"attachment":"file"}}}, file_form, knowledge=True)
                if response['data']['knowledgeSourceCreate']['status'] != 'success':
                    raise RuntimeError(f"Failed to upload file '{path}'. \nRaw response data: {response}")
                title = response['data']['knowledgeSourceCreate']['source']['title']
                sourceid = response['data']['knowledgeSourceCreate']['source']['knowledgeSourceId']
                if title not in ids:
                    ids[title] = [sourceid]
                else:
                    ids[title].append(sourceid)
                for file in file_form:
                    logger.info(f"File '{file[0]}' uploaded successfully")
                sleep(2)
        logger.info(f"Knowledge uploaded successfully | {ids}")
        return ids
        
    def edit_knowledge(self, knowledgeSourceId: int, title: str=None, content: str=None):
        variables = {"knowledgeSourceId": knowledgeSourceId, 
                     "sourceInput":{
                        "text_input":{
                            "title": title,
                            "content": content
                        }
                    }}
        response = self.send_request('gql_POST', 'Knowledge_EditKnowledgeSourceMutation', variables)
        if response['data']['knowledgeSourceEdit']['status'] != 'success':
            raise RuntimeError(f"Failed to edit knowledge source {knowledgeSourceId}. \nRaw response data: {response}")
        logger.info(f"Knowledge source {knowledgeSourceId} edited successfully")
        
    def get_citations(self, messageId: int):
        variables = {"messageId": messageId}
        response = self.send_request('gql_POST', 'MessageCitationSourceModalQuery', variables)
        if response['data']['message'] == None:
            logger.info(f"No citations found for message {messageId}")
        else:
            citations = response['data']['message']['citations']
            logger.info(f"Found {len(citations)} citations for message {messageId}")
            return citations
        
    def get_available_creation_models(self):
        response = self.send_request('gql_POST', 'CreateBotIndexPageQuery', {'messageId': None})
        if response['data'] == None and response["errors"]:
            raise RuntimeError(f"An unknown error occurred. Raw response data: {response}")
        models_data = response['data']['viewer']['botsAllowedForUserCreation']
        models = {
            'text': [bot['model'] for bot in models_data if not bot['isImageGen'] and not bot['isVideoGen']],
            'image': [bot['model'] for bot in models_data if bot['isImageGen']],
            'video': [bot['model'] for bot in models_data if bot['isVideoGen']]
        }
        return models
            
    def create_bot(self, handle, prompt, display_name=None, base_model="chinchilla", botCategory: int=None, description="", intro_message="", 
                   api_key=None, api_bot=False, api_url=None, prompt_public=True, pfp_url=None, markdown_rendering=True,
                   suggested_replies=False, private=False, temperature=None, customMessageLimit=None, messagePriceCc=None,
                   shouldCiteSources=True, knowledgeSourceIds:dict = {}, allowRelatedBotRecommendations=True,
                   inviteCodeReqHash=None
                ):

        if not re.match("^[a-zA-Z0-9_.-]{4,20}$", handle):
            raise ValueError("Invalid handle. Should be unique and use 4-20 characters, including letters, numbers, dashes, periods and underscores.")
        
        botCategories = self.get_available_creation_models()
        bot_models = [model for sublist in botCategories.values() for model in sublist]
        if base_model not in bot_models:
            raise ValueError(f"Invalid base model {base_model}. Please choose from {bot_models}")
        
        if botCategory:
            if botCategory in (1, 4):
                if base_model not in botCategories['text']:
                    raise ValueError(f"Invalid base model {base_model} for category {botCategory}. Please choose from {botCategories['text']}")
            elif botCategory == 2:
                if base_model not in botCategories['image']:
                    raise ValueError(f"Invalid base model {base_model} for category {botCategory}. Please choose from {botCategories['image']}")
            elif botCategory == 3:
                if base_model not in botCategories['video']:
                    raise ValueError(f"Invalid base model {base_model} for category {botCategory}. Please choose from {botCategories['video']}")
            else:
                raise ValueError(f"Invalid bot category {botCategory}. Please choose from 1 (Prompt bot), 2 (Image generation), 3 (Video generation) or 4 (Role play)")
        else:
            botCategory = 1 if base_model in botCategories['text'] else 2 if base_model in botCategories['image'] else 3 if base_model in botCategories['video'] else None
        
        # Auto complete profile
        try:
            self.send_request('gql_POST', 'MarkMultiplayerNuxCompleted', {})
        except:
            self.complete_profile()
        if knowledgeSourceIds != {}:
            sourceIds = [item for sublist in knowledgeSourceIds.values() for item in sublist]
        else:
            sourceIds = []

        variables = {
            "botCategory": botCategory,
            "model": base_model,
            "displayName": display_name,
            "handle": handle,
            "prompt": prompt,
            "isPromptPublic": prompt_public,
            "introduction": intro_message,
            "description": description,
            "profilePictureUrl": pfp_url,
            "apiUrl": api_url,
            "apiKey": api_key,
            "isApiBot": api_bot,
            "hasMarkdownRendering": markdown_rendering,
            "hasSuggestedReplies": suggested_replies,
            "isPrivateBot": private,
            "temperature": temperature,
            "customMessageLimit": customMessageLimit,
            "knowledgeSourceIds": sourceIds,
            "messagePriceCc": messagePriceCc,
            "shouldCiteSources": shouldCiteSources,
            "allowRelatedBotRecommendations": allowRelatedBotRecommendations,
            "inviteCodeReqHash": inviteCodeReqHash
        }
        
        result = self.send_request('gql_POST', 'PoeBotCreate', variables)['data']['poeBotCreate']
        if result["status"] != "success":
           logger.error(f"Poe returned an error while trying to create a bot: {result['status']}")
        else:
           logger.info(f"Bot created successfully | {handle}")
        
    # get_bot logic 
    def get_botData(self, handle):
        variables = {"useChat":False,"useBotName":True,"useBotId":False,"useShareCode":False,"usePostId":False,"chatCode":0,"botName":handle,"botId":0,"shareCode":"","postId":0}
        try:
            response_json = self.send_request('gql_POST', 'LayoutRightSidebarQuery', variables)
            return response_json['data']['bot']
        except Exception as e:
            raise ValueError(
                f"Fail to get botId from {handle}. Make sure the bot exists and you have access to it."
            ) from e

    def edit_bot(self, handle, prompt, new_handle=None, display_name=None, base_model="chinchilla", botCategory: int=None, description="",
                intro_message="", api_key=None, api_url=None, private=False, prompt_public=True,
                pfp_url=None, markdown_rendering=True, suggested_replies=False, temperature=None, 
                customMessageLimit=None, knowledgeSourceIdsToAdd:dict = {}, knowledgeSourceIdsToRemove:dict = {},
                messagePriceCc=None, shouldCiteSources=True, allowRelatedBotRecommendations=True): 
         
        if new_handle and not re.match("^[a-zA-Z0-9_.-]{4,20}$", new_handle):
            raise ValueError("Invalid handle. Should be unique and use 4-20 characters, including letters, numbers, dashes, periods and underscores.") 
        
        botCategories = self.get_available_creation_models()
        bot_models = [model for sublist in botCategories.values() for model in sublist]
        if base_model not in bot_models:
            raise ValueError(f"Invalid base model {base_model}. Please choose from {bot_models}")  
        
        if botCategory:
            if botCategory in (1, 4):
                if base_model not in botCategories['text']:
                    raise ValueError(f"Invalid base model {base_model} for category {botCategory}. Please choose from {botCategories['text']}")
            elif botCategory == 2:
                if base_model not in botCategories['image']:
                    raise ValueError(f"Invalid base model {base_model} for category {botCategory}. Please choose from {botCategories['image']}")
            elif botCategory == 3:
                if base_model not in botCategories['video']:
                    raise ValueError(f"Invalid base model {base_model} for category {botCategory}. Please choose from {botCategories['video']}")
            else:
                raise ValueError(f"Invalid bot category {botCategory}. Please choose from 1 (Prompt bot), 2 (Image generation), 3 (Video generation) or 4 (Role play)")
        else:
            botCategory = 1 if base_model in botCategories['text'] else 2 if base_model in botCategories['image'] else 3 if base_model in botCategories['video'] else None
        
        if knowledgeSourceIdsToAdd != {}:
            addIds = [item for sublist in knowledgeSourceIdsToAdd.values() for item in sublist]
        else:
            addIds = []
        if knowledgeSourceIdsToRemove != {}:
            removeIds = [item for sublist in knowledgeSourceIdsToRemove.values() for item in sublist]
        else:
            removeIds = []
        
        try: 
            botId = self.get_botData(handle)['botId']
        except Exception as e:
            raise ValueError(
                f"Fail to get botId from {handle}. Make sure the bot exists and you have access to it."
            ) from e
            
        variables = {
            "botCategory": botCategory,
            "baseBot": base_model,
            "botId": botId,
            "handle": new_handle if new_handle != None else handle,
            "displayName": display_name,
            "prompt": prompt,
            "isPromptPublic": prompt_public,
            "introduction": intro_message,
            "description": description,
            "profilePictureUrl": pfp_url,
            "apiUrl": api_url,
            "apiKey": api_key,
            "hasMarkdownRendering": markdown_rendering,
            "hasSuggestedReplies": suggested_replies,
            "isPrivateBot": private,
            "temperature": temperature,
            "customMessageLimit": customMessageLimit,
            "knowledgeSourceIdsToAdd": addIds,
            "knowledgeSourceIdsToRemove": removeIds,
            "messagePriceCc": messagePriceCc,
            "shouldCiteSources": shouldCiteSources,
            "allowRelatedBotRecommendations": allowRelatedBotRecommendations,
        }
        
        result = self.send_request('gql_POST', 'PoeBotEdit', variables)["data"]["poeBotEdit"]
        if result["status"] != "success":
            logger.error(f"Poe returned an error while trying to edit a bot: {result['status']}")
        else:
            if new_handle and handle != new_handle:
                if handle in self.current_thread:
                    self.current_thread[new_handle] = self.current_thread.pop(handle)
                logger.info(f"Bot edited successfully | New handle from {handle} to {new_handle}")
            else:
                logger.info(f"Bot edited successfully | {handle}")
                
        return {"status": result["status"], "botId": botId, "handle": new_handle if new_handle != None else handle}
      
    def delete_bot(self, handle):
        isCreator = self.get_botData(handle)['viewerIsCreator']
        botId = self.get_botData(handle)['botId']
        try:
            if isCreator == True:
                response = self.send_request('gql_POST', "BotInfoCardActionBar_poeBotDelete_Mutation", {"botId": botId})
            else:
                response = self.send_request('gql_POST',
                    "BotInfoCardActionBar_poeRemoveBotFromUserList_Mutation",
                    {"connections": [
                        "client:Vmlld2VyOjA=:__HomeBotSelector_viewer_availableBotsConnection_connection"],
                        "botId": botId}
                )
        except Exception:
            raise ValueError(
                f"Failed to delete bot {handle}. Make sure the bot exists and belongs to you."
            )
        if response["data"] is None and response["errors"]:
            raise ValueError(
                f"Failed to delete bot {handle} :{response['errors'][0]['message']}"
            )
        else:
            logger.info(f"Bot deleted successfully | {handle}")
            
    def get_available_categories(self):
        categories = []
        response_json = self.send_request('gql_POST', 'ExploreBotsIndexPageQuery', {"categoryName":"defaultCategory"})
        if response_json['data'] == None and response_json["errors"]:
            raise RuntimeError(f"An unknown error occurred. Raw response data: {response_json}")
        else:
            for category in response_json['data']['exploreBotsCategoryObjects']:
                categories.append(category['categoryName'])
        return categories
                
    def explore(self, categoryName: str='defaultCategory', search: str=None, entity_type: str = "bot", count: int = 50, explore_all: bool = False):
        if entity_type not in ["bot", "user"]:
            raise ValueError(f"Entity type {entity_type} not found. Make sure the entity type is either bot or user.")
        if categoryName != 'defaultCategory' and categoryName not in self.get_available_categories():
            raise ValueError(f"Category {categoryName} not found. Make sure the category exists before exploring.")
        bots = []
        if search == None:
            query_name = "ExploreBotsListPaginationQuery"
            variables = {"categoryName": categoryName, "count": count}
            connectionType = "exploreBotsConnection"
        else:
            query_name = "SearchResultsListPaginationQuery"
            variables = {"query": search, "entityType": entity_type, "count": 50}
            connectionType = "searchEntityConnection"
            
        result = self.send_request("gql_POST", query_name, variables)
        if search == None:
            new_cursor = result["data"][connectionType]["edges"][-1]["cursor"]
        else:
            new_cursor = 60
            
        if entity_type == "bot":
            bots += [
                each["node"]['handle'] for each in result["data"][connectionType]["edges"]
            ]
        else:
            bots += [
                each["node"]['nullableHandle'] for each in result["data"][connectionType]["edges"]
            ]
        if len(bots) >= count and not explore_all:
            if entity_type == "bot":
                logger.info("Succeed to explore bots")
            else:
                logger.info("Succeed to explore users")
            return bots[:count]
        while len(bots) < count or explore_all:
            if search == None:
                result = self.send_request("gql_POST", query_name, {"categoryName": categoryName, "count": count, "cursor": new_cursor})
            else:
                result = self.send_request("gql_POST", query_name, {"query": search, "entityType": entity_type, "count": 50, "cursor": new_cursor})
            if len(result["data"][connectionType]["edges"]) == 0:
                if not explore_all:
                    if entity_type == "bot":
                        logger.info(f"No more bots could be explored, only {len(bots)} bots found.")
                    else:
                        logger.info(f"No more users could be explored, only {len(bots)} users found.")
                return bots
            if search == None:
                new_cursor = result["data"][connectionType]["edges"][-1]["cursor"]
            else:
                new_cursor += 50
            if entity_type == "bot":
                new_bots = [
                    each["node"]['handle'] for each in result["data"][connectionType]["edges"]
                ]
            else:
                new_bots = [
                    each["node"]['nullableHandle'] for each in result["data"][connectionType]["edges"]
                ]
            bots += new_bots
        
        if entity_type == "bot":
            logger.info("Succeed to explore bots")
        else:
            logger.info("Succeed to explore users")
        return bots[:count]
    
    def share_chat(self, bot: str, chatId: int=None, chatCode: str=None, count: int=None):
        bot = bot_map(bot)
        chatdata = self.get_threadData(bot, chatCode, chatId)
        chatCode = chatdata['chatCode']
        chatId = chatdata['chatId']
        variables = {'chatCode': chatCode}
        response_json = self.send_request('gql_POST', 'ChatPageQuery', variables)
        edges = response_json['data']['chatOfCode']['messagesConnection']['edges']
        if count == None:
            count = len(edges)
        message_ids = []
        for edge in edges:
            message_ids.append(edge['node']['messageId'])
        variables = {'chatId': chatId, 'messageIds': message_ids if count == None else message_ids[:count]}
        response_json = self.send_request('gql_POST', 'ShareMessageMutation', variables)
        if response_json['data']['messagesShare']:
            shareCode = response_json['data']['messagesShare']["shareCode"]
            logger.info(f'Shared {count} messages with code: {shareCode}')
            return shareCode
        else:
            logger.error(f'An error occurred while sharing the messages')
            return None
        
    def import_chat(self, bot:str="", shareCode: str=""):
        bot = bot_map(bot)
        variables = {'botName': bot, 'shareCode': shareCode, 'postId': None}
        response_json = self.send_request('gql_POST', 'ContinueChatCTAButton_continueChatFromPoeShare_Mutation', variables)
        if response_json['data']['continueChatFromPoeShare']['status'] == 'success':
            logger.info(f'Chat imported successfully')
            chatCode = response_json['data']['continueChatFromPoeShare']['messages'][0]['node']['chat']['chatCode']
            chatdata = self.get_threadData(bot, chatCode=chatCode)
            chatId = chatdata['chatId']
            return {'chatId': chatId, 'chatCode': chatCode}
        else:
            logger.error(f'An error occurred while importing the chat')
            return None
        
    def create_group(self, group_name: str = None, bots: Optional[list] = None):
        bots = bots or []
        if group_name == None:
            group_name = ''.join(secrets.choice(string.ascii_letters + string.digits) for i in range(10))
        else:
            group_name = group_name.replace(" ", "_")
            
        if bots == []:
            raise ValueError(f"Please provide at least one bot to create a group.")
            
        if group_name in self.groups:
            raise ValueError(f"Group {group_name} already exists. Please try again with a different group name.")
        
        bots_list = []
        for bot in bots:
            if 'name' not in bot:
                bot['name'] = bot['bot']
            if 'talkativeness' not in bot:
                bot['talkativeness'] = 0.5
            bots_list.append({'bot': bot_map(bot['bot']), 'name': bot['name'].lower(), 'chatId': None, 'chatCode': None, 'priority': 0, 'bot_log': [], 'talkativeness': bot['talkativeness']})
        self.groups[group_name] = {'bots': bots_list, 'conversation_log': [], 'previous_bot': '', 'dual_lock': ['','']}
        logger.info(f"Group {group_name} created with the following bots: {bots}")
        return group_name
    
    def delete_group(self, group_name: str):
        if group_name not in self.groups:
            raise ValueError(f"Group {group_name} not found. Make sure the group exists before deleting.")
        if self.groups[group_name]['bots'] != {}:
            for bot, chatdata in self.groups[group_name]['bots'].items():
                if chatdata['chatId'] != None:
                    self.delete_chat(bot, chatdata['chatId'])
        del self.groups[group_name]
        logger.info(f"Group {group_name} deleted")
        
    def get_available_groups(self):
        return self.groups
    
    def get_group(self, group_name: str):
        if group_name not in self.groups:
            raise ValueError(f"Group {group_name} not found. Make sure the group exists before getting.")
        return self.groups[group_name]
    
    def save_group_history(self, group_name: str, file_path: str=None):
        try:
            oldData = self.load_group_history(group_name, file_path=file_path)
            oldData = oldData['group_data']['conversation_log']
        except:
            oldData = None
        
        groupData = self.groups[group_name]
        if oldData != None:
            new_conversation_log = oldData + groupData['conversation_log']
        else:
            new_conversation_log = groupData['conversation_log']
        saveData = {
            'bots' : groupData['bots'],
            'conversation_log' : new_conversation_log,
            'previous_bot' : groupData['previous_bot'],
            'dual_lock' : groupData['dual_lock']
        }
        if file_path == None:
            file_path = group_name + '.json'
        else:
            # check if file path is valid and is a json file
            if not os.path.exists(file_path):
                raise ValueError(f"File path {file_path} is invalid.")
            if not file_path.endswith('.json'):
                raise ValueError(f"File path {file_path} is not a json file.")
        with open(file_path, 'w') as f:
            f.write(orjson.dumps(saveData, option=orjson.OPT_INDENT_2))
        logger.info(f"Group {group_name} saved to {file_path}")
        return file_path
        
    def load_group_history(self, file_path: str=None):
        if file_path == None:
            raise ValueError(f"Please provide a valid file path.")
        else:
            if not os.path.exists(file_path):
                raise ValueError(f"File path {file_path} is invalid.")
            if not file_path.endswith('.json'):
                raise ValueError(f"File path {file_path} is not a json file.")
            if os.stat(file_path).st_size == 0:
                raise ValueError(f"File path {file_path} is empty.")
        with open(file_path, 'rb') as f:
            groupData = orjson.loads(f.read())
        group_name = file_path.split('.')[0]
        self.groups[group_name] = groupData
        logger.info(f"Group {group_name} loaded from {file_path}")
        return {'group_name': group_name, 'group_data': groupData}
    
    def get_most_mentioned(self, group_name: str, message: str):
        mod_message = message.lower()
        bots = self.groups[group_name]['bots']
        if len(bots) == 1:
            return bots[0]
        if any(bot['name'] in mod_message for bot in bots):
            for bot in bots:
                bot['priority'] = 0
            for bot in bots:
                bot_model = bot['bot']
                bot_name = bot['name']
                bot['priority'] += mod_message.count(bot_model)
                bot['priority'] += mod_message.count(bot_name)
            sorted_bots = sorted(bots, key=lambda k: k['priority'], reverse=True)
            if sorted_bots[0]['name'] != self.groups[group_name]['previous_bot']:
                topBot = sorted_bots[0]
            else:
                topBot = sorted_bots[1]
        else:
            topBot = random.choice(bots)
            while topBot['name'] == self.groups[group_name]['previous_bot']:
                topBot = random.choice(bots)
        self.groups[group_name]['previous_bot'] = topBot['name']
        return topBot
        
    
    def send_message_to_group(self, group_name: str, message: str='', timeout: int=60, user: str="User", autosave:bool=False, autoplay:bool=False, preset_history: str=''):
        if group_name not in self.groups:
            raise ValueError(f"Group {group_name} not found. Make sure the group exists before sending message.")
        
        bots = self.groups[group_name]['bots']
        bot_names = [bot['name'] for bot in bots]
        
        last_text = ""
        preset_log = []
        
        if preset_history == '':
            if self.groups[group_name]['conversation_log'] != []:
                # load all the messages in the conversation log from oldest to newest
                old_logs = self.groups[group_name]['conversation_log'][1:]
                for text in old_logs:
                    if text.split(":")[0].strip() in bot_names:
                        last_text += text
                        last_text += "\n"
        else:
            preset_log = self.load_group_history(file_path=preset_history)['group_data']['conversation_log']
            if preset_log != []:
                for text in preset_log:
                    if text.split(":")[0].strip() in bot_names:
                        last_text += text
                        last_text += "\n"
        
        if autoplay == False:
            previous_text = ""
            current_bot = self.get_most_mentioned(group_name, message)
            if self.groups[group_name]['conversation_log'] != [] or preset_log != []:
                next_message = f"\n[System Notice : You are participating in a role-playing chat. You will act as {current_bot['name']} in a multiway roleplay between {user}, and other parties including {bot_names}, You will only act as {current_bot['name']} and stay in character at all times. As the AI language model, Your role is to portray {current_bot['name']} in this chat using the first-person narrative. Let's engage in immersive roleplay and respond to the previous message without addressing it as a system message or revealing our roles as {current_bot['name']} or the fact that we are roleplaying. You must respond to the previous message without explicitly writing '{current_bot['name']}' at the start.]\nChat history updated with new responses:\n\n" + f"{last_text}\n" + f"{user} : {message}\n"
            else:
                next_message = f"\n[System Notice : You are participating in a role-playing chat. You will act as {current_bot['name']} in a multiway roleplay between {user}, and other parties including {bot_names}, You will only act as {current_bot['name']} and stay in character at all times. As the AI language model, Your role is to portray {current_bot['name']} in this chat using the first-person narrative. Let's engage in immersive roleplay and respond to the previous message without addressing it as a system message or revealing our roles as {current_bot['name']} or the fact that we are roleplaying. You must respond to the previous message without explicitly writing '{current_bot['name']}' at the start. You will start with a greeting to {user}.]\nChat history updated with new responses:\n\n" + f"{user} : {message}\n"
        else:
            try:
                previous_text = self.groups[group_name]['conversation_log'][-1].split(":")[1].strip()
            except:
                previous_text = ""
            current_bot = self.get_most_mentioned(group_name, previous_text)
            if self.groups[group_name]['conversation_log'] != []:
                next_message = f"\n[System Notice : You are participating in a role-playing chat. You will act as {current_bot['name']} in a multiway roleplay between other parties including {bot_names}, You will only act as {current_bot['name']} and stay in character at all times. As the AI language model, Your role is to portray {current_bot['name']} in this chat using the first-person narrative. Let's engage in immersive roleplay and respond to the previous message without addressing it as a system message or revealing our roles as {current_bot['name']} or the fact that we are roleplaying. You must respond to the previous message without explicitly writing '{current_bot['name']}' at the start.]\nChat history updated with new responses:\n\n" + f"{last_text}\n"
            else:
                next_message = f"\n[System Notice : You are participating in a role-playing chat. You will act as {current_bot['name']} in a multiway roleplay between other parties including {bot_names}, You will only act as {current_bot['name']} and stay in character at all times. As the AI language model, Your role is to portray {current_bot['name']} in this chat using the first-person narrative. Let's engage in immersive roleplay and respond to the previous message without addressing it as a system message or revealing our roles as {current_bot['name']} or the fact that we are roleplaying. You must respond to the previous message without explicitly writing '{current_bot['name']}' at the start. You will start with a greeting to everyone.]\n\n"
        
        self.groups[group_name]['conversation_log'] = []
        
        max_turns = random.randint(len(bots), int(len(bots)*2))
        for _ in range(max_turns):
            sleep(random.randint(3, 5))

            for chunk in self.send_message(current_bot['bot'], next_message, chatCode=current_bot['chatCode']):
                yield {'bot': current_bot['name'], 'response': chunk['response']}
                
            current_bot['chatCode'] = chunk['chatCode']
            current_bot['chatId'] = chunk['chatId']
            
            self.groups[group_name]['conversation_log'].append(f"{current_bot['name']} : {chunk['text']}\n")
            previous_text = chunk['text']
            prev_bot = current_bot

            if current_bot['name'] not in self.groups[group_name]['dual_lock']:
                self.groups[group_name]['dual_lock'][0] = current_bot['name']
                 
            # Fetch the next most mentioned bot
            current_bot = self.get_most_mentioned(group_name, previous_text)
            
            # Append the second bot to dual lock  
            if current_bot['name'] in self.groups[group_name]['dual_lock']:
                # The same dual lock
                current_bot['bot_log'] = [self.groups[group_name]['conversation_log'][-1]]
            else:
                # New dual lock
                if len(self.groups[group_name]['conversation_log']) > 10:
                    current_bot['bot_log'] = self.groups[group_name]['conversation_log'][-10:]
                else:
                    current_bot['bot_log'] = self.groups[group_name]['conversation_log']  
                    
                for index in range(len(self.groups[group_name]['dual_lock'])):
                    if self.groups[group_name]['dual_lock'][index] != prev_bot['name']:
                        self.groups[group_name]['dual_lock'][index] = current_bot['name']
                        break
                    
            if autoplay == False:
                next_message = f"\n[System Notice : You are participating in a role-playing chat. You will act as {current_bot['name']} in a multiway roleplay between {user}, and other parties including {bot_names}, You will only act as {current_bot['name']} and stay in character at all times. As the AI language model, Your role is to portray {current_bot['name']} in this chat using the first-person narrative. Let's engage in immersive roleplay and respond to the previous message without addressing it as a system message or revealing our roles as {current_bot['name']} or the fact that we are roleplaying. You must respond to the previous message without explicitly writing '{current_bot['name']}' at the start.]\nChat history updated with new responses:\n\n"
            else:
                next_message = f"\n[System Notice : You are participating in a role-playing chat. You will act as {current_bot['name']} in a multiway roleplay between other parties including {bot_names}, You will only act as {current_bot['name']} and stay in character at all times. As the AI language model, Your role is to portray {current_bot['name']} in this chat using the first-person narrative. Let's engage in immersive roleplay and respond to the previous message without addressing it as a system message or revealing our roles as {current_bot['name']} or the fact that we are roleplaying. You must respond to the previous message without explicitly writing '{current_bot['name']}' at the start.]\nChat history updated with new responses:\n\n"

            for text in current_bot['bot_log']:
                if text.split(":")[0].strip() in bot_names:
                    next_message += text
                    next_message += "\n"
                    
        if autosave:
            self.save_group_history(group_name)

    def set_context_optimization(self, chat_id: int, enabled: bool) -> dict:
        variables = {
            "chatId": chat_id,
            "isContextOptimizationOn": enabled
        }
        
        return self.send_request(
            path="gql_POST",
            query_name="ChatSettingsModal_ChatSetContextOptimization_Mutation",
            variables=variables
        )
