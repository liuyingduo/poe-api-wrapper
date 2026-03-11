import io
import httpx
import openai

http_client = httpx.Client(
    trust_env=False,  # Do not route localhost calls through HTTP_PROXY/HTTPS_PROXY.
    timeout=httpx.Timeout(60.0, connect=5.0),
)
client = openai.OpenAI(
    api_key="svc_app_zaiwen",
    base_url="http://207.180.218.216:8004/v1/",
    http_client=http_client,
)

# client = openai.OpenAI(
#     api_key="svc_app_zaiwen",
#     base_url="http://127.0.0.1:8003/v1/",
#     http_client=http_client,
# )
content = '''
仙鼓路 36_原文
2026年03月04日 14:23
发言人   00:00
你明白我的意思吗？这是第一个事儿，这个事儿你没讲到。第二个事情你要讲到是说就是要讲一下具身智能这个产业里头，为什么数据重要？为什么你做的数据重要？你理解我的意思，这事儿是你要讲清楚的。因为确实有一些投资人，他已经今天感觉到数据是制约自身能力的最重要的瓶颈。就换句话来讲，我们要讲清楚说，至少你抛出你的一个观点，在机器人机身智能里什么最重要数据？第一重要本体，第二重要模型，第三重要模型不重要。

发言人   00:51
为什么模型不重要？我们讲今天都是4.2的公司。如果大家不管讲word model也好，还是讲巨大的死频道变得智能也好，12公分就做不了机座，用的全是开源的平台的。你无非今天你说你word model，你要么用阿里的通义的VL视觉语言模型，用了VL2.5对吧？你要么用一个阿里的one 2.3开源，那对不起，阿里one 3.1了，二点儿、32点儿、52点儿、六三点儿、03.1人后面都不开的。那么开源的2.3比他们新的那个差太多一元差太多了，对不对？那凭什么你能行呢？今天市场上要么用这开源的，要么用派0.5做了。

发言人   01:47
Thank you. 有什么壁垒？谁真正是自己做的模型没壁垒，至少对这些巨人就能12的公司没壁垒，就有人觉得你傻逼，那么我们就是同model的，他有看法，不管他，你就找你姓名。你的观点就是这样，模型没壁垒。在中国的12个公司没壁垒，太爱创造模型。但人家发了一个0.5，反正你们带了一片节奏，人家后来不发了。你们现在头部的，国内百亿的头部的不好几家不愿意派0.5？对不对？剩下搞直播的model的都阿里。

发言人   02:28
通义VL模型有什么壁垒？要弄一个VR也有什么壁垒，对不对？我们先说第一模型没壁垒。第二真正能使模型变得智能的是什么？是数据。自动驾驶搞了好几十年，真正大家开始猛搞也搞了十几年。为什么开始了？是第一家？刚刚去年还是前年，你要把这信息掌握清楚才搞出来。

发言人   03:01
他说真正L四级别的自动驾驶，那不就是数据不够吗？核心是数据不够。为什么他一特牛逼？是因为tester掌握的最大规模的真实的数据，typhon每年是我每年100亿公里的数据，你的数据不够。对不对？

发言人   03:22
真正让做成自动驾驶最好的是你的算法好，还是你的数据好？今天大家肯定承认算法也越来越接近，差不多肯定是数据corner case，对不对？你上来就要抛这个观点，就是他们今天去争解决这个问题，我们认为数据是最大的瓶颈，数据是最大的壁垒。为什么？

发言人   03:50
讲一下你对模型和数据的看法，讲一下自动驾驶，就拿自的对标。因为有的投资是要被教育的，这个共识很快就会形成。现在头部的一些投资就已经开始形成共识。数据最重要的投的模型，公司接下来拼的不是他搞那个模型，投谁拼谁搞数据搞得快搞数据搞得多，搞数据搞的真实。明白我的意思吧？对不对？

发言人   04:26
你现实中也可以举出例子来，那支援卖那么多鸡蛋干嘛你都采出去的吗？人性招手买那么多手，干嘛都是采数据的，对不对？特斯拉搞那么多人做瑶操，干嘛不都搞数据的吗？

发言人   04:44
第一个层面就是放弃你什么交互这些想法没有不用讲那个事儿，你讲那个事儿只能让你掉价，分散出去。你的军里面，你不要跟投资人觉得哇塞这儿有个西瓜你不捡，你天天想的是那个芝麻。什么叫严冬下蛋？西瓜是最后一个才是严冬下蛋。你现在投资人都认为你那芝麻是你想达到的终极目标，你他妈把西瓜当成年度下来，你这不make sense。

发言人   05:16
我自己当时还是觉得这个要长期做，但是我现在想想，既然现在居然这么热，我就只讲这二星座就是你的愿景。你放两年三年那是另外一个回事，后面再讲也来得及，那是后面的事情。你现在讲的就是因为你讲这事情别人不感兴趣，别人又分散别人的精力，别人又觉得你的判断力不行。你明白我的意思吗？这个市场这么好，你不去做，你分析今年那个那他觉得你投机分子，你本来不想做这个事儿，只不过想从这边拿钱，你去做这个事儿。所以第一个得把那个改了。

发言人   05:57
第二个要讲数据为什么重要。然后另外为什么你采的数据重要？为什么这种以手为中心的你采的这个数据重，对不对？这个要讲清楚。很显然手部操作是最复杂的对吧？这个是最需要关注的。但这也有很多的方案，对不对？比如说戴手套是比较常用的方案，另外用光学动图也是比较常见的方案。

发言人   06:27
这个当然你可以在后面列，但是你要解释为什么，不管是什么方案，为什么手部的数据那么的重要？我们需要高质量的手部的数据，对不对？你把这个事情要讲了。

发言人   06:48
然后就是你的方案，包括你的视频，你都得改为什么呢？你现在演示的全是为这个手环来的，你现在至少要给他一个感觉，是你带这个东西就能控手。因为你能控手，你就能采做转换才比较好的数据。所以你重点的演示的不是你那些手势姿势，你要是这个手怎么动怎么抓，那边有一个手得能动，得能抓住出跟你一样的姿势。有有对吧？有中间这个就是对，但是这个感觉反应有点慢。对，这个是临时巧手的，效果不太好。买马上买一个贵的。

发言人   07:31
你要搞一个反应比较快的，比较好的，然后要让大家能觉得它是同步的这他妈不同步，你明白要同步，要是好的自由度要高对吧？然后你是三个映射在一起，你可以把三个画面拼接，你手怎么动，他这手怎么动，然后你这边的骨骼重建怎么动，然后你告诉大家这是实时的对吧？然后最好能演示什么呢？最好能演示抓，你明白我的意思吗？最好能演示抓。因为大家今天看到的都是什么呢？要是戴手套的话，对吧？要么就是我拿着一个遥控器，我扣一下扳机，或者我怎么样明白那个没法控处理这个营销手就自由度比较低的这么一个专。你今天就等于你不用带什么设备，用你的手手环你就能解决这个问题，就能实时捕捉出来，实时重建，实时驱动这个手。

发言人   08:29
这个大家就觉得为什么要做的，而不是这个你可以演示，但这不是重点，明白重点就是要让大家建立一个你跟机器人到底有啥关系。这么一个事儿对吧？然后你再比较方案都是手很重要。那现在大家怎么解决手套物品或者动物，为什么？你好，吧？你没有束缚，你空间没有那个手套的间隙大小不一样。然后你你你最好有一页跟大家讲说，我操我这个红线时间偏差一毫米，不用适配手型，谁手上去都行，你是大手小手都能配上是吧？你说我的麦片特别好，我的教训特别好。因为今天你要用手套这些东西，你得专门先做高精度的校准，对吧？然后即使校准，因为它会形变，它这里面一定要有偏差。

发言人   09:32
各方面就是你要讲这些问题，一定要知道。第一放弃你做什么操作界面，新互动方式，那你搞的事情没人信你，你又不是做OS的，你又不是做设备的。你今天讲的就是我解决居民智能最重要的问题。因为大家今天就想投居身智能，已经一部分人认识到数据的最大问题。你就说我就是巨人集团公司放弃那些东西，然后我要解决的就是数据问题，数据就是局限智能里最重要的问题，不是模型，不是本地。数据是最重要的问题，跟智能相关。

发言人   10:13
而我做的少的数据就是数据里最重要的，第一重要的比什么这些跳舞的数据，local motion的数据，胳膊的数据重要的多的东西最重要的，对不对？你把这个讲清楚才行。而且要讲为什么是你的手环？你的手环还能干嘛？因为今天核心对于机器人来讲，机器人对空间的认识视觉它依赖于哪些东西？一个就是在眼睛上跟人一样放摄像头对吧？传感器要是放胸口的，手上怎么放是个很tRicky的问题。有人放手腕上，这儿立一个还要很高，去看见几乎所有的手上面都有这个东西，还有人放手心儿，还有人放手指尖。这都是你要思考解释的问题。

发言人   11:09
为什么你的方案是最好的？为什么所有的机器人公司不但你收集数据，他真用这个东西，他就用你的就行了。他就用你的设备感知手上的环境、手上的操作、手上的对象。他不用自己去买一个类似real sense或者那个谁的那些传感器，就用你的国内那家叫什么也在报上市，就对real对标real sense的。Real就是做是深度摄像头做感知的。国内的吗？是个传感器公司还是传感器公司？你这些你们都工作做得远远不够。

发言人   11:55
明白我的意思吗？就是你要考虑的一个就是采数据，另外一个所有机器人可能都需要上你的这个手环。你的手环从目前来看，给人用就是采数据，给机器用做感知就是做感知，做实时充电。

发言人   12:15
你要在这个两个方面去讲，对吧？所以你你你的第一页的那个slogan，你回去我看一下。这不是这玩意儿，你讲这干嘛对吧？手艺就是你其实这个核心的目标是说啥呢？是说你就是空手的专家，本质上是这个事儿。那你可以起一个fancy的一个口号，就是怎么讲？比如说the bridge of real hand and war motion had，就类似这样。明白我的意思吧？

发言人   13:00
其实我就是连接你就应该是人手握机器人的手，对吧？你就给一张图，这张图用在别的很俗，用在你身上这是对的。我就是一个bridge，我把人手的信号转成机器人手，对吧？我把机械手的感知它重建，变成像人手一样这样影响。明白我的意思吧？可以是吧？这大概表达的是这个意思，然后你再这么一眼看就很清楚了，你就是专门玩手的。

发言人   13:40
你。要站在这个起诉渠道，不然你还解释这个解释人机交互跟你人机交互跟你现在做居人智能有啥关系？居人智能他希望都留给机器人交互个屁，对不对？Inner face平台你不是我们不讲平台，一讲平台就解释不清楚。我们他妈就是能最好的最高效的才最重要的数据，对吧？这你就要解释几个事儿，为啥数据重要？为啥你的数据类型重要？为啥你最高效？

发言人   14:22
这个你就要解释，这是第一个方向，我两个方向，一个方向就是最高效才最有价值的学生枕头的数据，第二个事情就是我可以为新人提供手上的最最高效最精准的感知和空间，你只要能把这俩事儿讲清楚，大家都知道你牛逼了。然后这里面一定要用大厂来去帮你带节奏。你比如说。假如，但我不知道是不是真的，你要去研究一下。比如说特斯拉有没有在采用一些方式就着重补手的数据对吧？手的数据对特斯拉有多重要？如果你有消息有新闻或者一个bug说我要补手的数据对我们最重要。然后后来他说我能有多少成本，多少机器人再补数据，那你就要写在那儿，伊拉玛斯的认为手的数据是最重要的，你这些必须写在这儿，这他妈帮你带节奏的，因为你说重要不管用，他说重要就管用，对不对？

发言人   15:43
包括数据的难度，数据的价值。就算他不养他的optimus，那在texel ler里的自动驾驶总总有很多文章，很多包括伊拉纳斯或者包括他们之间什么手续费也讲这个事。这个事你要翻出来说，自动驾驶肯定就在数据，以此类推，这个就是数据。自动驾驶数据你还现实，有的车你车确实在路上跑，你有用，而且确实不断在收集数据。那你金额数据那就变成先有鸡后有蛋，还是先有蛋后有鸡的。
'''
# ── Non-Streaming Example ──────────────────────────────────────────────────────
# response = client.chat.completions.create(
#     model="gpt-3.5-turbo",
#     messages=[
#         {"role": "system", "content": "You are a helpful assistant."},
#         {"role": "user", "content": "Hello!"},
#     ],
# )
# print(response.choices[0].message.content)

# # ── Streaming Example ──────────────────────────────────────────────────────────
stream = client.chat.completions.create(
    model="claude-opus-4.6",
    messages=[
        {"role": "user", "content": "请总结一下下面这段话的主要内容，要求用中文回答：\n\n" + content},
    ],
    stream=True,
)
for chunk in stream:
    if chunk.choices[0].delta.content is not None:
        print(chunk.choices[0].delta.content, end="")
# print()

# # # ── Vision / Image-Input Example ───────────────────────────────────────────────
IMAGE_URL = "https://ossnew.zaiwen.top/images/e95211642901a534d1ae572b5615f138b2b78c07aade05f2265626d0410c8deb.jpeg"

# image_input = client.chat.completions.create(
#     model="GPT-4o",
#     messages=[
#         {
#             "role": "user",
#             "content": [
#                 {"type": "text", "text": "What's in this image?"},
#                 {"type": "image_url", "image_url": {"url": IMAGE_URL}},
#             ],
#         }
#     ],
# )
# print(image_input.choices[0].message.content)

# ── Image-Edit Example ─────────────────────────────────────────────────────────
# Download the reference image and pass it as a file-like object.
image_bytes = http_client.get(IMAGE_URL).content
edit_resp = client.images.edit(
    model="flux-2-klein-9b-base",
    image=("reference.jpg", image_bytes, "image/jpeg"),
    prompt="add a little flower",
    n=1,
    size="1024x1024",
)
print(edit_resp.data[0].url)


# ── File Analysis Example ──────────────────────────────────────────────────────
# 支持图片、PDF 等文件 URL，模型会分析文件内容并回答问题。
# 多个文件直接在 content 列表里追加多个 image_url 条目即可。

# ANALYZE_FILES = [
#     "https://lib.hueb.edu.cn/__local/C/03/BD/ABAB4CD4F1A81D63A6B2C978081_B112E78E_1198F9.pdf?e=.pdf",
# ]
# ANALYZE_PROMPT = "请分析这些文件的主要内容，用中文回答。"
# ANALYZE_MODEL = "GPT-4o"

# content_parts = [{"type": "text", "text": ANALYZE_PROMPT}]
# for f in ANALYZE_FILES:
#     content_parts.append({"type": "file", "file": {"url": f}})

# file_analysis = client.chat.completions.create(
#     model=ANALYZE_MODEL,
#     messages=[{"role": "user", "content": content_parts}],
#     stream=True,
# )
# print(f"\n=== 文件分析结果 ({len(ANALYZE_FILES)} 个文件) ===")
# for chunk in file_analysis:
#     if chunk.choices[0].delta.content is not None:
#         print(chunk.choices[0].delta.content, end="")
# print()


# import concurrent.futures
# import time

# TOTAL_REQUESTS = 1
# PROMPT = "A cute girl with a little flower"
# MODEL = "Qwen-Image"

# def generate_image(index: int):
#     try:
#         start = time.time()
#         result = client.images.generate(
#             model=MODEL,
#             prompt=PROMPT,
#             n=1,
#         )
#         elapsed = time.time() - start
#         url = result.data[0].url if result.data else None
#         print(f"[{index:03d}] 成功 ({elapsed:.2f}s): {url}")
#         return {"index": index, "success": True, "url": url, "elapsed": elapsed}
#     except Exception as e:
#         print(f"[{index:03d}] 失败: {e}")
#         return {"index": index, "success": False, "error": str(e)}

# print(f"开始发起 {TOTAL_REQUESTS} 个并发图片生成请求...")
# overall_start = time.time()

# with concurrent.futures.ThreadPoolExecutor(max_workers=TOTAL_REQUESTS) as executor:
#     futures = {executor.submit(generate_image, i): i for i in range(1, TOTAL_REQUESTS + 1)}
#     results = [future.result() for future in concurrent.futures.as_completed(futures)]

# overall_elapsed = time.time() - overall_start
# success_count = sum(1 for r in results if r["success"])
# fail_count = TOTAL_REQUESTS - success_count

# print(f"\n===== 测试完成 =====")
# print(f"总请求数: {TOTAL_REQUESTS}")
# print(f"成功: {success_count} | 失败: {fail_count}")
# print(f"总耗时: {overall_elapsed:.2f}s")