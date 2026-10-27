#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
钉钉 <-> zeroclaw 远程控制机器人（合并通用版 v3.2：识别 + 发送 双通 / 长任务不中断）
完整使用手册 + 技术原理见【文件末尾 TECHNICAL NOTES】，本块只放最小上手信息。

原理一句话：bot 主动外连钉钉 WebSocket（Stream 模式），收到消息后 fork 子进程跑
    zeroclaw agent -a <AGENT> -m "<内容>"，再把结果（文本 / 文件 / 图片）发回钉钉。
    Stream 是出站连接，不需要公网回调地址 / 域名 / HTTPS 证书，与安全组入站规则无关。

============================ 5 分钟上手 ============================
  1) 替换脚本顶部占位符。必填 5 个：
       <CLIENT_ID>  <CLIENT_SECRET>  <ZEROCLAW_PATH>  <AGENT_NAME>  <HOME_DIR>
     可选 3 个（只在 VISION_ON=True 开启视觉兜底时才需要填）：
       <VISION_URI>  <VISION_KEY>  <VISION_MODEL>
     其余全部可用默认值：WS_DIR / SCAN_DIRS / RUN_ENV 都由 HOME + AGENT 派生。
  2) 装依赖（websockets 必须 <14）：
       pip3 install --break-system-packages dingtalk-stream "websockets<14" requests
  3) 放到 /opt/dingtalk-bot/bot.py（systemd 单元模板见文件末尾 [0.4] 节）
  4) systemctl daemon-reload && systemctl enable --now dingbot
  5) journalctl -u dingbot -f 看日志

自检（钉钉里依次发这 4 条）：
  !date                 -> 返回时间，说明 shell 通道通
  #file /etc/hostname   -> 收到文件卡片，说明发送通道通
  报一下当前负载         -> agent 回中文报告，说明 agent 通道通
  一张图片               -> agent 描述图片内容，说明识别通道通

============================ 指令一览 ============================
  中文句子            -> 交给 zeroclaw agent 理解执行（支持 [FILE] 自动回传文件）
  !命令               -> 直接跑 shell（如 !df -h），120s 超时
  #file <路径>        -> 把服务器上任意文件/图片发回钉钉（别名 !file / !pic / !img）
  !vision <图片路径>  -> 不经 agent，直接用视觉模型描述这张图（排查用）
  钉钉发图片          -> 下载 → 识别 → agent 分析（识别不了会原图回传）

============================ v3.2 解决了什么 ============================
  症状：模型还在思考，任务却在 300s 被硬杀；界面像卡死，有时还会重复执行同一条。
  根因有三个，全部已修（详见文件末尾 [15]）：

    1) TIMEOUT_AGENT 只有 300s —— 现在提到 3600s，且它是「上限」不是期待值，
       子进程还在跑就一直等；万一真跑满，也会把已完成的部分输出回传，不白干。
    2) process() 是同步阻塞的 —— 执行期间 SDK 的 event loop 被占住，心跳发不出去，
       钉钉判定机器人掉线并重投消息。现在改为「先 ACK，再丢线程池后台执行」。
    3) 子进程输出走管道 —— 输出多时管道写满会反向阻塞子进程，且超时后 stdout 常为空。
       现在重定向到临时文件，随时可读。

  顺带：超时后按进程组 kill（防止 zeroclaw 变孤儿继续烧 token）；
        长任务到点主动发「还在跑」进度提示；
        SDK 回复失败时降级走 OpenAPI（sessionWebhook 有时效）。

============================ 本版合了什么 ============================
  A 版（旧 bot.py）：能发图发文件，但收不到 / 识别不了钉钉来的图片
      ★ 可用资产：sessionWebhook 发送通道（base64 内联发图已实测成功）
                  图片下载 downloadCode、产物图扫描推送、视觉兜底
  B 版（完整版）：能识别，但发文件发图失败
      ★ 可用资产：#file 指令、[FILE] 投递约定、二进制护栏、
                  OpenAPI sampleImageMsg / sampleFile 主动发送通道

  合并策略（关键）：
    1) 【收图识别】走 A 版链路：downloadCode 下载 → 落盘 → zeroclaw image_info 识别
       → agent 若确实读不了图，把原图回传给用户（IMAGE_FALLBACK）
    2) 【发图发文件】统一收敛到一个 deliver() 出口，图片按显示效果降级：
         ① sessionWebhook base64 内联（直接显示图，★勿加人为大小门槛）
         ② OpenAPI sampleImageMsg photoURL=media_id（也能直接显示，需应用权限）
         ③ webhook file 卡片（只能下载，末级兜底）
       文件走 ③ webhook file 卡片 → ② OpenAPI sampleFile
    3) 【token 严格分家】
         api.dingtalk.com（新版）→ 必须用 /v1.0/oauth2/accessToken 的 accessToken
         oapi.dingtalk.com（老版）→ 必须用 /gettoken 的 access_token（上传只认这个）
"""

import asyncio
import base64
import functools
import hashlib
import json
import os
import re
import shlex
import shutil
import signal
import struct
import subprocess
import tempfile
import time

import requests
import dingtalk_stream
from dingtalk_stream import AckMessage

# ============================ 配置区（改这里） ============================
# ---- 部署时必须替换的 5 个占位符 ----
CLIENT_ID     = "<CLIENT_ID>"       # 钉钉开放平台 → 应用 → 凭证与基础信息 → Client ID（AppKey）
CLIENT_SECRET = "<CLIENT_SECRET>"   # 同上页面的 Client Secret（AppSecret）
ZEROCLAW      = "<ZEROCLAW_PATH>"   # 服务器上 `which zeroclaw` 的绝对路径
                                    #   systemd 的 PATH 不含 cargo 目录，必须写绝对路径
                                    #   实测环境示例：/root/.cargo/bin/zeroclaw
AGENT         = "<AGENT_NAME>"      # zeroclaw agent 别名；实测环境示例：ops
HOME          = "<HOME_DIR>"        # 运行 bot 的系统用户家目录；root 即 /root

# ---- 图片识别（可选配置：VISION_ON=False 时，下面 3 个填不填都不影响运行）----
# 默认关闭的原因：zeroclaw agent 自带 image_info 工具，能直接读取服务器本地图片，
#                不需要额外接视觉 API。只有当你的 agent 确实读不了图时才需要打开。
# 开启方法：VISION_ON = True，并把下面 3 个占位符换成你自己的 OpenAI 兼容视觉端点。
VISION_ON    = False
VISION_URI   = "<VISION_URI>"        # 可选。OpenAI 兼容端点，如 https://api.example.com/v1
VISION_KEY   = "<VISION_KEY>"        # 可选。该端点的 key；没有鉴权就填 ""
VISION_MODEL = "<VISION_MODEL>"      # 可选。必须支持视觉，如 gpt-4o / qwen-vl-max / glm-4v
VISION_PROMPT = "请用中文简洁描述这张图片，重点提取：文字内容、报错信息、界面结构、关键数据。"
# agent 明确表示读不了图时，把原图直接回传给用户，至少让人眼能看到
IMAGE_FALLBACK = True

MEDIA_DIR = "/opt/dingtalk-bot/media"    # 钉钉发来的图片落盘目录（运行用户需有写权限）
WS_DIR = os.path.join(HOME, ".zeroclaw", "agents", AGENT, "workspace")
                                         # zeroclaw 默认工作区，agent 生成的文件落在这里
                                         # 若改过 zeroclaw 的 workspace 配置，直接写绝对路径

# ---- agent 跑完后自动把新产生的图片/文件推到钉钉 ----
AUTO_SEND_IMAGE = True
MAX_IMAGE_MB = 10                        # 自动推送的单图上限
MAX_UPLOAD_MB = 20                       # 钉钉媒体上传硬上限
SCAN_DIRS = ["/tmp", HOME, MEDIA_DIR, WS_DIR]   # 可按需追加：网站根目录、项目目录等
SCAN_MAX_DEPTH = 3                       # 扫描深度，防止遍历整个磁盘
SKIP_DIRS = ("node_modules", ".git", "cache", "logs", "vendor", "__pycache__")

# ---- 发送降级链（写死在 deliver() 里，无需配置） ----
# 图片：①webhook base64 内联(直接显示) → ②OpenAPI sampleImageMsg(直接显示,需应用权限)
#       → ③webhook file 卡片(只能下载) → ④markdown 公网链接(需配 PUBLISH_DIR)
# 文件：①webhook file 卡片 → ②OpenAPI sampleFile → 文字报错

# 兜底：把文件拷到网站目录用公网链接发（没装 nginx 就保持为空，不影响其它通道）
PUBLISH_DIR = ""                         # 例："/www/wwwroot/media"
PUBLIC_BASE_URL = ""                     # 例："http://example.com/media"

# ---- 超时与长任务（v3.2 重点改造，见文件末尾 [15]） ----
# TIMEOUT_AGENT 是「最坏情况下的上限」，不是期待值：只要子进程还在跑就一直等，
# 跑满才中断。原来设 300 会把还在思考的模型硬杀掉 —— 现在给到 1 小时。
# ★ 真正的卡点往往不在本脚本：zeroclaw 自己的 provider 也有超时（timeout_secs），
#   必须同步调大，否则模型侧先断。见文件末尾 [10] P14。
TIMEOUT_SHELL = 120                      # !命令 上限
TIMEOUT_AGENT = 3600                     # 交给大模型/multi-step 任务的上限（1 小时）
TIMEOUT_VISION = 120

# 进度提示：长任务沉默太久用户会以为挂了，到点主动报一下还在跑
PROGRESS_HINT = True
PROGRESS_AFTER = 120                     # 超过 120s（2 分钟）发第一条「还在跑」
PROGRESS_EVERY = 180                     # 之后每 180s（3 分钟）再报一次
PROGRESS_MAX = 10                        # 单任务最多报几条

# 同时执行的长任务上限（超出只提示，避免线程池被占满后心跳线程都排不上）
AGENT_MAX_CONCURRENT = 3

MAX_OUT = 3000
# ==========================================================================

IMG_EXT = (".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp")
ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
RUN_ENV = {"PATH": os.path.join(HOME, ".cargo/bin")
                   + ":/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
                   # 必须包含 zeroclaw 所在目录，否则 systemd 下调用会 command not found
           "HOME": HOME, "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8"}

# media_id 缓存：key=(路径, mtime)，避免同一文件重复上传钉钉
_media_cache = {}
_sent_images = set()      # 已自动推送过的图，防重复


def clean(s, limit=MAX_OUT):
    """去 ANSI 控制符 + 截断"""
    s = ANSI.sub("", (s or "").strip()) or "(无输出)"
    return s[:limit] + ("\n... 已截断" if len(s) > limit else "")


# ------------------------- shell 执行（含二进制护栏，来自 B 版） -------------------------

def _is_binary(data):
    """判定输出是否为二进制。真二进制（图片 / zip / pdf 等）不能回灌文本给钉钉"""
    # 性能优化：只扫头部 64KB + 尾部 8KB，避免大文件全量遍历
    head, tail = data[:65536], data[-8192:] if len(data) > 65536 else b""
    for seg in (head, tail):
        if not seg:
            continue
        if len(seg) >= 8 and seg[:8] == b"\x89PNG\r\n\x1a\n":
            return True
        if seg[:4] in (b"GIF8", b"%PDF") or seg[:2] in (b"PK", b"\x1f\x8b", b"BM", b"\xff\xd8"):
            return True
        if len(seg) < 64:
            continue
        ctrl = sum(1 for b in seg if b < 32 and b not in (9, 10, 13))
        if ctrl / len(seg) > 0.05:
            return True
        try:
            seg.decode("utf-8")
        except UnicodeDecodeError:
            try:
                seg.decode("gbk")
            except UnicodeDecodeError:
                return True
    return False


def _kill_tree(proc):
    """杀整个进程组。shell=True 会先起 sh 再 fork zeroclaw，
    只 kill 顶层 sh 会留下 zeroclaw 孤儿继续烧 token —— 必须按组杀"""
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass


def run(cmd, timeout, on_tick=None, tick_after=0, tick_every=0, tick_max=0):
    """执行 shell 命令。v3.2 相比 v3.1 的三处改动（都是为长任务）：

      ① 子进程输出重定向到【临时文件】而不是管道
         管道缓冲区写满会反向阻塞子进程（输出多时必现），而且 subprocess.run(timeout)
         超时后拿到的 stdout 经常是空的；落文件则随时可读，即便被中断也能把
         已完成的那部分输出回传给用户，不至于几百秒的活彻底白干。
      ② 超时后按【进程组】kill，避免留下 zeroclaw 孤儿进程
      ③ 支持 on_tick(elapsed) 进度回调，长任务到点主动发声（配合 PROGRESS_* 配置）

      注意：timeout 是「上限」不是期待值 —— 只要子进程还在跑就一直等。
    """
    tmp = None
    timed_out = False
    try:
        fd, tmp = tempfile.mkstemp(prefix="dingbot_", suffix=".out")
        os.close(fd)
        t0 = time.time()
        with open(tmp, "wb") as fo:
            p = subprocess.Popen(cmd, shell=True, stdout=fo, stderr=subprocess.STDOUT,
                                 stdin=subprocess.DEVNULL, cwd=HOME, env=RUN_ENV,
                                 start_new_session=True)   # 独立进程组，便于整组 kill

        ticks = 0
        nxt = t0 + (tick_after or timeout + 1)
        while p.poll() is None:
            now = time.time()
            if now - t0 >= timeout:
                timed_out = True
                _kill_tree(p)
                print("[timeout] %ss 到点已杀进程组: %s" % (timeout, cmd[:80]), flush=True)
                break
            if on_tick and ticks < tick_max and now >= nxt:
                ticks += 1
                try:
                    on_tick(int(now - t0))
                except Exception as e:
                    print("[tick] 进度回调失败: %s" % e, flush=True)
                nxt = t0 + tick_after + ticks * tick_every   # 按起点算绝对时刻，避免逐次漂移
            time.sleep(0.4)
        if not timed_out:               # 主进程已退出，兜底等一下，防孙进程还在写文件
            try:
                p.wait(timeout=5)
            except Exception:
                pass

        with open(tmp, "rb") as f:
            raw = f.read()
    except Exception as e:
        return clean("执行失败: %s" % e)
    finally:
        if tmp:
            try:
                os.remove(tmp)
            except Exception:
                pass

    if _is_binary(raw):
        prefix = raw.split(b"\x89PNG")[0].split(b"PK\x03\x04")[0]
        head_txt = ANSI.sub("", prefix.decode("utf-8", errors="ignore").strip())[:300]
        info = ("(检测到二进制输出 %d 字节，已省略。如需在钉钉里查看请发送: #file <路径>)" % len(raw))
        print("[bin] blocked %d bytes, cmd=%s" % (len(raw), cmd[:80]), flush=True)
        return (head_txt + "\n\n" + info) if head_txt else info

    try:
        out = raw.decode("utf-8")
    except UnicodeDecodeError:
        try:
            out = raw.decode("gbk")
        except UnicodeDecodeError:
            out = raw.decode("utf-8", errors="replace")
    out = out.strip() or "(无输出)"

    if timed_out:
        out += "\n\n[已到 %ds 上限被中断，以上是中断前已完成的部分输出]" % timeout
    return clean(out)


# ------------------------- 两套 token（严格分家，勿混用） -------------------------

_new_tok = {"v": "", "exp": 0}
_old_tok = {"v": "", "exp": 0}


def get_token():
    """新版 accessToken：给 api.dingtalk.com 用（下载图片、OpenAPI 主动发送）
    头部字段固定为 x-acs-dingtalk-access-token"""
    if _new_tok["v"] and time.time() < _new_tok["exp"]:
        return _new_tok["v"]
    r = requests.post("https://api.dingtalk.com/v1.0/oauth2/accessToken",
                      json={"appKey": CLIENT_ID, "appSecret": CLIENT_SECRET},
                      timeout=15).json()
    if "accessToken" not in r:
        raise RuntimeError("获取 accessToken 失败: %s" % str(r)[:200])
    _new_tok["v"] = r["accessToken"]
    _new_tok["exp"] = time.time() + 7000     # 官方 7200s，留 200s 余量
    return _new_tok["v"]


def get_legacy_token():
    """老版 access_token：给 oapi.dingtalk.com 用（/media/upload 只认这一套）
    必须走 query 参数 ?access_token="""
    if _old_tok["v"] and time.time() < _old_tok["exp"]:
        return _old_tok["v"]
    r = requests.get("https://oapi.dingtalk.com/gettoken",
                     params={"appkey": CLIENT_ID, "appsecret": CLIENT_SECRET},
                     timeout=15).json()
    if r.get("errcode") != 0:
        raise RuntimeError("获取老版 access_token 失败: %s" % str(r)[:200])
    _old_tok["v"] = r["access_token"]
    _old_tok["exp"] = time.time() + 7000
    return _old_tok["v"]


# ------------------------- 媒体上传 -------------------------

def upload_media(path):
    """上传本地文件到钉钉媒体服务器 → media_id（带 mtime 缓存）
    表单字段名固定是 media；图片 type=image，其它 type=file"""
    if not os.path.isfile(path):
        raise RuntimeError("文件不存在: %s" % path)
    size = os.path.getsize(path)
    if size > MAX_UPLOAD_MB * 1024 * 1024:
        raise RuntimeError("文件 %.1fMB 超过钉钉 %dMB 上限" % (size / 1048576.0, MAX_UPLOAD_MB))
    key = (path, int(os.path.getmtime(path)))
    if key in _media_cache:
        return _media_cache[key]

    ftype = "image" if path.lower().endswith(IMG_EXT) else "file"
    with open(path, "rb") as f:
        r = requests.post("https://oapi.dingtalk.com/media/upload",
                          params={"access_token": get_legacy_token(), "type": ftype},
                          files={"media": (os.path.basename(path), f)},
                          timeout=120).json()
    if r.get("errcode") != 0 or not r.get("media_id"):
        raise RuntimeError("上传失败: %s" % str(r)[:250])
    _media_cache[key] = r["media_id"]
    return r["media_id"]


# ------------------------- 发送通道 ①：sessionWebhook -------------------------

def post_session(sw, payload):
    """往当前会话推消息。★ 必须校验 errcode：
    sessionWebhook 业务失败时也返回 HTTP 200，只看 HTTP 状态会误判成成功"""
    r = requests.post(sw, json=payload, timeout=25).json()
    err = r.get("errcode")
    if err not in (0, None):
        raise RuntimeError("钉钉拒绝: errcode=%s %s" % (err, str(r.get("errmsg", ""))[:150]))
    return r


def image_b64(path):
    """返回 (base64串, md5)。钉钉 webhook image 消息要 base64 + md5 成对"""
    with open(path, "rb") as f:
        data = f.read()
    return base64.b64encode(data).decode(), hashlib.md5(data).hexdigest()


def send_b64(sw, path):
    """图片直显①：sessionWebhook base64 内联 —— A 版实测成功、钉钉里直接显示图片，
    不需要任何外部服务 / 公网 URL。★ 不要人为加大小门槛拦它（v3.1 曾加 2MB 上限，
    结果 1.png 被拦去走文件卡片，用户看到的是"文件"而不是图）；真失败由 deliver 降级"""
    size = os.path.getsize(path)
    if size > MAX_UPLOAD_MB * 1024 * 1024:
        raise RuntimeError("图片 %.1fMB 超过 %dMB，跳过 base64 内联" % (size / 1048576.0, MAX_UPLOAD_MB))
    b64, md5 = image_b64(path)
    post_session(sw, {"msgtype": "image", "image": {"base64": b64, "md5": md5}})


def send_wh_file(sw, path):
    """webhook file 卡片：mediaId + fileName + fileType。
    图片走这里会变成"可下载的文件"，不内联显示 —— 所以只作图片的末级兜底。
    ★ 图片不要走 {"image":{"mediaId":..}} —— 必报 400802 miss param: image->picURL"""
    mid = upload_media(path)
    name = os.path.basename(path)
    post_session(sw, {"msgtype": "file", "file": {
        "mediaId": mid,
        "fileName": name,
        "fileType": name.rsplit(".", 1)[-1].lower() if "." in name else "bin",
    }})


# ------------------------- 发送通道 ②：OpenAPI 主动发送 -------------------------

def _pick(msg, names):
    """不同版本 SDK 属性名不一致（下划线 / 驼峰 / 原始 dict），按优先级逐个试"""
    for n in names:
        v = getattr(msg, n, None)
        if v:
            return v
    for holder in (getattr(msg, "data", None), getattr(msg, "message_dict", None)):
        if isinstance(holder, dict):
            for n in names:
                v = holder.get(n)
                if v:
                    return v
    return ""


def openapi_post(msg, msg_key, msg_param):
    """OpenAPI 主动发送的通用出口（群聊 / 单聊自动分流）
    需要应用开通「企业内机器人发送消息」权限并发布新版本"""
    ctype = str(_pick(msg, ("conversation_type", "conversationType")))   # "1"=单聊 "2"=群聊
    conv_id = str(_pick(msg, ("conversation_id", "conversationId")))
    staff = str(_pick(msg, ("sender_staff_id", "senderStaffId")))

    if ctype == "2":
        url = "https://api.dingtalk.com/v1.0/robot/groupMessages/send"
        payload = {"msgParam": msg_param, "msgKey": msg_key,
                   "openConversationId": conv_id, "robotCode": CLIENT_ID}
        via = "groupMessages"
    elif ctype == "1" and staff:
        url = "https://api.dingtalk.com/v1.0/robot/oToMessages/batchSend"
        payload = {"robotCode": CLIENT_ID, "userIds": [staff],
                   "msgParam": msg_param, "msgKey": msg_key}
        via = "oToMessages"
    else:
        raise RuntimeError("无法识别会话类型 conversationType=%r" % ctype)

    r = requests.post(url, json=payload,
                      headers={"x-acs-dingtalk-access-token": get_token()}, timeout=20)
    print("[openapi] HTTP %s -> %s/%s" % (r.status_code, url.rsplit("/", 1)[-1], msg_key), flush=True)
    if not r.ok:
        raise RuntimeError("HTTP %s: %s" % (r.status_code, r.text[:200]))
    body = r.json() if r.text else {}
    if body.get("code"):
        raise RuntimeError("业务失败: %s" % str(body)[:200])
    return via


def openapi_send_text(msg, text, title="ZeroClaw"):
    """★ 文本兜底通道。背景：sessionWebhook 有时效，长任务跑几十分钟后
    SDK 自带回复 / webhook 可能已失效，用户会「什么都收不到」。
    所以 _text/_md 失败时必须还有这条路能说话。文本过长自动转 markdown。"""
    body = clean(text)
    if len(body) > 1200:
        openapi_post(msg, "sampleMarkdown", json.dumps({"title": title, "text": body}))
    else:
        openapi_post(msg, "sampleText", json.dumps({"content": body}))
    print("[reply] 已通过 OpenAPI 兜底发送文本(%d 字符)" % len(body), flush=True)


def openapi_send(msg, path):
    """通道②：新版 OpenAPI 主动发送文件/图片"""
    mid = upload_media(path)
    is_img = path.lower().endswith(IMG_EXT)
    name = os.path.basename(path)

    if is_img:
        msg_key = "sampleImageMsg"
        msg_param = json.dumps({"photoURL": mid})     # 实测 photoURL 可直接吃 media_id
    else:
        msg_key = "sampleFile"
        msg_param = json.dumps({"mediaId": mid, "fileName": name,
                                "fileType": name.rsplit(".", 1)[-1].lower() if "." in name else "bin"})

    via = openapi_post(msg, msg_key, msg_param)
    return "%s/%s" % (via, msg_key)


# ------------------------- 统一投递出口 -------------------------

def publish_to_web(path):
    """最后兜底：拷到网站目录换公网 URL。没配置 PUBLISH_DIR 时直接返回 None（不碰 nginx）"""
    if not (PUBLISH_DIR and PUBLIC_BASE_URL):
        return None
    os.makedirs(PUBLISH_DIR, exist_ok=True)
    name = "img_%d_%s" % (int(time.time()), os.path.basename(path))
    dst = os.path.join(PUBLISH_DIR, name)
    shutil.copy(path, dst)
    os.chmod(dst, 0o644)
    return PUBLIC_BASE_URL.rstrip("/") + "/" + name


def deliver(sw, msg, path):
    """★ 发图/发文件的唯一出口。图片按「显示效果」降级（前两级都能在钉钉里直接显示图片），
    文件按「可达性」降级。返回 (ok:bool, detail:str)，失败时 detail 含每级的失败原因"""
    errs = []
    is_img = path.lower().endswith(IMG_EXT)

    if is_img:
        chain = [
            ("image/base64内联(直接显示)", lambda: send_b64(sw, path)),
            ("image/openapi(直接显示)",    lambda: openapi_send(msg, path)),
            ("file/文件卡片",              lambda: send_wh_file(sw, path)),
        ]
    else:
        chain = [
            ("file/webhook", lambda: send_wh_file(sw, path)),
            ("file/openapi", lambda: openapi_send(msg, path)),
        ]

    for name, fn in chain:
        try:
            fn()
            return True, name
        except Exception as e:
            errs.append("%s: %s" % (name, e))
            print("[deliver] %s 失败 %s -> %s" % (name, path, e), flush=True)

    # 全部失败：图片试 markdown 公网链接（仅在配置了 PUBLISH_DIR 时有效）
    if is_img:
        url = publish_to_web(path)
        if url and sw:
            try:
                post_session(sw, {"msgtype": "markdown", "markdown":
                    {"title": os.path.basename(path), "text": "![](%s)" % url}})
                return True, "markdown/公网链接"
            except Exception as e:
                errs.append("markdown: %s" % e)
    return False, " | ".join(errs)[:600]


# ------------------------- 图片：下载 / 探测 / 视觉 -------------------------

def download_image(code):
    """用 downloadCode 换下载 URL 再取字节。
    ★ 注意这里用的是【新版 accessToken】（api.dingtalk.com），不能用老版 token"""
    r = requests.post("https://api.dingtalk.com/v1.0/robot/messageFiles/download",
                      headers={"x-acs-dingtalk-access-token": get_token(),
                               "Content-Type": "application/json"},
                      json={"downloadCode": code, "robotCode": CLIENT_ID},
                      timeout=25).json()
    url = r.get("downloadUrl")
    if not url:
        raise RuntimeError("downloadCode 换 URL 失败: %s" % str(r)[:250])
    return requests.get(url, timeout=40).content


def image_probe(path):
    """纯 stdlib 读文件头拿 (宽,高)，给 agent 一点上下文；读不出返回 (0,0)"""
    try:
        with open(path, "rb") as f:
            h = f.read(64)
        if h[:8] == b"\x89PNG\r\n\x1a\n" and len(h) >= 24:
            return struct.unpack(">II", h[16:24])
        if h[:3] == b"GIF":
            return struct.unpack("<HH", h[6:10])
        if h[:2] == b"BM" and len(h) >= 26:
            return struct.unpack("<ii", h[18:26])
        if h[:4] == b"RIFF" and h[8:12] == b"WEBP":
            return (0, 0)
        if h[:2] == b"\xff\xd8":
            with open(path, "rb") as f:
                data = f.read(65536)
            i = 2
            while i < len(data) - 9:
                if data[i] != 0xFF:
                    i += 1
                    continue
                m = data[i + 1]
                if m in (0xC0, 0xC1, 0xC2):
                    return struct.unpack(">HH", data[i + 5:i + 9])
                if m in (0xD8, 0xD9) or 0xD0 <= m <= 0xD7:
                    i += 2
                    continue
                i += 2 + struct.unpack(">H", data[i + 2:i + 4])[0]
    except Exception:
        pass
    return (0, 0)


def vision_describe(path):
    """走 OpenAI 兼容视觉端点描述图片（模型必须支持视觉）"""
    with open(path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode()
    ext = os.path.splitext(path)[1].lower().lstrip(".") or "jpeg"
    mime = {"jpg": "jpeg"}.get(ext, ext)
    body = {"model": VISION_MODEL,
            "messages": [{"role": "user", "content": [
                {"type": "text", "text": VISION_PROMPT},
                {"type": "image_url", "image_url": {"url": "data:image/%s;base64,%s" % (mime, b64)}}]}],
            "max_tokens": 800}
    r = requests.post(VISION_URI.rstrip("/") + "/chat/completions",
                      headers={"Authorization": "Bearer " + VISION_KEY,
                               "Content-Type": "application/json"},
                      json=body, timeout=TIMEOUT_VISION).json()
    if "choices" in r:
        return (r["choices"][0]["message"]["content"] or "").strip()
    raise RuntimeError("视觉接口返回异常: %s" % str(r)[:250])


# ------------------------- 原始消息解析 -------------------------

def _content(raw):
    """raw['content'] 有时是 JSON 字符串，统一成 dict"""
    c = raw.get("content")
    if isinstance(c, str):
        try:
            return json.loads(c)
        except Exception:
            return None
    return c if isinstance(c, dict) else None


def deep_find(obj, keyword, depth=0):
    """模糊找字段名「包含」keyword 的字符串值（兜底用）"""
    if depth > 8:
        return None
    if isinstance(obj, dict):
        for k, v in obj.items():
            if keyword in str(k).lower().replace("_", "") and isinstance(v, str) and v:
                return v
            r = deep_find(v, keyword, depth + 1)
            if r:
                return r
    elif isinstance(obj, list):
        for v in obj:
            r = deep_find(v, keyword, depth + 1)
            if r:
                return r
    return None


def find_key_exact(obj, name, depth=0):
    """★ 精确匹配字段名（忽略大小写/下划线）。
    钉钉图片消息里同时有 pictureDownloadCode 和 downloadCode 两个码，
    下载接口只认 downloadCode —— 用包含匹配会误取 pictureDownloadCode 导致 unknownError"""
    if depth > 8:
        return None
    target = name.lower().replace("_", "")
    if isinstance(obj, dict):
        for k, v in obj.items():
            if str(k).lower().replace("_", "") == target and isinstance(v, str) and v:
                return v
        for v in obj.values():
            r = find_key_exact(v, name, depth + 1)
            if r:
                return r
    elif isinstance(obj, list):
        for v in obj:
            r = find_key_exact(v, name, depth + 1)
            if r:
                return r
    return None


def rich_text_content(raw):
    """富文本（图文混发）时 msg.text 为空，需要从 richText 片段里拼出文字"""
    c = _content(raw)
    if not c:
        return ""
    parts = []
    for item in (c.get("richText") or []):
        if isinstance(item, dict) and item.get("text"):
            parts.append(item["text"])
    return "".join(parts).strip()


def is_image_msg(raw):
    """判断是不是图片消息：优先看 msgtype / richText 结构，退化才用全文本匹配"""
    c = _content(raw)
    if c:
        mt = str(c.get("msgtype") or "").lower()
        if mt in ("picture", "image"):
            return True
        for item in (c.get("richText") or []):
            if isinstance(item, dict) and ("pictureDownloadCode" in item or "downloadCode" in item):
                return True
    return "downloadcode" in json.dumps(raw, ensure_ascii=False).lower()


def extract_image_code(raw):
    """取 downloadCode：先精确 key，再模糊兜底"""
    c = _content(raw)
    for host in (c, raw):
        v = find_key_exact(host, "downloadCode")
        if v:
            return v
    return deep_find({"raw": raw, "content": c}, "downloadcode")


# ------------------------- 产物图扫描（A 版资产） -------------------------

IMG_IN_TEXT = re.compile(r"(/[\w./\-]+\.(?:png|jpe?g|gif|webp|bmp))", re.I)


def _new_images_since(since_ts, limit=3):
    """在常用产出目录里找 since_ts 之后新建/修改的图片"""
    hits = []
    for base in SCAN_DIRS:
        if not base or not os.path.isdir(base):
            continue
        for root, dirs, files in os.walk(base):
            if root[len(base):].count(os.sep) > SCAN_MAX_DEPTH:
                dirs[:] = []
                continue
            dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
            for f in files:
                if not f.lower().endswith(IMG_EXT):
                    continue
                p = os.path.join(root, f)
                try:
                    st = os.stat(p)
                except Exception:
                    continue
                if st.st_mtime >= since_ts and st.st_size > 0:
                    hits.append((st.st_mtime, p))
    hits.sort(reverse=True)
    return [p for _, p in hits[:limit]]


def pick_images(out_text, since_ts, limit=3):
    """优先从 agent 输出里抠图片路径；抠不到再扫目录"""
    found, seen = [], set()
    for raw_path in IMG_IN_TEXT.findall(out_text or ""):
        p = raw_path.strip("。，,;；、)）]】\"'")
        low = p.lower()
        if any(x in low for x in ("/icon", "/favicon", "/logo", "/assets/emoji", ".min.")):
            continue
        if os.path.isfile(p) and p not in seen:
            seen.add(p)
            found.append(p)
    if not found:
        found = _new_images_since(since_ts, limit)
    return [p for p in found if p not in _sent_images][:limit]


AGENT_FILE_PAT = re.compile(r"^\[FILE\]\s*(\S+)", re.M)
CANT_SEE = re.compile(r"(无法(查看|读取|访问|显示|打开)|看不了|看不到|不能读|看不到图片|"
                      r"无法直接(查看|访问)|can'?t (view|see|read)|cannot (view|see|read))", re.I)


try:                                     # Python 3.9+
    _to_thread = asyncio.to_thread
except AttributeError:                   # 3.8 回退到线程池 executor，语义一致
    def _to_thread(fn, *args, **kwargs):
        return asyncio.get_event_loop().run_in_executor(
            None, functools.partial(fn, *args, **kwargs))


# ------------------------- 主处理器 -------------------------

class Bot(dingtalk_stream.ChatbotHandler):

    _busy = 0          # 正在执行的长任务数（类变量，所有 handler 共享）

    # ---- 回复：SDK 优先，失败降级 OpenAPI ----
    # 为什么要降级：sessionWebhook 有时效。任务跑几十分钟后回来才发现发不出去，
    # 用户视角就是"机器人彻底没反应了"，所以必须有第二条路。
    def _text(self, msg, s):
        try:
            self.reply_text(clean(s), msg)
            return
        except Exception as e:
            print("[reply] SDK text 失败(%s)，降级 OpenAPI" % e, flush=True)
        try:
            openapi_send_text(msg, s)
        except Exception as e:
            print("[reply] OpenAPI text 也失败: %s" % e, flush=True)

    def _md(self, msg, title, s):
        try:
            self.reply_markdown(title, clean(s), msg)
            return
        except Exception as e:
            print("[reply] SDK markdown 失败(%s)，降级 OpenAPI" % e, flush=True)
        try:
            openapi_send_text(msg, s, title)
        except Exception as e:
            print("[reply] OpenAPI markdown 也失败: %s" % e, flush=True)

    # ---- 并发编排：占位 → 丢线程池 → 释放 ----
    @classmethod
    def _reserve(cls):
        """在处理消息时同步占位（早于建后台任务），保证计数即时准确，不会出现
        「同时放行 10 个长任务」的时间窗。满了就直接告诉用户，别让线程堆积。"""
        if cls._busy >= AGENT_MAX_CONCURRENT:
            return False
        cls._busy += 1
        print("[busy] +1 -> %d/%d" % (cls._busy, AGENT_MAX_CONCURRENT), flush=True)
        return True

    async def _spawn(self, msg, fn, *args, **kwargs):
        """后台执行阻塞任务：包异常 + 收尾释放计数 + 兜底报错，保证 _busy 不会泄漏"""
        try:
            return await _to_thread(fn, *args, **kwargs)
        except Exception:
            import traceback
            traceback.print_exc()
            try:
                self._text(msg, "内部错误，详情见服务器日志。")
            except Exception:
                pass
        finally:
            Bot._busy -= 1
            print("[busy] -1 -> %d/%d" % (Bot._busy, AGENT_MAX_CONCURRENT), flush=True)

    def _ticker(self, msg, what):
        """长任务进度回调：到点发一条「还在跑」，避免用户以为机器人挂了"""
        if not PROGRESS_HINT:
            return None

        def cb(elapsed):
            # 耗时按「分:秒」显示，避免长任务里一串 4 位数秒看不出跑了多久
            self._text(msg, "⏳ %s 还在跑（已 %d 分 %02d 秒），完成后会自动回复。"
                       % (what, elapsed // 60, elapsed % 60))

        return cb

    def _resolve(self, p):
        """相对路径依次尝试 家目录 / zeroclaw 工作区"""
        if os.path.isabs(p):
            return p if os.path.isfile(p) else None
        for base in (HOME, WS_DIR):
            c = os.path.join(base, p)
            if os.path.isfile(c):
                return c
        return None

    # ---- 发文件/图片的对外方法 ----
    def send_path(self, msg, sw, path, label=None):
        ok, detail = deliver(sw, msg, path)
        if ok:
            print("[send] OK %s via %s" % (path, detail), flush=True)
            self._text(msg, "%s 已发送（通道：%s）" % (label or os.path.basename(path), detail))
        else:
            print("[send] FAIL %s : %s" % (path, detail), flush=True)
            self._text(msg, "发送失败：%s\n文件仍在服务器：%s" % (detail, path))
        return ok

    def _maybe_send_images(self, msg, sw, out_text, since_ts):
        """agent 跑完后，把它这次新产生的图片推到钉钉"""
        if not AUTO_SEND_IMAGE or not sw:
            return 0
        n = 0
        for path in pick_images(out_text, since_ts):
            _sent_images.add(path)
            ok, detail = deliver(sw, msg, path)
            print("[imgout] %s %s via %s" % ("OK" if ok else "FAIL", path, detail), flush=True)
            if ok:
                n += 1
            else:
                self._text(msg, "图片已生成但发不出（%s），服务器路径：%s" % (detail, path))
        return n

    def _send_attached(self, msg, sw, out):
        """处理 [FILE] 约定：agent 在回复末尾写 [FILE] 路径 → 自动作为附件投递"""
        paths = AGENT_FILE_PAT.findall(out or "")
        if not paths and re.search(r"已生成|已保存|保存在|已复制到工作区", out or ""):
            for cand in re.findall(r"([\w./\-]{2,160}\.(?:png|jpg|jpeg|gif|webp|bmp|zip|pdf|mp4|bin))",
                                   out, re.I):
                full = self._resolve(cand.strip(".").lstrip("/"))
                if full:
                    paths.append(full)
        n = 0
        for p in paths:
            full = self._resolve(p)
            if not full:
                continue
            if deliver(sw, msg, full)[0]:
                n += 1
                print("[file] auto-sent %s" % full, flush=True)
        return n

    # ---- 图片消息：下载 → 识别 → agent ----
    def _handle_image(self, msg, sw, raw, text):
        code = extract_image_code(raw)
        if not code:
            self._text(msg, "收到图片但没解析出 downloadCode（钉钉字段格式可能有变），原文已打到日志。")
            return

        os.makedirs(MEDIA_DIR, exist_ok=True)
        path = os.path.join(MEDIA_DIR, "img_%d.jpg" % int(time.time()))
        try:
            data = download_image(code)
            with open(path, "wb") as f:
                f.write(data)
            print("[img] 保存 %s (%d 字节)" % (path, len(data)), flush=True)
        except Exception as e:
            print("[img] 下载失败: %s" % e, flush=True)
            self._text(msg, "图片下载失败：%s" % e)
            return

        w, h = image_probe(path)
        size_kb = os.path.getsize(path) / 1024.0

        desc = ""
        if VISION_ON:
            try:
                desc = vision_describe(path)
                print("[vision] %s" % desc[:300], flush=True)
            except Exception as e:
                desc = "（视觉识别失败：%s）" % e

        ask = (text or "").strip(" @/").replace("zeroclaw", "").strip()
        task = (
            "用户刚在钉钉里发了一张图片，已保存到服务器：%s（%dx%d，%.0fKB）\n"
            "请先用 image_info 工具读取并理解这张图片内容，然后回应用户；"
            "如果图里有报错、配置或数据，重点分析并给出具体可执行的建议。\n"
            "%s\n用户随图文字：%s"
            % (path, w, h, size_kb,
               ("视觉模型已给出的图片描述：%s" % desc) if desc else "",
               ask or "（无）")
        )
        t0 = time.time() - 2
        out = run("%s agent -a %s -m %s" % (ZEROCLAW, AGENT, shlex.quote(task)), TIMEOUT_AGENT,
                  on_tick=self._ticker(msg, "图片分析"),
                  tick_after=PROGRESS_AFTER, tick_every=PROGRESS_EVERY, tick_max=PROGRESS_MAX)
        self._md(msg, "ZeroClaw", out)

        # agent 明确表示读不了图 → 原图回传，至少让人眼能看到
        if IMAGE_FALLBACK and (not desc) and CANT_SEE.search(out or ""):
            self._text(msg, "agent 表示无法直接读取该图片，原图回传如下：")
            self.send_path(msg, sw, path, os.path.basename(path))
        self._send_attached(msg, sw, out)
        self._maybe_send_images(msg, sw, out, t0)

    _tasks = set()     # 持有后台任务引用，防止被 GC 中途回收

    def _dispatch(self, msg, fn, *args):
        """★ 任务放行口：占位 → 建后台任务 → 立刻返回。
        关键在「立刻返回」：v3.1 里 process 会一直等到命令执行完才 ACK，
        期间 SDK 的 event loop 被阻塞 → 心跳发不出去 → 钉钉判定机器人掉线、
        重投消息，表现就是「卡死 / 重复执行」。现在先 ACK 再干活，
        处理多久都不影响连接。"""
        if not self._reserve():
            self._text(msg, "当前有 %d 个任务在执行（上限 %d），稍等几分钟再发。"
                            % (Bot._busy, AGENT_MAX_CONCURRENT))
            return
        t = asyncio.create_task(self._spawn(msg, fn, *args))
        Bot._tasks.add(t)
        t.add_done_callback(Bot._tasks.discard)

    # ---- 各分支的实际执行体（都在线程池里跑） ----
    def _do_send(self, msg, sw, target):
        """② #file / !file / !pic / !img：把文件 / 图片发回钉钉"""
        target = (target or "").strip().strip('"').strip("'")
        full = self._resolve(target)
        if not full:
            self._text(msg, "文件不存在：%s（绝对路径，或相对 %s / %s）" % (target, HOME, WS_DIR))
            return
        self.send_path(msg, sw, full, os.path.basename(full))

    def _do_vision(self, msg, arg):
        """③ !vision：不经 agent，直接用视觉模型描述图片（排查用）"""
        p = self._resolve((arg or "").strip().strip('"').strip("'"))
        if not p:
            self._text(msg, "文件不存在：%s" % (arg or "").strip())
            return
        if not VISION_ON:
            self._text(msg, "VISION_ON=False，未启用视觉模型。如需开启：把配置区 VISION_ON 设为 True，"
                            "并填好 VISION_URI / VISION_KEY / VISION_MODEL 三个占位符（模型须支持视觉）。")
            return
        try:
            self._text(msg, vision_describe(p))
        except Exception as e:
            self._text(msg, "视觉识别失败：%s" % e)

    def _do_shell(self, msg, sw, cmd):
        """④ !开头：shell 直通"""
        out = run(cmd, TIMEOUT_SHELL,
                  on_tick=self._ticker(msg, "命令"),
                  tick_after=PROGRESS_AFTER, tick_every=PROGRESS_EVERY, tick_max=PROGRESS_MAX)
        self._md(msg, "Shell", out)
        # shell 输出里若含图片路径，顺手回传（如 !ls 之后想看图）
        self._maybe_send_images(msg, sw, out, time.time() - 5)

    def _do_agent(self, msg, sw, text):
        """⑤ 其余中文：交给 zeroclaw agent 理解执行"""
        t0 = time.time() - 2
        prompt = (text + "\n\n（系统约定：如果你生成或需要把文件/图片交给用户，"
                  "请在回复最后另起一行输出 [FILE] 文件完整路径，例如 [FILE] /root/a.png；"
                  "纯文字回答请忽略本约定。）")
        out = run("%s agent -a %s -m %s" % (ZEROCLAW, AGENT, shlex.quote(prompt)), TIMEOUT_AGENT,
                  on_tick=self._ticker(msg, "任务"),
                  tick_after=PROGRESS_AFTER, tick_every=PROGRESS_EVERY, tick_max=PROGRESS_MAX)
        sent = self._send_attached(msg, sw, out)
        if sent:
            out = AGENT_FILE_PAT.sub("", out).strip()
            out += "\n\n—— 以上 %d 个文件已作为附件发送" % sent
        self._md(msg, "ZeroClaw", out)
        self._maybe_send_images(msg, sw, out, t0)

    async def process(self, callback):
        raw = callback.data or {}
        print("[raw]", json.dumps(raw, ensure_ascii=False)[:1200], flush=True)
        try:
            msg = dingtalk_stream.ChatbotMessage.from_dict(raw)
            sw = getattr(msg, "session_webhook", "") or ""

            text = ""
            if getattr(msg, "text", None):
                text = (msg.text.content or "").strip()
            if not text:
                text = rich_text_content(raw)
            if text.startswith("@") and " " in text:      # 群聊去掉 @机器人 前缀
                text = text.split(" ", 1)[1].strip()
            print("[msg]", text or "(图片/非文本消息)", flush=True)

            # ① 图片消息：下载 → 识别 → agent
            if is_image_msg(raw):
                self._dispatch(msg, self._handle_image, msg, sw, raw, text)
                return AckMessage.STATUS_OK, "OK"
            if not text:
                return AckMessage.STATUS_OK, "OK"

            # ② 发文件 / 发图片（#file / !file / !pic / !img 等价）
            m = re.match(r"^(#file|!file|!pic|!img)\s+(.+)$", text, re.I)
            if m:
                self._dispatch(msg, self._do_send, msg, sw, m.group(2))
                return AckMessage.STATUS_OK, "OK"

            # ③ 视觉直读（排查用，不走 agent）
            if text.startswith("!vision "):
                self._dispatch(msg, self._do_vision, msg, text[8:])
                return AckMessage.STATUS_OK, "OK"

            # ④ shell 直通
            if text.startswith("!"):
                self._dispatch(msg, self._do_shell, msg, sw, text[1:].strip())
                return AckMessage.STATUS_OK, "OK"

            # ⑤ 其余交给 zeroclaw agent
            self._dispatch(msg, self._do_agent, msg, sw, text)
            return AckMessage.STATUS_OK, "OK"

        except Exception:
            import traceback
            traceback.print_exc()
        return AckMessage.STATUS_OK, "OK"


def main():
    client = dingtalk_stream.DingTalkStreamClient(
        dingtalk_stream.Credential(CLIENT_ID, CLIENT_SECRET))
    client.register_callback_handler(dingtalk_stream.ChatbotMessage.TOPIC, Bot())
    print("启动钉钉机器人（合并通用版 v3.2：双通 + 长任务不中断）...", flush=True)
    client.start_forever()


if __name__ == "__main__":
    main()

# ==========================================================================
#  完整使用手册 + 技术原理与维护说明
#  版本：合并通用版 v3.2（识别 + 发送 双通 / 长任务不中断）
#  本块即全部文档：使用方法 + 实现原理 + 排错手册都在这里，无需另找 md。
#  改代码前请至少读 [3] 发送降级链、[10] 已知坑位 两节。
# ==========================================================================
#
# [0] 使用方法
#
#   0.1 前置条件
#     · 服务器 Linux，能出网访问 api.dingtalk.com / oapi.dingtalk.com
#     · Python 3.8+
#     · zeroclaw 已安装，且 `zeroclaw agent -a <别名> -m "测试"` 能正常返回
#     · 钉钉开放平台已创建「企业内部机器人」，消息接收模式选 Stream 模式
#     · 可选：想用 OpenAPI 通道发文件，需开通「企业内机器人发送消息」并发布新版本
#
#   0.2 占位符清单（脚本顶部配置区）：必填 5 个 + 可选 3 个
#
#     【必填】不替换跑不起来
#     ┌────────────────────┬──────────────────────────────────┬───────────────────────┐
#     │ 占位符             │ 去哪里找                         │ 示例                  │
#     ├────────────────────┼──────────────────────────────────┼───────────────────────┤
#     │ <CLIENT_ID>        │ 开放平台→应用→凭证与基础信息      │ dingxxxxxxxxxxxx      │
#     │ <CLIENT_SECRET>    │ 同页面 Client Secret（AppSecret）│ 一长串随机字符         │
#     │ <ZEROCLAW_PATH>    │ 服务器上 which zeroclaw          │ /root/.cargo/bin/...  │
#     │ <AGENT_NAME>       │ zeroclaw agent --list 里的别名   │ ops                   │
#     │ <HOME_DIR>         │ 运行 bot 的系统用户家目录         │ /root                 │
#     └────────────────────┴──────────────────────────────────┴───────────────────────┘
#
#     【可选】仅当 VISION_ON=True 时才需要（agent 读不了图、要接视觉模型兜底）
#     ┌────────────────────┬──────────────────────────────────┬───────────────────────┐
#     │ <VISION_URI>       │ OpenAI 兼容视觉端点地址           │ http://host:port/v1   │
#     │ <VISION_KEY>       │ 该端点的 key（无鉴权填空串）      │ sk-xxxxxx             │
#     │ <VISION_MODEL>     │ 支持视觉的模型名                 │ gpt-4o                │
#     └────────────────────┴──────────────────────────────────┴───────────────────────┘
#
#     注意 1：ZEROCLAW 必须写绝对路径 —— systemd 的 PATH 不含 cargo 目录，
#             写相对路径会 command not found。
#     注意 2：VISION_ON 默认 False。zeroclaw agent 自带 image_info 能直接读本地图片，
#             所以绝大多数情况不需要视觉端点，那 3 个可选占位符留着不填也不影响运行。
#
#   0.3 部署步骤
#
#     # 1) 装依赖（websockets 必须 <14，高版本会静默握手失败）
#     pip3 install --break-system-packages dingtalk-stream "websockets<14" requests
#
#     # 2) 建目录
#     mkdir -p /opt/dingtalk-bot/media
#
#     # 3) 放脚本（先替换 5 个占位符）
#     vi /opt/dingtalk-bot/bot.py
#
#     # 4) 备份旧版（覆盖前）
#     cp /opt/dingtalk-bot/bot.py /opt/dingtalk-bot/bot.py.bak.$(date +%F)
#
#   0.4 systemd 单元（/etc/systemd/system/dingbot.service）
#
#     [Unit]
#     Description=DingTalk <-> zeroclaw bot
#     After=network.target
#
#     [Service]
#     Type=simple
#     WorkingDirectory=/opt/dingtalk-bot
#     ExecStart=/usr/bin/python3 /opt/dingtalk-bot/bot.py
#     Restart=always
#     RestartSec=5
#
#     [Install]
#     WantedBy=multi-user.target
#
#     systemctl daemon-reload
#     systemctl enable --now dingbot
#     journalctl -u dingbot -f
#
#   0.5 自检（钉钉里依次发这 4 条）
#
#     !date                 -> 返回时间，说明 shell 通道通
#     #file /etc/hostname   -> 收到文件卡片，说明发送通道通
#     报一下当前负载         -> agent 回中文报告，说明 agent 通道通
#     一张图片               -> agent 描述图片内容，说明识别通道通
#
#   0.6 指令一览
#
#     中文句子            -> 交给 zeroclaw agent 理解执行（支持 [FILE] 自动回传文件）
#     !命令               -> 直接跑 shell（如 !df -h），120s 超时
#     #file <路径>        -> 把服务器上任意文件/图片发回钉钉（别名 !file / !pic / !img）
#     !vision <图片路径>  -> 不经 agent，直接用视觉模型描述这张图（排查用）
#     钉钉发图片          -> 下载 → 识别 → agent 分析（识别不了会原图回传）
#
#     路径可以是绝对路径，也可以相对 <HOME_DIR> 或 zeroclaw 工作区。
#
#   0.7 配置项速查
#
#     VISION_ON        False   视觉兜底开关；False=用 agent 自带 image_info，无需视觉 API
#     VISION_URI       -       可选。OpenAI 兼容端点（VISION_ON=True 时才用到）
#     VISION_KEY       -       可选。该端点的 key
#     VISION_MODEL     -       可选。必须支持视觉的模型名
#     IMAGE_FALLBACK   True    agent 读不了图时把原图回传给用户
#     AUTO_SEND_IMAGE  True    agent 产物图自动推送
#     MAX_IMAGE_MB     10      自动推送的单图上限
#     MAX_UPLOAD_MB    20      钉钉媒体上传硬上限
#     SCAN_DIRS        -       产物图扫描目录
#     SCAN_MAX_DEPTH   3       扫描深度，防止遍历整个磁盘
#     PUBLISH_DIR      ""      空 = 不碰 nginx，不影响其它发送通道
#     TIMEOUT_SHELL    120     !命令 上限（秒）
#     TIMEOUT_AGENT    3600    agent 上限（秒）。★上限≠期待值，没跑完不杀
#     TIMEOUT_VISION   120     视觉识别超时（秒）
#     PROGRESS_HINT    True    长任务是否发「还在跑」进度提示
#     PROGRESS_AFTER   120     多少秒后发第一条进度提示（默认 2 分钟）
#     PROGRESS_EVERY   180     之后每隔多少秒再发一条（默认 3 分钟）
#     PROGRESS_MAX     10      单任务最多发几条进度提示
#     AGENT_MAX_CONCURRENT 3   同时执行的长任务上限，超出直接提示用户稍等
#     MAX_OUT          3000    文本结果最大字符数
#
#   ★ 用 Genesys / 任何多轮 AI agent 时，还要同步看 zeroclaw 自己的
#     provider timeout_secs（见 [10] P14）—— 那道墙比本文件的 TIMEOUT 更早生效。
#
# --------------------------------------------------------------------------
#
# [1] 架构与数据流
#
#   钉钉云端  <──── Stream 出站 WebSocket ────>  bot.py  <── subprocess ──>  zeroclaw agent
#               收：ChatbotMessage.TOPIC               单轮：agent -a <AGENT> -m "<prompt>"
#               发：sessionWebhook / OpenAPI
#
#   时序（v3.2，注意第 3 步已经先 ACK）：
#     1) DingTalkStreamClient(Credential(...)) 主动外连网关，
#        register_callback_handler(ChatbotMessage.TOPIC, Bot())
#     2) 用户消息以 callback.data 推入 Bot.process()
#     3) process 解析出文本 / 图片 downloadCode，按前缀路由后：
#           _reserve() 占位 → asyncio.create_task(...) → 立刻 return ACK
#        ★ 这一步必须快：阻塞超过 SDK 心跳周期，钉钉会认为机器人掉线并重投消息
#     4) 后台线程池里 fork 子进程跑 `zeroclaw agent -a <AGENT> -m "<prompt>"`，
#        期间按 PROGRESS_* 节奏给用户发「还在跑」提示
#     5) 结果文本回钉钉（SDK 失败则降级 OpenAPI）；若含附件则走 deliver() 投递（见 [3]）
#
#   三层并发模型（改动前请确认自己动的是哪一层）：
#     asyncio event loop   ← SDK 心跳 / 收消息，绝不能被阻塞
#         └─ 线程池 executor  ← 所有 subprocess / HTTP 调用（_to_thread）
#                └─ 子进程      ← zeroclaw agent 本体，独立进程组，可整组 kill
#
#   关键点：Stream 模式是【出站连接】，不需要公网回调地址 / 域名 / HTTPS 证书，
#   与服务器安全组的入站规则无关 —— 只要服务器能出网访问钉钉即可。
#
# [2] 两套 API、两种 token（最易踩的坑，绝不可混用）
#
#   ┌────────────┬──────────────────────────────────┬────────────────────────────┐
#   │            │ 新版 api.dingtalk.com            │ 老版 oapi.dingtalk.com     │
#   ├────────────┼──────────────────────────────────┼────────────────────────────┤
#   │ 取 token   │ POST /v1.0/oauth2/accessToken    │ GET /gettoken              │
#   │            │ body {appKey, appSecret}         │ ?appkey=&appsecret=        │
#   │ 响应字段   │ accessToken                      │ access_token / errcode     │
#   │ 携带方式   │ 头 x-acs-dingtalk-access-token   │ 参数 ?access_token=        │
#   │ 本脚本用途 │ messageFiles/download            │ /media/upload              │
#   │            │ groupMessages/send               │                            │
#   │            │ oToMessages/batchSend            │                            │
#   │ 缓存变量   │ _new_tok（7000s）                │ _old_tok（7000s）          │
#   └────────────┴──────────────────────────────────┴────────────────────────────┘
#
#   实测结论：/media/upload 用新版 token 不好使，必须用老版 /gettoken。
#   历史 bug：旧版拿老版 token 去请求新版下载接口，偶发 code=unknownError。
#
# [3] 发送降级链（deliver() 内固定；图片按「显示效果」优先）
#
#   图片：
#     ① send_b64        webhook  {"msgtype":"image","image":{"base64","md5"}}
#                       → 钉钉里直接显示图片。实测可靠路径，★勿加人为大小门槛
#     ② openapi_send    OpenAPI  sampleImageMsg {"photoURL": media_id}
#                       → 也能直接显示；需应用开通「企业内机器人发送消息」权限
#     ③ send_wh_file    webhook  {"msgtype":"file","file":{"mediaId","fileName","fileType"}}
#                       → 只能下载，末级兜底
#     ④ publish_to_web  markdown ![](公网URL)   仅配置了 PUBLISH_DIR 才有
#   文件：
#     ① send_wh_file（webhook file 卡片） → ② openapi_send（sampleFile） → 文字报错
#
#   ★ v3.1 教训：v3.0 给 ① 加了 2MB 软上限，导致实测图片被拦、直接退化成 ③ 文件卡片
#     （用户看到的是"文件"而不是图片）。已经由用户实测确认 ① 可直显成功。
#     结论：不要给实测可用的直显路径加人为限制，失败交给降级链处理。
#   ★ 图片不要走 {"image":{"mediaId":..}} —— 必报 errcode=400802 miss param: image->picURL
#   ★ sessionWebhook 业务失败也返回 HTTP 200，必须解析 errcode（见 [4]）
#
# [4] 发送 API 契约
#
#   sessionWebhook（每条消息自带，只对当前会话有效，免应用权限）
#     文本     {"msgtype":"text","text":{"content":".."}}
#     markdown {"msgtype":"markdown","markdown":{"title":"..","text":".."}}
#     图片     {"msgtype":"image","image":{"base64":"..","md5":".."}}
#     文件     {"msgtype":"file","file":{"mediaId":"..","fileName":"..","fileType":".."}}
#     响应     {"errcode":0,"errmsg":"ok"} —— ★ 非 0 即为失败，即使 HTTP 是 200
#              （只看 HTTP 状态码会误判成功：历史 bug 是提示"图片已发送"但群里没有）
#
#   OpenAPI 主动发送（头 x-acs-dingtalk-access-token，需权限并发布版本）
#     群聊  POST /v1.0/robot/groupMessages/send
#           {robotCode, openConversationId, msgKey, msgParam}
#     单聊  POST /v1.0/robot/oToMessages/batchSend
#           {robotCode, userIds:[staffId], msgKey, msgParam}
#     ★ msgParam 必须是 JSON 字符串（json.dumps），传对象会报参数错误
#
#   msgKey / msgParam 对照
#     sampleText       {"content":".."}
#     sampleMarkdown   {"title":"..","text":".."}
#     sampleImageMsg   {"photoURL":".."}                      实测可直接填 media_id
#     sampleFile       {"mediaId":"..","fileName":"..","fileType":".."}
#
# [5] 消息接收：原始结构与解析
#
#   callback.data 常见形态：
#
#     // 纯文本
#     {"msgtype":"text","text":{"content":"hi"},
#      "sessionWebhook":"..","conversationType":"2","conversationId":"..","senderStaffId":".."}
#
#     // 富文本（图文混发）—— 注意同时存在两个 code
#     {"msgtype":"richText","content":{"richText":[
#         {"text":"看看这个"},
#         {"pictureDownloadCode":"..","downloadCode":".."}]}}
#
#   解析要点：
#     · 图文混发时 msg.text 为空 → 必须从 content.richText[].text 拼文字（rich_text_content）
#     · 下载图片只能用 downloadCode，用 pictureDownloadCode 会返回 unknownError
#       → 所以用 find_key_exact 精确匹配字段名；deep_find（包含匹配）只作兜底
#     · 群聊文本带 "@机器人 " 前缀 → 不剥离会导致指令匹配不上
#
# [6] 图片识别链路
#
#   downloadCode
#     → POST /v1.0/robot/messageFiles/download {downloadCode, robotCode}
#     → 取 downloadUrl → GET 下载字节 → 落盘 MEDIA_DIR/img_<ts>.jpg
#     → image_probe() 读文件头拿宽高（纯 stdlib，支持 PNG / GIF / BMP / JPEG）
#     → (VISION_ON) 视觉模型描述
#     → 拼 prompt：路径 + 尺寸 + 视觉描述 + 用户随图文字，要求 agent 用 image_info 工具
#     → zeroclaw agent 回复
#     → 若 agent 输出匹配「无法查看 / 看不了 / can't see」且无视觉描述
#        → IMAGE_FALLBACK：把原图回传给用户（至少让人眼能看到）
#
#   设计取舍：zeroclaw agent 自带 image_info 工具能直接读服务器本地图片，
#   所以默认 VISION_ON=False（不额外依赖视觉 API）。只有 agent 确实读不了图时才开，
#   且模型必须支持视觉。
#
# [7] 二进制护栏
#
#   subprocess 取 bytes 后判定：
#     · 魔数命中 PNG / ZIP(PK) / GIF / PDF / gzip / BMP / JPEG → 二进制
#     · 否则统计控制字符占比 > 5%，或 UTF-8 / GBK 都解不动 → 二进制
#     · 只抽样头部 64KB + 尾部 8KB，避免大文件全量扫描
#   命中后不回灌文本，改为提示 "#file <路径>" —— 否则图片字节会把钉钉刷屏。
#
# [8] [FILE] 自动投递约定
#
#   给 agent 的 prompt 末尾追加约定：要交文件时在回复最后另起一行写
#
#       [FILE] /绝对路径/文件.png
#
#   bot 用 AGENT_FILE_PAT 解析 → deliver() 投递 → 把该行从正文剔除 →
#   文末补一句「N 个文件已作为附件发送」。
#   兜底：输出含「已生成 / 已保存 / 保存在 / 已复制到工作区」时，正则扫路径尝试补发。
#
# [9] 指令路由（process 内顺序，勿随意调整）
#
#     process() 只做「解析 + 路由 + 放行」，不做执行；执行体在线程池里跑：
#
#     1  is_image_msg(raw)             → _handle_image   下载→识别→agent
#     2  #file / !file / !pic / !img   → _do_send        发文件/图片
#     3  !vision <路径>                → _do_vision      直调视觉模型（排查用）
#     4  !<任意>                       → _do_shell       shell 直通，上限 TIMEOUT_SHELL
#     5  其它中文                      → _do_agent       agent，上限 TIMEOUT_AGENT + [FILE] 约定
#     6  空文本                        → 直接 ACK，不回复
#
#   放行链路：_dispatch → _reserve(占位/限流) → create_task(_spawn)
#                       → _to_thread(fn)      → finally 释放 _busy
#   ★ 新增分支必须走 _dispatch，不要直接在 process 里 await 阻塞调用，
#     也不要忘记给新分支套 try —— 异常由 _spawn 统一兜住并记日志。
#
# [10] 已知坑位
#
#   P1  websockets 必须 <14；17.x 会让 WSS 握手静默挂死（不报错、连不上）
#   P2  日志 topic=ping 是心跳不是错误；接入成功看控制台「连接接入成功」
#   P3  zeroclaw 单轮必须带 -m；写成 agent -a ops "文本" 报 unexpected argument
#   P4  systemd 的 PATH 不含 cargo 目录 → RUN_ENV 显式注入，ZEROCLAW 必须绝对路径
#   P5  media_id 以 @ 开头，拼 URL 时不要 urlencode（会变 %40 导致裂图）
#   P6  sessionWebhook 失败也返回 HTTP 200 → 必须解析 errcode
#   P7  {"image":{"mediaId":..}} 必报 400802 miss param: image->picURL
#   P8  群聊 "@机器人 " 前缀不剥离 → 指令匹配不上
#   P9  图文混发 msg.text 为空 → 需从 richText 取字
#   P10 pictureDownloadCode 与 downloadCode 并存 → 只取后者，精确匹配
#   P11 旧版图片分支曾误用未定义变量 session_webhook（应为 sw），被外层 except
#       静默吞掉，表现为「什么都没发生」；本版统一为 sw
#   P12 不要给 base64 直显加人为大小门槛（v3.1 教训，见 [3]）
#   P13 OpenAPI 发送要先在开放平台开通权限【并发布新版本】，否则必然失败
#   P14 ★长任务的超时往往是「两道墙」：本脚本的 TIMEOUT_AGENT 之外，
#       zeroclaw 自己的 provider 还有 timeout_secs（常见默认 300~600），
#       它先到点的话，本脚本再怎么放宽也没用 —— 两边都要放大。
#       检查：~/.zeroclaw/config.toml 里 provider 的 timeout_secs / request_timeout。
#   P15 process() 里任何阻塞调用都会掐断 SDK 心跳 → 钉钉重投同一条消息，
#       表现为「指令被执行两次」「机器人反复说同一句话」。新代码一律走 _dispatch。
#   P16 subprocess 输出别用管道：缓冲区写满会反向阻塞子进程（输出多时必现），
#       且 timeout 后拿到的 stdout 经常是空的。本版统一重定向到临时文件。
#   P17 超时只 kill 顶层进程不够：shell=True 会 fork 出 zeroclaw，
#       留下孤儿会继续跑并烧 token。本版 start_new_session + os.killpg 整组杀。
#   P18 asyncio.to_thread 需要 Python 3.9+；3.8 会走 fallback(executor)，语义一致。
#       不要再引入裸的 time.sleep / 同步 requests 到 process 的协程路径里。
#   P19 sessionWebhook 有时效。任务跑几十分钟后回来再回复可能已失效，
#       所以 _text/_md 失败必须降级 OpenAPI —— 降级也失败才算真失败。
#
# [11] 日志标记速查
#
#   [raw]     原始消息（截断 1200 字符）
#   [msg]     解析后的指令文本
#   [img]     图片落盘结果
#   [vision]  视觉模型描述
#   [send]    #file 手动发送结果
#   [file]    [FILE] 自动投递
#   [imgout]  agent 产物图推送
#   [bin]     二进制输出被拦截
#   [deliver] 某一级发送失败原因（★ 排发送问题先看这个）
#   [openapi] OpenAPI 发送 HTTP 状态
#   [wh]      webhook 通道内部降级提示
#   [busy]    长任务并发占用 +1/-1（★ 卡住时看它是否只增不减）
#   [timeout] 任务跑满上限被中断（后面会跟被杀的命令）
#   [tick]    进度提示回调自身抛错（不影响主任务）
#   [reply]   回复通道降级：SDK 失败转 OpenAPI 时会打这一行
#
#     journalctl -u dingbot -n 100 --no-pager | grep -F "[deliver]"
#     journalctl -u dingbot -n 100 --no-pager | grep -F "[img]"
#     journalctl -u dingbot -n 100 --no-pager | grep -F "[busy]"
#     journalctl -u dingbot -f | grep -E "\[(timeout|tick|busy)\]"
#
# [12] 依赖与版本约束
#
#     pip3 install --break-system-packages dingtalk-stream "websockets<14" requests
#
#   websockets       <14      17.x 与 SDK 握手不兼容，静默挂死
#   dingtalk-stream  最新     官方 Stream SDK
#   requests         最新     全部 HTTP 调用
#
#   Python 3.9+（3.8 可用，asyncio.to_thread 有 executor fallback，见 P18）。
#   仅用标准库 asyncio / subprocess / tempfile 实现并发，无第三方程式依赖。
#
#   服务 /etc/systemd/system/dingbot.service，工作目录 /opt/dingtalk-bot，
#   媒体目录 /opt/dingtalk-bot/media（需可写）。
#   长任务务必留足重启余量：默认 RestartSec=5，改完记得 daemon-reload。
#
# [13] 扩展指南
#
#   加新指令    → process() 加分支 + 写一个 _do_xxx 执行体，一律经 _dispatch 放行
#                 （★ 不要在 process 里直接 await 阻塞调用，会掐断心跳，见 P15）
#   换视觉模型  → VISION_MODEL（必须支持视觉）、VISION_URI
#   加发送通道  → 写 send_xxx(sw, path)，追加到 deliver() 的 chain 列表
#   改扫描范围  → SCAN_DIRS / SCAN_MAX_DEPTH / SKIP_DIRS
#   关自动推送  → AUTO_SEND_IMAGE = False
#   调长任务    → TIMEOUT_AGENT（本侧上限）+ PROGRESS_*（进度提示节奏）
#                 + AGENT_MAX_CONCURRENT（并发）；别忘 zeroclaw 侧 timeout_secs（P14）
#
# [14] 排错决策树
#
#   发文件/图片失败
#     → grep [deliver]
#         三级全挂     → 看 base64 那一级的 errcode；再看 openapi 是否 403 / 权限未开
#         只有 openapi 挂 → 权限未开或没发布新版本
#   图片下载失败 unknownError
#     → grep [img] + [raw]，确认取到的是 downloadCode 而非 pictureDownloadCode
#   中文没反应
#     → grep [msg] 是否为空（@前缀未剥离 / 富文本走了 richText 分支）
#   agent 说看不了图
#     → 开 VISION_ON；或依赖 IMAGE_FALLBACK 让原图回传
#   连上就断 / 完全没日志
#     → 检查 websockets<14；topic=ping 是正常心跳
#   任务跑到一半被中断、或 300s 就结束
#     → grep [timeout]：本侧到点的话调大 TIMEOUT_AGENT；
#       没有 [timeout] 却提前结束 → 是 zeroclaw 侧 provider timeout_secs 到点了（P14）
#   同一条指令被执行两次
#     → process 里混进了阻塞调用导致 ACK 超时被网关重投（P15）；
#       顺手 grep [msg] 看是不是真的收到了两条重复投递
#   机器人彻底没反应 / 不回话
#     → grep [busy]：只增不减说明任务抛异常没走到 finally（查 traceback）
#       一路满到上限 → 有任务卡死，用 ps 看 zeroclaw 残留进程
#   长任务完成但用户收不到结果
#     → grep [reply]：出现「SDK ... 降级 OpenAPI」说明 webhook 已过期；
#       降级也失败 = OpenAPI 权限没开（P13），此时结果只能去服务器 /tmp 翻
#
# [15] 变更历史
#
#   v3.0  合并 A/B 两版：识别 + 发送双通；token 分家；[FILE] 约定；二进制护栏
#   v3.1  修复图片被发成「文件卡片」：删除 2MB 内联门槛，图片改为按显示效果降级
#         （base64 直显 → OpenAPI sampleImageMsg → webhook file 卡片 → markdown 链接）
#         用户实测确认：base64 内联可在钉钉里直接显示图片
#
#   v3.2  【长任务专项】解决「模型还在思考就被 300s 杀掉 / 像卡死 / 重复执行」
#         1) TIMEOUT_AGENT 300 → 3600。它是上限不是期待值，子进程还在跑就一直等；
#            真跑满时把已完成的部分输出回传，不让几百秒的活白干。
#         2) process() 不再阻塞等待：改为 _reserve 占位 → create_task → 立即 ACK。
#            这是「卡死 / 消息被重投」的真正根因（原实现占住 event loop，心跳发不出去）。
#            所有阻塞调用经 _to_thread 丢线程池（3.9+ 原生，3.8 有 executor fallback）。
#         3) run() 重写：输出重定向到临时文件而非管道
#            —— 管道写满会反向阻塞子进程，且超时后 stdout 常为空；落文件随时可读。
#         4) 超时后 start_new_session + os.killpg 按进程组杀，防 zeroclaw 变孤儿烧 token。
#         5) 新增进度提示：PROGRESS_AFTER 秒起，每 PROGRESS_EVERY 秒发一条「还在跑」
#            （默认 120s 首报 / 180s 一条，耗时按「分:秒」显示）。
#         6) 新增并发上限 AGENT_MAX_CONCURRENT，超出直接提示而不是让线程无限堆积。
#         7) 新增回复降级：_text/_md 的 SDK 回复失败时转 OpenAPI sampleText/Markdown
#            —— sessionWebhook 有时效，长任务跑完后很可能已失效。
#         8) 抽出 openapi_post 作为 OpenAPI 统一出口（原逻辑内联在 openapi_send 里）。
#         自测（本机 stub 掉网络依赖）15 项全过：正常输出 / 超时保底输出 /
#            1.6MB 大输出不死锁 / 二进制护栏 / 进度回调节奏 / process 0.00s 立即 ACK /
#            _busy 无泄漏。
# ==========================================================================
