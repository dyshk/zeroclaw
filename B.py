#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
钉钉 <-> zeroclaw 远程控制机器人（文本 + 图片双向支持）

原理：bot 主动外连钉钉 WebSocket(Stream 模式)，收消息 -> 处理 -> 结果回钉钉。

支持四类输入：
    中文            -> 交给 zeroclaw agent 理解执行
    !命令           -> 直接执行 shell（如 !df -h）
    钉钉发图片       -> 下载到本地 -> 调视觉模型识别 -> 连同描述交给 agent
    !pic <路径/URL> -> 把服务器上的图片（或公网图片）发回钉钉

部署：
    mkdir -p /opt/dingtalk-bot/media
    pip3 install --break-system-packages dingtalk-stream "websockets<14" requests
    systemctl restart dingbot
"""

import base64
import json
import os
import re
import shlex
import shutil
import subprocess
import time

import requests
import dingtalk_stream
from dingtalk_stream import AckMessage

# ============================ 配置区（改这里） ============================
CLIENT_ID = "dingx5jlxpvhpdci1jkr"
CLIENT_SECRET = "WFjiHGnAXm7CY1TUiIEVcZXR7MfLmhiCCH_ksLyF2wownMqE5NE51wv_2PfEfZ5k"

ZEROCLAW = "/root/.cargo/bin/zeroclaw"   # which zeroclaw 查；systemd 下必须绝对路径
AGENT = "ops"                            # zeroclaw agent 别名

# ---- 图片处理 ----
# 重要：zeroclaw agent 自带 image_info 工具，能直接读取服务器上的本地图片并理解内容，
# 所以默认不需要 bot 侧再调视觉 API（VISION_ON=False），把图片路径告诉 agent 即可。
# 如果你的 agent 看不了图，再把 VISION_ON 打开走视觉接口兜底（模型需支持视觉）。
VISION_ON = False
VISION_URI = "http://127.0.0.1:7863/v1"  # OpenAI 兼容端点，和 zeroclaw 同一个网关
VISION_KEY = "6666"
VISION_MODEL = "cn:hy3"                  # ★ 换成支持视觉的模型；不支持就把 VISION_ON 关掉
VISION_PROMPT = "请用中文简洁描述这张图片，重点提取：文字内容、报错信息、界面结构、关键数据。"

MEDIA_DIR = "/opt/dingtalk-bot/media"    # 钉钉发来的图片存这里

# ---- agent 生成图片后自动推送到钉钉 ----
# 原理：zeroclaw 只会在服务器本地生成图片文件，不会往外发；
#       bot 在执行完 agent 任务后，找出这次新产生的图片，上传到钉钉发出去。
AUTO_SEND_IMAGE = True
MAX_IMAGE_MB = 10                        # 超过此大小的图不自动发（钉钉有限制）
SCAN_DIRS = ["/tmp", "/root", MEDIA_DIR, "/www/wwwroot", "/opt/nezha"]
SCAN_MAX_DEPTH = 3                       # 扫描目录深度，防止遍历整个磁盘
SKIP_DIRS = ("node_modules", ".git", "cache", "logs", "vendor", "__pycache__")

# 备选：把图拷到本地网站目录，用公网链接发（media_id 方式失败时可兜底）
PUBLISH_DIR = ""                         # 例："/www/wwwroot/media"
PUBLIC_BASE_URL = ""                     # 例："http://zdyshk.cc.cd/media"

TIMEOUT_SHELL = 120                      # shell 命令超时（秒）
TIMEOUT_AGENT = 300                      # agent 超时（秒）
TIMEOUT_VISION = 120                     # 视觉识别超时（秒）
MAX_OUT = 3000                           # 文本结果最大字符数
# ==========================================================================


# ------------------------- 通用工具 -------------------------

ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")


def clean(s, limit=MAX_OUT):
    """去 ANSI 控制符 + 截断"""
    s = ANSI.sub("", (s or "").strip()) or "(无输出)"
    return s[:limit] + ("\n... 已截断" if len(s) > limit else "")


def run(cmd, timeout):
    """执行 shell 命令"""
    try:
        p = subprocess.run(
            cmd, shell=True, capture_output=True, text=True, timeout=timeout, cwd="/root",
            env={"PATH": "/root/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
                 "HOME": "/root", "LANG": "C.UTF-8"},
        )
        return clean((p.stdout or "") + (p.stderr or ""))
    except subprocess.TimeoutExpired:
        return "执行超时（%ss）" % timeout
    except Exception as e:
        return "执行失败: %s" % e


# ------------------------- 钉钉 API -------------------------

_tok = {"v": "", "exp": 0}


def get_token():
    """获取/缓存钉钉 access_token（有效期 2 小时，提前 5 分钟刷新）"""
    if _tok["v"] and time.time() < _tok["exp"] - 300:
        return _tok["v"]
    r = requests.get(
        "https://oapi.dingtalk.com/gettoken",
        params={"appkey": CLIENT_ID, "appsecret": CLIENT_SECRET}, timeout=15,
    ).json()
    if r.get("errcode") != 0 or not r.get("access_token"):
        raise RuntimeError("gettoken 失败: %s" % str(r)[:200])
    _tok["v"] = r["access_token"]
    _tok["exp"] = time.time() + int(r.get("expires_in", 7200))
    return _tok["v"]


def deep_find(obj, keyword, depth=0):
    """在任意嵌套结构里找字段名含 keyword 的值（钉钉各版本字段命名不统一，这样最稳）"""
    if depth > 8:
        return None
    if isinstance(obj, dict):
        for k, v in obj.items():
            if keyword in str(k).lower().replace("_", ""):
                if isinstance(v, str) and v:
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


def download_image(code):
    """用 downloadCode 从钉钉下载图片，返回字节内容"""
    token = get_token()
    r = requests.post(
        "https://api.dingtalk.com/v1.0/robot/messageFiles/download",
        headers={"x-acs-dingtalk-access-token": token, "Content-Type": "application/json"},
        json={"downloadCode": code, "robotCode": CLIENT_ID}, timeout=25,
    ).json()
    url = r.get("downloadUrl")
    if not url:
        raise RuntimeError("downloadCode 换 URL 失败: %s" % str(r)[:300])
    return requests.get(url, timeout=40).content


def _check_size(path):
    size = os.path.getsize(path)
    if size > MAX_IMAGE_MB * 1024 * 1024:
        raise RuntimeError("图片 %.1fMB，超过上限 %dMB" % (size / 1048576.0, MAX_IMAGE_MB))
    return size


def upload_image(path):
    """上传本地图片到钉钉，返回 media_id"""
    _check_size(path)
    token = get_token()
    with open(path, "rb") as fh:
        r = requests.post(
            "https://oapi.dingtalk.com/media/upload",
            params={"access_token": token, "type": "image"},
            files={"media": (os.path.basename(path), fh)}, timeout=90,
        ).json()
    if not r.get("media_id"):
        raise RuntimeError("上传失败: %s" % str(r)[:300])
    return r["media_id"]


def post_session(session_webhook, payload):
    """往当前会话（单聊/群）发消息"""
    return requests.post(session_webhook, json=payload, timeout=20).json()


def send_image_url(sw, url, title="图片"):
    """用 markdown 嵌入公网图片（要求 URL 公网可访问）"""
    return post_session(sw, {
        "msgtype": "markdown",
        "markdown": {"title": title, "text": "![](%s)" % url},
    })


# ------------------------- 视觉识别 -------------------------

def vision_describe(path):
    """把图片交给视觉模型，返回文字描述"""
    with open(path, "rb") as fh:
        b64 = base64.b64encode(fh.read()).decode()

    body = {
        "model": VISION_MODEL,
        "messages": [{
            "role": "user",
            "content": [
                {"type": "text", "text": VISION_PROMPT},
                {"type": "image_url", "image_url": {"url": "data:image/jpeg;base64," + b64}},
            ],
        }],
        "max_tokens": 800,
    }
    r = requests.post(
        VISION_URI.rstrip("/") + "/chat/completions",
        headers={"Authorization": "Bearer " + VISION_KEY, "Content-Type": "application/json"},
        json=body, timeout=TIMEOUT_VISION,
    ).json()

    if "choices" in r:
        return (r["choices"][0]["message"]["content"] or "").strip()
    raise RuntimeError("视觉接口返回异常: %s" % str(r)[:300])


# ------------------------- 产物图片：找出 agent 刚生成的图 -------------------------

IMG_EXT = (".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp")
IMG_IN_TEXT = re.compile(r"(/[\w./\-]+\.(?:png|jpe?g|gif|webp|bmp))", re.I)

_sent_images = set()   # 已经发过的图，避免重复推送


def _new_images_since(since_ts, limit=3):
    """在常用产出目录里找指定时间之后新建/修改的图片"""
    hits = []
    for base in SCAN_DIRS:
        if not os.path.isdir(base):
            continue
        for root, dirs, files in os.walk(base):
            depth = root[len(base):].count(os.sep)
            if depth > SCAN_MAX_DEPTH:
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
    """优先从 agent 输出文本里抠图片路径；抠不到再扫目录"""
    found = []
    seen = set()
    for raw_path in IMG_IN_TEXT.findall(out_text or ""):
        p = raw_path.strip("。，,;；、)）]】\"'")
        # 跳过图标类噪声资源
        low = p.lower()
        if any(x in low for x in ("/icon", "/favicon", "/logo", "/assets/emoji", ".min.")):
            continue
        if os.path.isfile(p) and p not in seen:
            seen.add(p)
            found.append(p)
    if not found:
        found = _new_images_since(since_ts, limit)
    return [p for p in found if p not in _sent_images][:limit]


def publish_to_web(path):
    """兜底：把图拷到网站目录，返回公网 URL"""
    if not (PUBLISH_DIR and PUBLIC_BASE_URL):
        return None
    os.makedirs(PUBLISH_DIR, exist_ok=True)
    name = "img_%d_%s" % (int(time.time()), os.path.basename(path))
    dst = os.path.join(PUBLISH_DIR, name)
    shutil.copy(path, dst)
    os.chmod(dst, 0o644)
    return PUBLIC_BASE_URL.rstrip("/") + "/" + name


# ------------------------- 消息解析 -------------------------

def find_key_exact(obj, name, depth=0):
    """字段名精确匹配（忽略大小写和下划线）——钉钉图片消息里同时有 pictureDownloadCode
       和 downloadCode 两个码，接口只认 downloadCode，必须精确匹配，不能用包含匹配"""
    if depth > 8:
        return None
    target = name.lower().replace("_", "")
    if isinstance(obj, dict):
        for k, v in obj.items():
            if str(k).lower().replace("_", "") == target:
                if isinstance(v, str) and v:
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
    """富文本消息：把用户输入的文字（richText 里的 text 片段）拼出来"""
    content = raw.get("content")
    if isinstance(content, str):
        try:
            content = json.loads(content)
        except Exception:
            content = None
    if not isinstance(content, dict):
        return ""
    parts = []
    for item in (content.get("richText") or []):
        if isinstance(item, dict) and item.get("text"):
            parts.append(item["text"])
    return "".join(parts).strip()


def extract_image_code(raw):
    """取出图片 downloadCode：优先精确 key，失败再模糊兜底"""
    content = raw.get("content")
    if isinstance(content, str):
        try:
            content = json.loads(content)
        except Exception:
            pass
    for host in (content, raw):
        v = find_key_exact(host, "downloadCode")
        if v:
            return v
    return deep_find({"raw": raw, "content": content}, "downloadcode")


def is_image_msg(raw):
    """判断这条消息是不是图片"""
    blob = json.dumps(raw, ensure_ascii=False).lower()
    return ("msgtype" in blob and "picture" in blob) or "downloadcode" in blob


# ------------------------- 主处理器 -------------------------

class Bot(dingtalk_stream.ChatbotHandler):

    def _reply_text(self, msg, s):
        try:
            self.reply_text(clean(s), msg)
        except Exception:
            pass

    def _maybe_send_images(self, msg, sw, out_text, since_ts):
        """agent 跑完后，把它这次生成的图片发到钉钉"""
        if not AUTO_SEND_IMAGE or not sw:
            return
        imgs = pick_images(out_text, since_ts)
        for path in imgs:
            _sent_images.add(path)
            try:
                mid = upload_image(path)
                post_session(sw, {"msgtype": "image", "image": {"media_id": mid}})
                print("[imgout] 已发送 %s" % path, flush=True)
            except Exception as e:
                # 原生 image 消息失败时，退回到公网链接方式
                url = publish_to_web(path)
                if url:
                    try:
                        send_image_url(sw, url, os.path.basename(path))
                        print("[imgout] 改用链接发送 %s" % url, flush=True)
                        continue
                    except Exception as e2:
                        print("[imgout] 链接发送也失败: %s" % e2, flush=True)
                print("[imgout] 发送失败 %s: %s" % (path, e), flush=True)
                self._reply_text(msg, "图片生成了但发不出去：%s\n文件在服务器：%s" % (e, path))

    async def process(self, callback):
        raw = callback.data or {}
        print("[raw]", json.dumps(raw, ensure_ascii=False)[:1200], flush=True)

        try:
            msg = dingtalk_stream.ChatbotMessage.from_dict(raw)
            sw = getattr(msg, "session_webhook", "") or ""

            text = ""
            if getattr(msg, "text", None):
                text = (msg.text.content or "").strip()
            # 富文本消息（图片 + 文字混发）时 msg.text 为空，从 richText 里取文字
            if not text:
                text = rich_text_content(raw)
            print("[text]", text, flush=True)
            # 群聊消息带「@机器人」前缀，去掉否则指令匹配不上
            if text.startswith("@") and " " in text:
                text = text.split(" ", 1)[1].strip()
            print("[msg]", text or "(图片/非文本消息)", flush=True)

            # ---------- ① 图片消息：下载 -> 识别 -> 交给 agent ----------
            if is_image_msg(raw):
                code = extract_image_code(raw)
                if not code:
                    self._reply_text(msg, "收到图片，但没解析出 downloadCode（钉钉字段格式有变），原文已打到日志。")
                    return AckMessage.STATUS_OK, "OK"

                os.makedirs(MEDIA_DIR, exist_ok=True)
                path = os.path.join(MEDIA_DIR, "img_%d.jpg" % int(time.time()))
                try:
                    data = download_image(code)
                    with open(path, "wb") as fh:
                        fh.write(data)
                    print("[img] 已保存 %s (%d 字节)" % (path, len(data)), flush=True)
                except Exception as e:
                    self._reply_text(msg, "图片下载失败：%s" % e)
                    return AckMessage.STATUS_OK, "OK"

                desc = ""
                if VISION_ON:
                    try:
                        desc = vision_describe(path)
                        print("[vision]", desc[:300], flush=True)
                    except Exception as e:
                        desc = "（图片识别失败：%s）" % e

                ask = text.strip(" @/").replace("zeroclaw", "").strip()
                task = (
                    "用户刚才在钉钉里发了一张图片，图片已保存到服务器：%s\n"
                    "请先用 image_info 工具查看这张图片、理解其内容，然后回应用户。"
                    "如果图里有报错、配置或数据，重点分析并给出具体建议。\n"
                    "用户随图发送的文字是：%s" % (path, ask or "（无）")
                )
                t0 = time.time() - 2
                out = run("%s agent -a %s -m %s" % (ZEROCLAW, AGENT, shlex.quote(task)), TIMEOUT_AGENT)
                self._reply_text(msg, out)
                self._maybe_send_images(msg, session_webhook, out, t0)
                return AckMessage.STATUS_OK, "OK"

            if not text:
                return AckMessage.STATUS_OK, "OK"

            # ---------- ② !pic 发送图片到钉钉 ----------
            if text.startswith("!pic "):
                target = text[5:].strip().strip('"').strip("'")
                try:
                    if target.startswith("http://") or target.startswith("https://"):
                        send_image_url(sw, target, "图片")
                    else:
                        if not os.path.isfile(target):
                            self._reply_text(msg, "文件不存在：%s" % target)
                            return AckMessage.STATUS_OK, "OK"
                        mid = upload_image(target)
                        post_session(sw, {"msgtype": "image", "image": {"media_id": mid}})
                    self._reply_text(msg, "图片已发送")
                except Exception as e:
                    self._reply_text(msg, "发送图片失败：%s" % e)
                return AckMessage.STATUS_OK, "OK"

            # ---------- ③ shell 直通 ----------
            if text.startswith("!"):
                self._reply_text(msg, run(text[1:].strip(), TIMEOUT_SHELL))
                return AckMessage.STATUS_OK, "OK"

            # ---------- ④ 其余交给 zeroclaw agent ----------
            t0 = time.time() - 2
            out = run("%s agent -a %s -m %s" % (ZEROCLAW, AGENT, shlex.quote(text)), TIMEOUT_AGENT)
            try:
                self.reply_markdown("ZeroClaw", out, msg)
            except Exception:
                self._reply_text(msg, out)
            self._maybe_send_images(msg, sw, out, t0)
            return AckMessage.STATUS_OK, "OK"

        except Exception:
            import traceback
            traceback.print_exc()
        return AckMessage.STATUS_OK, "OK"


def main():
    client = dingtalk_stream.DingTalkStreamClient(
        dingtalk_stream.Credential(CLIENT_ID, CLIENT_SECRET)
    )
    client.register_callback_handler(dingtalk_stream.ChatbotMessage.TOPIC, Bot())
    print("启动钉钉机器人（文本 + 图片）...", flush=True)
    client.start_forever()


if __name__ == "__main__":
    main()
