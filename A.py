#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
钉钉远程操控服务器 bot（完整版）
================================
在原版（收消息 → zeroclaw / !命令 → 回 markdown）基础上新增：

  #file <绝对路径>   把服务器上的文件/图片发回钉钉会话
                     （先上传到钉钉媒体服务器换 downloadCode，再回 file/picture 消息）

部署（服务器上执行）：
  1) 备份旧脚本：cp /opt/dingtalk-bot/bot.py /opt/dingtalk-bot/bot.py.bak
  2) 用本文件覆盖 /opt/dingtalk-bot/bot.py（改好下面 4 个配置项）
  3) 装依赖：pip3 install --break-system-packages requests
  4) 重启：systemctl restart dingbot
  5) 钉钉里发：#file /etc/hostname  → 收到文件卡片即通

开放平台前提：应用需开通「企业内机器人发送消息」权限（勾选后记得发布新版本）。
"""

import json
import os
import re
import time
import shlex
import requests
import dingtalk_stream
from dingtalk_stream import AckMessage

# ==================== 配置区（改成你自己的） ====================
CLIENT_ID    = "dingx5jlxpvhpdci1jkr"              # 钉钉 Client ID（AppKey）
CLIENT_SECRET = "WFjiHGnAXm7CY1TUiIEVcZXR7MfLmhiCCH_ksLyF2wownMqE5NE51wv_2PfEfZ5k"  # Client Secret（AppSecret）
ZEROCLAW     = "/root/.cargo/bin/zeroclaw"         # which zeroclaw 的绝对路径
AGENT        = "ops"                               # zeroclaw agent 别名
# ================================================================

API  = "https://api.dingtalk.com"      # 新版 OpenAPI
OAPI = "https://oapi.dingtalk.com"     # 老版 API（媒体上传用这套，稳定）
MAX_UPLOAD_MB = 20                                  # 钉钉媒体上传上限
IMG_EXT = (".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp")

ANSI = re.compile(r'\x1b\[[0-9;]*[A-Za-z]')
HOME = "/root"                                      # 非 root 用户改成自己的家目录

_token_cache = {"token": "", "expire": 0}
_legacy_cache = {"token": "", "expire": 0}


def get_token():
    """新版 accessToken（api.dingtalk.com 用，约 2 小时有效，缓存复用）"""
    if _token_cache["token"] and time.time() < _token_cache["expire"]:
        return _token_cache["token"]
    r = requests.post(
        f"{API}/v1.0/oauth2/accessToken",
        json={"appKey": CLIENT_ID, "appSecret": CLIENT_SECRET},
        timeout=10,
    ).json()
    if "accessToken" not in r:
        raise RuntimeError(f"获取 accessToken 失败: {r}")
    _token_cache["token"] = r["accessToken"]
    _token_cache["expire"] = time.time() + 7000     # 官方 ~7200s，留 200s 余量
    return _token_cache["token"]


def get_legacy_token():
    """老版 access_token（oapi.dingtalk.com 媒体上传用，约 2 小时有效）"""
    if _legacy_cache["token"] and time.time() < _legacy_cache["expire"]:
        return _legacy_cache["token"]
    r = requests.get(
        f"{OAPI}/gettoken",
        params={"appkey": CLIENT_ID, "appsecret": CLIENT_SECRET},
        timeout=10,
    ).json()
    if r.get("errcode") != 0:
        raise RuntimeError(f"获取 access_token 失败: {r}")
    _legacy_cache["token"] = r["access_token"]
    _legacy_cache["expire"] = time.time() + 7000
    return _legacy_cache["token"]


def upload_file(path):
    """上传本地文件到钉钉媒体服务器（老接口 /media/upload，稳定可用）
    → 返回 media_id。图片传 type=image，其它传 type=file。≤20MB"""
    size = os.path.getsize(path)
    if size > MAX_UPLOAD_MB * 1024 * 1024:
        raise RuntimeError(f"文件 {size/1048576:.1f}MB 超过钉钉 {MAX_UPLOAD_MB}MB 上限")
    ftype = "image" if path.lower().endswith(IMG_EXT) else "file"
    with open(path, "rb") as f:
        r = requests.post(
            f"{OAPI}/media/upload",
            params={"access_token": get_legacy_token(), "type": ftype},
            files={"media": (os.path.basename(path), f)},   # 老接口表单字段名必须是 media
            timeout=120,
        ).json()
    if r.get("errcode") != 0:
        raise RuntimeError(f"上传失败: {r}")
    return r["media_id"]


def run(cmd, timeout):
    """本地执行 shell，二进制捕获 + 多编码解码，清理 ANSI 颜色码，限制输出长度"""
    try:
        p = subprocess.run(cmd, shell=True, capture_output=True,
                           timeout=timeout, cwd=HOME,
                           env={"PATH": "/root/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
                                "HOME": HOME, "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8"})
        raw = (p.stdout or b"") + (p.stderr or b"")

        # 二进制护栏（性能优化：只在头部 64KB + 尾部 8KB 上做判定，避免大文件全量扫描）
        def _is_binary(data):
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
                    txt = seg.decode("utf-8")
                except UnicodeDecodeError:
                    try:
                        txt = seg.decode("gbk")
                    except UnicodeDecodeError:
                        return True                    # UTF-8/GBK 都解不动 → 当二进制
                if txt.count("\ufffd") > 20:
                    return True
            return False

        if _is_binary(raw):
            # 提取二进制开始前的可读文本前缀（通常是 zeroclaw 自己说的话）
            prefix = raw.split(b"\x89PNG")[0].split(b"PK\x03\x04")[0]
            try:
                head_txt = prefix.decode("utf-8", errors="ignore").strip()
            except Exception:
                head_txt = ""
            head_txt = ANSI.sub("", head_txt)[:300]
            info = "(检测到二进制输出 %d 字节，已省略。如需在钉钉中查看请发送: #file <路径>)" % len(raw)
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
    except subprocess.TimeoutExpired:
        out = "执行超时（%ss）" % timeout
    except Exception as e:
        out = "执行失败: %s" % e
    out = ANSI.sub("", out)
    return out[:3000] + ("\n... 已截断" if len(out) > 3000 else "")


import subprocess  # noqa: E402  （run 用到，放这里避免顶部 import 顺序干扰阅读）


class Bot(dingtalk_stream.ChatbotHandler):

    # ---------- 文件/图片发送 ----------
    def reply_file(self, path, msg):
        """把本地文件发回当前会话：老接口上传 media_id → OpenAPI 主动发送
        依据官方文档 /v1.0/robot/groupMessages/send：
          - 参数名是 msgParam（JSON 字符串），不是对象
          - 图片 sampleImageMsg: {"photoURL": mediaId}
          - 文件 sampleFile:     {"mediaId":..., "fileName":..., "fileType":...}
        群聊走 groupMessages/send；人与机器人单聊走 oToMessages/batchSend"""
        media_id = upload_file(path)
        is_img = path.lower().endswith(IMG_EXT)
        name = os.path.basename(path)
        print("[file] uploaded %s (%d bytes, media_id=%s...)"
              % (path, os.path.getsize(path), media_id[:16]), flush=True)

        if is_img:
            msg_key, msg_param = "sampleImageMsg", json.dumps({"photoURL": media_id})
        else:
            msg_param = json.dumps({
                "mediaId": media_id,
                "fileName": name,
                "fileType": name.rsplit(".", 1)[-1].lower() if "." in name else "bin",
            })
            msg_key = "sampleFile"

        # 取字段策略：不同版本 SDK 属性名不同，按优先级逐个尝试
        #   dingtalk-stream 新版对象属性（下划线）→ 驼峰属性 → 原始字典 msg.data
        def _pick(names):
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

        ctype = str(_pick(("conversation_type", "conversationType")))   # "1"=单聊 "2"=群聊
        conv_id = str(_pick(("conversation_id", "conversationId")))
        staff = str(_pick(("sender_staff_id", "senderStaffId")))
        print("[file] conversationType=%r conversationId=%r senderStaffId=%r"
              % (ctype, conv_id, staff), flush=True)

        if ctype == "2":                            # 群聊
            payload = {
                "msgParam": msg_param,
                "msgKey": msg_key,
                "openConversationId": conv_id,
                "robotCode": CLIENT_ID,
            }
            url = f"{API}/v1.0/robot/groupMessages/send"
        elif ctype == "1" and staff:                # 人与机器人单聊
            payload = {
                "robotCode": CLIENT_ID,
                "userIds": [staff],
                "msgParam": msg_param,
                "msgKey": msg_key,
            }
            url = f"{API}/v1.0/robot/oToMessages/batchSend"
        else:
            raise RuntimeError(f"无法识别会话类型 conversationType={ctype!r}，请在群聊里 @机器人使用")

        r = requests.post(url, json=payload,
                          headers={"x-acs-dingtalk-access-token": get_token()},
                          timeout=15)
        print("[file] OpenAPI HTTP %s (%s)" % (r.status_code, url.rsplit('/', 1)[-1]), flush=True)
        if not r.ok:
            raise RuntimeError(f"发送失败 HTTP {r.status_code}: {r.text[:200]}")

    # ---------- zeroclaw 产出文件自动投递 ----------
    WS_DIR = "/root/.zeroclaw/agents/ops/workspace"   # zeroclaw 默认工作区（生成文件落在这里）

    def _resolve(self, p):
        """相对路径依次尝试 家目录 / zeroclaw 工作区；返回存在的绝对路径或 None"""
        if os.path.isabs(p):
            return p if os.path.isfile(p) else None
        for base in (HOME, self.WS_DIR):
            c = os.path.join(base, p)
            if os.path.isfile(c):
                return c
        return None

    def _send_attached(self, out, msg):
        """从 zeroclaw 输出中提取要投递的文件并发送，返回成功数量"""
        paths = re.findall(r'^\[FILE\]\s*(\S+)', out, flags=re.M)
        # 兜底：输出声称已生成/已保存时，扫描其中存在的媒体/文档路径
        if not paths and re.search(r"已生成|已保存|保存在|已复制到工作区", out):
            for cand in re.findall(r'([\w./\-]{2,160}\.(?:png|jpg|jpeg|gif|webp|bmp|zip|pdf|mp4|bin))',
                                   out, flags=re.I):
                cand = cand.strip('.').lstrip('/')
                r = self._resolve(cand)
                if r and r not in paths:
                    paths.append(r)
        n = 0
        for p in paths:
            full = self._resolve(p)
            if not full:
                continue
            try:
                self.reply_file(full, msg)
                n += 1
                print("[file] auto-sent %s" % full, flush=True)
            except Exception as e:
                print("[file] auto-send failed %s: %s" % (full, e), flush=True)
        return n

    # ---------- 消息主入口 ----------
    async def process(self, callback):
        msg = dingtalk_stream.ChatbotMessage.from_dict(callback.data)
        text = (msg.text.content or "").strip()
        if text.startswith("@") and " " in text:      # 群聊去掉 @机器人 前缀
            text = text.split(" ", 1)[1].strip()
        print("[msg]", text, flush=True)
        if not text:
            return AckMessage.STATUS_OK, "OK"

        # 别名归一：!pic / !file 等价于 #file（!pic 专发图片卡片）
        if text.startswith("!pic "):
            text = "#file " + text[5:].strip()
        elif text.startswith("!file "):
            text = "#file " + text[6:].strip()

        try:
            if text.startswith("#file "):
                # 发文件/图片：#file <绝对路径>
                path = text[6:].strip()
                if not os.path.isabs(path):
                    path = os.path.join(HOME, path)   # 允许相对家目录写
                if not os.path.isfile(path):
                    self.reply_text(f"文件不存在: {path}", msg)
                else:
                    self.reply_file(path, msg)
            elif text.startswith("!"):
                out = run(text[1:].strip(), 120)      # !开头 = 直跑 shell，120s 超时
                self.reply_markdown("Shell", out, msg)
            else:
                # 其他消息 = 交给 zeroclaw agent 理解执行，300s 超时
                # 附带投递约定：zeroclaw 要发文件时在回复末尾输出 [FILE] 完整路径
                prompt = (text + "\n\n（系统约定：如果你生成或需要把文件/图片交给用户，"
                          "请在回复最后另起一行输出 [FILE] 文件完整路径，例如 [FILE] /root/generated_image.png；"
                          "纯文字回答则忽略本约定。）")
                out = run("%s agent -a %s -m %s" % (ZEROCLAW, AGENT, shlex.quote(prompt)), 300)
                sent = self._send_attached(out, msg)
                if sent:
                    out = re.sub(r'^\[FILE\].*$', '', out, flags=re.M).strip()
                    out += "\n\n—— 以上 %d 个文件已作为附件发到群里" % sent
                self.reply_markdown("ZeroClaw", out, msg)
        except Exception as e:
            try:
                self.reply_text(f"出错了: {e}", msg)
            except Exception:
                pass
        return AckMessage.STATUS_OK, "OK"


client = dingtalk_stream.DingTalkStreamClient(
    dingtalk_stream.Credential(CLIENT_ID, CLIENT_SECRET))
client.register_callback_handler(dingtalk_stream.ChatbotMessage.TOPIC, Bot())
client.start_forever()
