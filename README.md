# 用钉钉远程操控服务器 · 通用部署文档

> **效果**：在钉钉里 @机器人 发**中文** → 服务器上的 AI agent 理解并执行 → 结果以 markdown 卡片回传；发 **`!命令`** → 直跑 shell，秒回。
> 
> **适用**：任何能出网的 Linux 服务器（云服务器、VPS、内网机器都行），不需要公网 IP、域名、HTTPS 证书。

---

## 开始前：准备 5 样东西

|   | 需要什么 | 说明 |
|---|---|---|
| 1 | 一台 Linux 服务器 | Ubuntu / Debian / CentOS 都行，能访问外网即可 |
| 2 | 钉钉开放平台账号 | 有权限创建「企业内部应用」（个人也能创建） |
| 3 | 一个 LLM API | 给 AI agent 用，OpenAI 兼容格式即可（自建网关、中转、官方 API 都行） |
| 4 | zeroclaw | 负责把中文翻译成命令并执行，**可选**（见 6.4，只想要命令执行可不装） |
| 5 | Python 3 + pip | 服务器上一般自带 |

**部署前先把这几个变量定下来**，后面全程用：

| 变量 | 取值来源 | 本文示例 |
|---|---|---|
| `<CLIENT_ID>` | 钉钉应用 → 凭据与基础信息 → Client ID | `dingxxxxxxxx` |
| `<CLIENT_SECRET>` | 同上 → Client Secret | （一串长字符串） |
| `<AGENT_NAME>` | zeroclaw onboarding 时建的 agent 别名 | `ops` |
| `<ZEROCLAW_PATH>` | `which zeroclaw` 的输出 | `/root/.cargo/bin/zeroclaw` |

---

## 一、原理（一句话）

```
钉钉 App ──发消息──> 钉钉云端 ──WebSocket(Stream)──> 服务器上的 bot.py
                                                       │
                                          中文 ──> zeroclaw agent -a <AGENT> -m "中文"
                                          !开头 ──> 直接执行 shell
                                                       │
                                     <────结果回传钉钉────┘
```

**为什么用 Stream 模式**：bot 主动往外连钉钉，所以**不需要公网回调地址、不需要域名、不需要 HTTPS 证书**。云厂商的安全组只管入站，跟这个出站长连接无关——不用去开端口。

---

## 二、安装 zeroclaw

它负责"中文 → 理解意图 → 执行命令"，是这个方案的大脑。

```bash
curl -fsSL https://raw.githubusercontent.com/zeroclaw-labs/zeroclaw/master/install.sh | sh
"${CARGO_HOME:-$HOME/.cargo}/bin/zeroclaw" quickstart
```

### 2.1 onboarding 向导怎么选

| 选择器 | 怎么走 | 填什么 |
|---|---|---|
| Model provider | Enter → Custom (OpenAI-compatible) | alias 随便起；uri 填你的 OpenAI 兼容地址；api_key 填你的 key；model 填模型名 |
| Risk profile | Enter → 选 **YOLO** → 关键一步 | 无额外输入（允许所有命令、自动批准、无沙盒） |
| Memory | Enter → sqlite | 默认回车 |
| Channels | **直接 Esc 跳过**（钉钉由 bot 负责） | — |
| Peer groups | 跳过 | — |
| Plugins | Esc 跳过 | — |
| Agent identity | Enter → 填一个名字 | 如 `ops`、重名就加数字 |

### 2.2 强制走原生工具调用（推荐开）

```bash
zeroclaw config set providers.models.custom.<你的alias>.native_tools true
```

> 让 agent 用模型的原生 tool-call 能力，而不是靠文本解析猜命令，执行更稳、失败更少。

### 文件访问不受限

```bash
在配置文件config.toml内 [risk_profiles.yolo] 后添加 forbidden_paths = [] 
```

### 2.3 验证 agent 能跑

```bash
# 把 .cargo/bin 加到 PATH（以后直接输 zeroclaw 就能跑）
echo 'export PATH="$PATH:$HOME/.cargo/bin"' >> ~/.bashrc
source ~/.bashrc
```

```bash
zeroclaw agent -a <AGENT_NAME> -m "报一下当前时间和系统负载"
```

返回中文报告即通，通常 10~30 秒（要过一次 LLM）。

> ⚠️ 单次模式**必须加 `-m`**，直接跟文本会报 `unexpected argument`。

---

## 三、钉钉开放平台配置

1. **创建应用**：开放平台 → 应用开发 → 企业内部应用 → 类型选「**机器人**」。
2. **事件订阅 → 推送方式**：选「**Stream 模式推送**」→ 保存。（bot 跑起来后这里会显示绿色「✅ 连接接入成功」）
3. **版本发布**：任何改动都要点「版本管理与发布 → 发布」，顶部黄条「版本发布后，当前修改才能生效」消失才算生效。**这步最容易漏。**
4. **拿凭据**：凭据与基础信息页里的 **Client ID** 和 **Client Secret**，就是前面那两个变量。
```bash
权限开通：Card.Streaming.Write  Card.Instance.Write  qyapi_robot_sendmsg
```
---

## 四、部署 bot（核心）

### 4.1 装依赖

```bash
pip3 install dingtalk-stream 2>/dev/null || pip3 install --break-system-packages dingtalk-stream
# 关键：websockets 必须 <14，否则 WSS 握手会静默卡死（无任何报错！）
pip3 install --break-system-packages "websockets<14"
python3 -c "import dingtalk_stream; print('SDK OK')"
```

### 4.2 写脚本（请把 4 个占位符替换成你自己的值）

```bash
mkdir -p /opt/dingtalk-bot && cat > /opt/dingtalk-bot/bot.py << 'PYEOF'
import subprocess, shlex, re, dingtalk_stream
from dingtalk_stream import AckMessage

# ===== 需要你填的 4 个值 =====
CLIENT_ID = "<CLIENT_ID>"           # 钉钉 Client ID
CLIENT_SECRET = "<CLIENT_SECRET>"   # 钉钉 Client Secret
ZEROCLAW = "<ZEROCLAW_PATH>"        # zeroclaw 绝对路径，which zeroclaw 查
AGENT = "<AGENT_NAME>"              # zeroclaw agent 别名，如 ops
# ===========================

ANSI = re.compile(r'\x1b\[[0-9;]*[A-Za-z]')
HOME = "/root"                      # 非 root 用户改成自己的家目录

def run(cmd, timeout):
    try:
        p = subprocess.run(cmd, shell=True, capture_output=True, text=True,
                           timeout=timeout, cwd=HOME,
                           env={"PATH": "/root/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
                                "HOME": HOME, "LANG": "C.UTF-8"})
        out = (p.stdout + p.stderr).strip() or "(无输出)"
    except subprocess.TimeoutExpired:
        out = "执行超时（%ss）" % timeout
    except Exception as e:
        out = "执行失败: %s" % e
    out = ANSI.sub("", out)
    return out[:3000] + ("\n... 已截断" if len(out) > 3000 else "")

class Bot(dingtalk_stream.ChatbotHandler):
    async def process(self, callback):
        msg = dingtalk_stream.ChatbotMessage.from_dict(callback.data)
        text = (msg.text.content or "").strip()
        if text.startswith("@") and " " in text:      # 群聊去掉 @机器人 前缀
            text = text.split(" ", 1)[1].strip()
        print("[msg]", text, flush=True)
        if not text:
            return AckMessage.STATUS_OK, "OK"
        if text.startswith("!"):
            out = run(text[1:].strip(), 120)          # !开头 = 直跑 shell，120s 超时
        else:
            # 其他消息 = 交给 zeroclaw agent 理解执行，300s 超时
            out = run("%s agent -a %s -m %s" % (ZEROCLAW, AGENT, shlex.quote(text)), 300)
        try:
            self.reply_markdown("ZeroClaw", out, msg) # markdown 卡片更好看
        except Exception:
            self.reply_text(out, msg)                 # 老版本 SDK 回退纯文本
        return AckMessage.STATUS_OK, "OK"

client = dingtalk_stream.DingTalkStreamClient(
    dingtalk_stream.Credential(CLIENT_ID, CLIENT_SECRET))
client.register_callback_handler(dingtalk_stream.ChatbotMessage.TOPIC, Bot())
client.start_forever()
PYEOF
python3 -m py_compile /opt/dingtalk-bot/bot.py && echo "语法 OK"
```

>  <HOME_DIR> 为当前 WSL 用户家目录，执行 echo $HOME 获取；<ZEROCLAW_PATH> 是 zeroclaw 二进制完整绝对路径，用 which zeroclaw 查询；<AGENT_NAME> 是 agent 别名，由 zeroclaw agent --list 列出；以上脚本无法识别图片发送文件，若需要将脚本改成项目中的bot.py


### 4.3 手动测试

```bash
python3 /opt/dingtalk-bot/bot.py
```

- 日志出现 `open connection` + `endpoint is {...}` 后没有更多输出是正常的（SDK 不一定打印 connect success）。
- 日志里刷 `unknown message topic, topic=ping` 是**心跳包，代表连接活着**，不是错误。
- 判定连没连上：看钉钉后台那个绿色「✅ 连接接入成功」。
- 钉钉里发中文（如「报一下磁盘占用」）→ 终端出现 `[msg] ...` → 十几秒后收到报告。发 `!df -h` 秒回。

### 4.4 挂 systemd 常驻（开机自启 + 崩溃自动拉起）

Ctrl+C 停掉手动进程，然后：

```bash
cat > /etc/systemd/system/dingbot.service << 'EOF'
[Unit]
Description=DingTalk Remote Bot
After=network-online.target
[Service]
ExecStart=/usr/bin/python3 /opt/dingtalk-bot/bot.py
Restart=always
RestartSec=5
[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload && systemctl enable --now dingbot
systemctl status dingbot --no-pager | head -10   # 看到 active (running) 即完成
```

---

## 五、日常用法

| 你发什么 | 发生什么 |
|---|---|
| 「看下磁盘占用」「服务器现在什么情况」 | agent 用 LLM 理解 → 自动执行命令 → 回 markdown 报告 |
| 「重启 nginx」「看下 xx 容器日志」 | agent 直接执行（YOLO 档案，无需批准） |
| `!free -h`、`!docker ps` | 跳过 agent，直跑 shell，秒回 |
| 任何 shell 能干的事 | 用中文描述即可，agent 会自己拆解成命令 |

**安全提示**：这套方案没有身份校验——**任何能给机器人发消息的人等于拿到你的服务器权限**。建议只单聊使用，**不要把机器人拉进群**；如需限制，在脚本里加一层 `sender_staff_id` 白名单判断即可。

## 六、运维命令

```bash
journalctl -u dingbot -f      # 实时日志（每条指令都会打印 [msg]）
systemctl restart dingbot     # 改了 bot.py 或 zeroclaw 配置后执行
systemctl status dingbot
```
```bash
# 查看有哪些agent
zeroclaw agents list

# 删除不用的 agent
zeroclaw agents delete <AGENT_NAME> --yes

# 启动聊天
zeroclaw agent -a <AGENT_NAME>

# 查询运行的agent进程
pgrep -af zeroclaw

# 强制关闭它
kill -9 查到的PID
```

**文件位置**：`/opt/dingtalk-bot/bot.py`、`/etc/systemd/system/dingbot.service`
**依赖版本**：`dingtalk-stream 0.24.3`、`websockets 13.1`（不要升到 14+）
