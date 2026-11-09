# AI 智能客服

基于 DeepAgents 的智能客服服务。第一阶段提供可运行的最小闭环：

- OpenAI 兼容的 Chat Completions 模型接入，主模型/质检模型/Embedding 分开配置。
- DeepAgents 负责澄清、工具选择、知识检索和回复生成。
- HTTP 普通响应与 SSE 流式响应。
- 可插拔 `enterprise-rag` 知识适配器；未启用时不注册知识工具。
- 发送卡片、选择器、转人工的结构化 outbound action。
- Redis 会话存储和按会话租约锁；本地 Redis 限制为 64 MB。
- 租户隔离：知识检索和会话键都带 `tenant_id`。
- 按两个换行拆分回复，单条消息长度可配置。
- 飞书、QQ OneBot、微信公众号和网页渠道适配器；网页渠道自带联调页面。

当前仍是单实例版本：渠道事件在进程内异步处理，会话绑定和事件去重可以使用 Redis。多租户识别、持久化消息队列、跨实例取消和钉钉适配仍待后续补全。

## 快速启动

```bash
cd ai-chatbot
cp .env.example .env
# 编辑 .env，至少填写 LLM_BASE_URL / LLM_API_KEY / LLM_MODEL

uv venv --python 3.11 .venv
uv pip install -e '.[dev]'

docker compose up -d redis
.venv/bin/uvicorn chatbot.main:app --reload --port 8000
```

启动后打开 `http://localhost:8000/` 即可使用网页联调页。页面支持流式/完整回复切换、选择器、卡片、会话恢复和转人工状态展示。

如果本机暂时没有 Docker，可把 `.env` 中的 `SESSION_BACKEND` 改成 `memory`，先验证 Agent 链路。Redis 模式下服务启动时会检查连接，不可用会直接报错，避免静默丢会话。

Docker 里的 Redis 使用：

```text
maxmemory 64mb
maxmemory-policy volatile-lru
save ""
appendonly no
```

会话键带 TTL，适合本地开发。线上应按真实会话量重新评估容量和淘汰策略。

## 接口

### 非流式

```bash
curl -sS http://localhost:8000/v1/chat \
  -H 'Content-Type: application/json' \
  -d '{
    "tenant_id": "acme",
    "user_id": "u_1001",
    "channel": "web",
    "message": "我想查一下订单"
  }'
```

响应中的 `messages` 是可直接交给渠道适配器的消息列表：

```json
{
  "session_id": "a5c5...",
  "turn_no": 1,
  "epoch": 1,
  "status": "active",
  "messages": [
    {"kind": "text", "text": "你想查哪个订单？", "payload": {}},
    {
      "kind": "select",
      "text": null,
      "payload": {
        "question": "你想办理哪项？",
        "options": [
          {"label": "查物流", "value": "查物流"},
          {"label": "开发票", "value": "开发票"}
        ],
        "allow_multiple": false
      }
    }
  ]
}
```

`kind` 当前支持 `text`、`card`、`select`、`handoff`。卡片和选择器的 `payload` 是渠道无关结构，各渠道适配器负责转换为飞书卡片、网页组件或文本降级。

`epoch` 表示本轮用户消息的代际。流式事件中的 `session/delta/done` 也会带 `epoch`；前端或渠道适配器可以用它丢弃已经被新消息取代的旧回复。

### 流式 SSE

```bash
curl -N http://localhost:8000/v1/chat/stream \
  -H 'Content-Type: application/json' \
  -d '{
    "tenant_id": "acme",
    "user_id": "u_1001",
    "message": "介绍一下你们的售后流程"
  }'
```

事件顺序：

| 事件 | 含义 |
| --- | --- |
| `session` | 会话已创建或读取，包含 `session_id` 和轮次 |
| `delta` | 模型增量文本，用于边生成边显示 |
| `message` | 已按空行拆分的完整文本消息，可交给渠道发送 |
| `action` | 卡片、选择器或转人工动作 |
| `done` | 本轮结束，包含最终会话状态 |
| `error` | 稳定错误码和用户可读信息 |

流式场景建议前端用 `delta` 做即时展示，用最终 `message`/`action` 做正式发送，避免渠道收到半句话。

### 其他接口

- `GET /health`：模型、RAG、Redis 和 sandbox 后端状态。
- `GET /`：网页联调页面。
- `GET|POST /v1/channels/{channel}/events`：统一渠道回调入口；当前支持 `web`、`feishu`、`qq`、`wechat`。
- `GET /v1/sessions/{session_id}?tenant_id=...&user_id=...`：读取会话。
- `http://localhost:8000/docs`：OpenAPI 文档。

当前 HTTP 接口信任请求里的 `tenant_id`/`user_id`。生产环境必须由企业网关或渠道适配器完成身份鉴权，并用鉴权结果覆盖这两个字段，不能直接相信客户端传入值。

## 渠道接入

所有渠道共用一个入口：`/v1/channels/{channel}/events`。除网页渠道以外的渠道会先快速 ACK，再在后台任务中生成并发送回复。事件 ID 会按 `channel + tenant_id + event_id` 去重，TTL 由 `CHANNEL_EVENT_DEDUPE_TTL_SECONDS` 控制；同一渠道账号的 `tenant_id + channel + session_key` 会绑定到同一个客服会话。

### 网页渠道

`WEB_ENABLED=true` 时启用：

- `GET /`：网页聊天页面，静态资源位于 `/static/`。
- `POST /v1/channels/web/events`：同步返回 `ChatResponse`，请求体就是 `ChatRequest`。
- `POST /v1/chat/stream`：网页页面可选的 SSE 流式接口。

页面默认从 URL 的 `tenant_id`、`user_id` 参数读取联调身份，例如：

```text
http://localhost:8000/?tenant_id=acme&user_id=u_1001
```

未提供时使用 `demo` 和浏览器本地随机访客 ID。页面上的“切换测试身份”仅用于本地联调。生产环境应移除该入口，由企业登录网关覆盖身份，并将页面嵌入已有门户；不能把页面上的身份字段当作可信认证。

### 飞书企业自建应用

配置：

```bash
FEISHU_ENABLED=true
FEISHU_TENANT_ID=acme
FEISHU_APP_ID=cli_xxx
FEISHU_APP_SECRET=xxx
FEISHU_ENCRYPT_KEY=xxx
FEISHU_VERIFICATION_TOKEN=xxx
```

在飞书开放平台中：

1. 创建企业自建应用并启用机器人能力。
2. 申请收发消息所需权限，至少包含读取用户发给机器人的单聊/群聊消息、以应用身份发消息、发送及更新交互卡片。
3. 在“事件与回调”中订阅 `im.message.receive_v1`（接收消息）和 `card.action.trigger`（卡片按钮回调）。
4. 将请求地址配置为 `https://<你的域名>/v1/channels/feishu/events`，按平台流程完成 URL 校验。
5. 生产环境配置 `FEISHU_ENCRYPT_KEY` 和 `FEISHU_VERIFICATION_TOKEN`，服务会校验签名和 verification token。

服务使用企业自建应用的 `tenant_access_token` 发送消息。`text` 转为文本消息，`select`/`card` 转为 interactive card；按钮回调会被转换成一条新的用户输入继续交给 Agent。当前没有使用延迟更新 token 原地更新卡片，所以点击按钮后会发送新消息，而不是修改原卡片。

### QQ：OneBot 11 HTTP

当前实现面向 NapCat、go-cqhttp 等兼容 OneBot 11 的实现，不是 QQ 官方机器人协议。

```bash
QQ_ENABLED=true
QQ_TENANT_ID=acme
QQ_ONEBOT_API_BASE_URL=http://127.0.0.1:3000
QQ_ONEBOT_ACCESS_TOKEN=optional-token
QQ_ONEBOT_WEBHOOK_SECRET=optional-webhook-secret
```

将 OneBot 的 HTTP 事件上报地址配置为：

```text
http://<chatbot-host>:8000/v1/channels/qq/events
```

同时开启 HTTP API，并保证 `QQ_ONEBOT_API_BASE_URL` 指向该 API。服务通过 `send_private_msg` 和 `send_group_msg` 回复；群聊按 `group:<group_id>:<user_id>` 维护独立会话。

两个密钥用途不同：

- `QQ_ONEBOT_ACCESS_TOKEN`：服务调用 OneBot HTTP API 时使用的 access token，对应 OneBot 配置的 `access_token`，请求会带 `Authorization: Bearer ...`。
- `QQ_ONEBOT_WEBHOOK_SECRET`：OneBot 向聊天服务上报事件时使用的签名 secret，对应 HTTP 客户端配置的 `secret`。配置后服务会按 OneBot 11 规范校验 `X-Signature: sha1=<HMAC-SHA1(body, secret)>`。

公网部署时两个都应配置，并保持与 OneBot 侧完全一致。只配置 access token 而 OneBot 使用签名 secret 时，入站事件会被拒绝。

当前只处理文本消息，卡片/选择器会降级为编号文本。QQ 官方 Bot API、频道消息和按钮交互不在本适配器范围内。

### 微信公众号

当前实现面向微信公众号（服务号/订阅号）的回调，不是个人微信或企业微信。

```bash
WECHAT_ENABLED=true
WECHAT_TENANT_ID=acme
WECHAT_TOKEN=callback-token
WECHAT_ENCODING_AES_KEY=optional-43-char-key
WECHAT_APP_ID=wx_xxx
WECHAT_APP_SECRET=xxx
```

公众号后台的服务器地址填写：

```text
https://<你的域名>/v1/channels/wechat/events
```

服务支持明文模式和安全模式。配置 `WECHAT_ENCODING_AES_KEY` 后会校验 `msg_signature` 并解密 `Encrypt` 内容；不配置时按明文回调处理，此时收到加密事件会拒绝。消息通过微信公众号客服消息接口异步发送，因此需要公众号具备客服消息权限，并注意微信的客服消息时间窗口限制。

当前只处理文本消息；卡片/选择器降级为文本。个人微信、企业微信和公众号被动回复 XML 不在本适配器范围内。

### 渠道共同限制

- 每个进程当前只配置一个静态渠道租户（`*_TENANT_ID`）。如果同一套应用接收多个企业的飞书/微信/QQ 应用，需要新增应用 ID 到租户的映射，不能只依赖静态环境变量。
- `BackgroundTasks` 不是持久队列，进程崩溃会丢事件。生产环境应改成 Redis Stream / Kafka + outbox，并配合幂等消费和死信队列。
- 飞书/微信/QQ 的事件 ACK 和实际发送是分离的；发送失败只记录日志并尝试发送兜底文本，不会重试渠道事件。
- 飞书和微信的发送客户端没有实现渠道侧的频控/退避重试；上线前需要补速率限制和错误分类。
- 钉钉和真实人工坐席路由尚未实现。当前 `handoff` 只改变会话状态并输出动作，网页页面会停止自动对话，飞书等渠道需要后续接入真实坐席/群通知。

## System Prompt 与 KV Cache

System Prompt 位于 `src/chatbot/prompts.py`，编排顺序固定为：

1. 角色和身份边界。
2. 理解、澄清、检索和回答顺序。
3. 口语化、消息拆分、卡片/选择器使用规则。
4. 事实约束和提示注入防护。
5. 红线与转人工规则。
6. 工具暴露和隐私约束。

为了尽可能命中 provider 的前缀/KV cache：

- System Prompt 完全静态，不包含租户 ID、用户 ID、时间戳、请求 ID 或当前日期；当前约 1.3k tokens，满足常见 provider 的前缀缓存阈值。
- Agent 和工具列表在进程启动时只构建一次，工具顺序稳定。
- 每轮只追加历史消息和新的用户消息，不重写系统前缀。
- 租户上下文只通过 `ToolRuntime[AgentContext]` 注入工具，不拼进 System Prompt。
- `max_history_messages` 默认 30；真的需要窗口裁剪时，后续应改为滚动摘要或稳定的压缩段，避免每轮改变前缀。

DeepAgents 自带的 Anthropic prompt-caching 中间件对 OpenAI 兼容模型不生效；OpenAI 自动前缀缓存无需显式标记，关键是保持前缀稳定。

## DeepAgents 沙箱边界

第一阶段使用 `StateBackend`：文件和 agent 中间状态存放在 LangGraph 会话状态里，不提供 shell `execute`，不会读写宿主机文件。

DeepAgents 自带的 `LocalShellBackend` 并没有隔离，命令会直接用当前用户权限在宿主机执行，不能用于这个客服服务。后续如果需要代码执行，应实现独立 Docker/VM sandbox backend，并限制网络、文件系统、CPU、内存和执行时间。

## RAG 接入

默认 `RAG_ENABLED=false`，此时不注册 `search_knowledge` 工具。启用后：

```bash
RAG_ENABLED=true
RAG_BASE_URL=http://localhost:8080
RAG_TENANT_FIELD=tenant_id
RAG_TOP_K=4
```

适配器读取 `enterprise-rag` 的 `POST /v1/query` SSE 流，只取 `sources` 事件后立即关闭连接，不会等待第二遍 LLM 生成。请求会携带：

```json
{"filters": {"tenant_id": "<当前租户>"}}
```

因此 `enterprise-rag` 的向量 payload 必须包含对应字段，并且只能召回当前租户的数据。不同租户拿到不同知识，不能只依赖 Prompt 约束。

## 会话、并发和上限

当前版本：

- Redis 按 `tenant_id + session_id` 存储会话，并设置 TTL。
- 渠道首次消息使用会话级锁完成原子 `get-or-create`，同一用户并发首消息不会创建多个会话。
- 每条用户消息分配递增 `active_epoch`，并用 `revision` CAS 写入；用户消息在生成前落库，旧生成的 assistant 结果不会覆盖新消息。
- 同一进程内新消息会取消旧的非流式生成；流式生成会在下一个事件检查点停止。旧结果提交前和渠道发送前都会再次校验 epoch。
- `MAX_TURNS` 默认 12；达到上限后自动产生 `handoff` action，并将会话标记为 `handed_off`。
- 会话过期、会话结束、跨用户访问分别返回 404/409/403。

latest-wins 的语义是“最后收到的用户消息优先”，不是“最后生成的回复优先”：

- 旧 epoch 的回复可以被丢弃，但已经发送到渠道的多条消息无法撤回。
- 有副作用的工具必须绑定 `turn_id/epoch` 并幂等；不可逆操作必须二次确认。当前工具只有卡片、选择器和知识检索，尚未接入写业务工具。
- 本切片通过 CAS 保证跨实例旧结果不会落库或发送；Redis Pub/Sub/Stream 的主动取消广播、持久队列和 outbox 重试仍属于下一阶段。

## 下一阶段待办

1. 真实转人工路由：飞书机器人转给开发者、其他渠道转给对应坐席/群，并记录人工接管状态。
2. 多租户渠道身份映射：按飞书 app_id、微信 app_id、QQ self_id 等解析租户，而不是使用静态 `*_TENANT_ID`。
3. 质检模型：自然度、事实一致性、身份和红线复核；不合格重生成，红线直接转人工。
4. Jev 路由/选择适配器，用于意图路由、工具选择和高风险分流。
5. 满意度评分、会话关闭策略、会话监控和运营指标。
6. 多实例消息消费、Redis 取消广播、outbox 幂等发送、重试和死信队列。
7. 飞书卡片延迟更新、QQ 官方 Bot API、钉钉适配器。
8. 可选的 Docker sandbox 和更细粒度工具权限。

## 测试

```bash
.venv/bin/pytest -q
```

当前测试覆盖文本拆分、配置、知识事件解析、会话隔离、轮次上限、流式事件、HTTP/SSE 接口、渠道回调签名、事件去重、网页静态入口和微信安全模式解密。
