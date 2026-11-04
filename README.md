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

当前是单实例最小版本。飞书/微信/QQ/钉钉/网页组件的具体鉴权、事件订阅和渠道发送适配，放在下一阶段。

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

`kind` 当前支持 `text`、`card`、`select`、`handoff`。卡片和选择器的 `payload` 是渠道无关结构，后续各渠道适配器负责转换为飞书卡片、网页组件等。

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
- `GET /v1/sessions/{session_id}?tenant_id=...&user_id=...`：读取会话。
- `http://localhost:8000/docs`：OpenAPI 文档。

当前 HTTP 接口信任请求里的 `tenant_id`/`user_id`。生产环境必须由企业网关或渠道适配器完成身份鉴权，并用鉴权结果覆盖这两个字段，不能直接相信客户端传入值。

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
- 每个会话使用 Redis `SET NX PX` 租约锁和 Lua 比较 token 释放；锁等待超时返回 409。
- `MAX_TURNS` 默认 12；达到上限后自动产生 `handoff` action，并将会话标记为 `handed_off`。
- 会话过期、会话结束、跨用户访问分别返回 404/409/403。

下一阶段处理“新消息到达时旧回复过期”：

- 每次用户消息生成新的 `epoch`，旧生成任务收到取消信号。
- 生成结果提交前校验 `epoch`，旧结果不落库、不发送。
- 跨实例通过 Redis Stream 或 Pub/Sub 广播取消事件，并用 fencing token 防止旧实例写入。
- 有副作用的工具必须幂等；不可逆操作必须二次确认。

## 下一阶段待办

1. 飞书、微信、QQ、钉钉、网页组件的渠道适配器和卡片回调协议。
2. 真实转人工路由：飞书机器人转给开发者、其他渠道转给对应坐席/群。
3. 质检模型：自然度、事实一致性、身份和红线复核；不合格重生成，红线直接转人工。
4. Jev 路由/选择适配器，用于意图路由、工具选择和高风险分流。
5. 满意度评分、会话关闭策略、会话监控和运营指标。
6. 多实例消息消费、取消广播、幂等和死信队列。
7. 可选的 Docker sandbox 和更细粒度工具权限。

## 测试

```bash
.venv/bin/pytest -q
```

当前测试覆盖文本拆分、配置、知识事件解析、会话隔离、轮次上限、流式事件和 HTTP/SSE 接口。
