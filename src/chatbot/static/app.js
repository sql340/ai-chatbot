const API_BASE = window.CHATBOT_BASE_URL || "";
const IDENTITY_KEY = "chatbot.web.identity";
const SESSION_PREFIX = "chatbot.web.session";
const STREAM_KEY = "chatbot.web.streaming";

const $ = (id) => document.getElementById(id);
const els = {
  messages: $("messages"),
  emptyState: $("emptyState"),
  input: $("messageInput"),
  composer: $("composer"),
  sendButton: $("sendButton"),
  sendLabel: $("sendLabel"),
  stopButton: $("stopButton"),
  streamToggle: $("streamToggle"),
  newSessionButton: $("newSessionButton"),
  editIdentityButton: $("editIdentityButton"),
  identityDialog: $("identityDialog"),
  identityForm: $("identityForm"),
  tenantInput: $("tenantInput"),
  userInput: $("userInput"),
  cancelIdentity: $("cancelIdentity"),
  identityLabel: $("identityLabel"),
  sessionLabel: $("sessionLabel"),
  connectionDot: $("connectionDot"),
  connectionText: $("connectionText"),
  stageState: $("stageState"),
  notice: $("notice"),
  toast: $("toast"),
};

const identity = loadIdentity();
const state = {
  tenantId: identity.tenantId,
  userId: identity.userId,
  sessionId: null,
  sessionStatus: "active",
  streaming: readStorage(STREAM_KEY) !== "false",
  busy: false,
  abortController: null,
  runToken: 0,
  draft: null,
  draftText: "",
  finalMessageSeen: false,
  hadStreamError: false,
  toastTimer: null,
};

class ApiError extends Error {
  constructor(message, status = 0) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
}

function loadIdentity() {
  const params = new URLSearchParams(window.location.search);
  let stored = {};
  try {
    stored = JSON.parse(localStorage.getItem(IDENTITY_KEY) || "{}") || {};
  } catch {
    stored = {};
  }
  const fallbackUser = `visitor-${randomId()}`;
  return {
    tenantId: sanitizeTenant(params.get("tenant_id") || stored.tenantId || "demo"),
    userId: sanitizeUser(params.get("user_id") || stored.userId || fallbackUser),
  };
}

function sanitizeTenant(value) {
  const cleaned = String(value || "").trim().replace(/[^A-Za-z0-9._-]/g, "");
  return cleaned.slice(0, 64) || "demo";
}

function sanitizeUser(value) {
  return String(value || "").trim().slice(0, 128) || `visitor-${randomId()}`;
}

function randomId() {
  if (window.crypto && typeof window.crypto.randomUUID === "function") {
    return window.crypto.randomUUID().slice(0, 8);
  }
  return Math.random().toString(36).slice(2, 10);
}

function readStorage(key) {
  try {
    return localStorage.getItem(key);
  } catch {
    return null;
  }
}

function writeStorage(key, value) {
  try {
    localStorage.setItem(key, value);
  } catch {
    // Local storage can be unavailable in privacy modes; the chat still works in memory.
  }
}

function removeStorage(key) {
  try {
    localStorage.removeItem(key);
  } catch {
    // Ignore unavailable storage.
  }
}

function sessionStorageKey() {
  return `${SESSION_PREFIX}:${state.tenantId}:${state.userId}`;
}

function identityStorageValue() {
  return JSON.stringify({ tenantId: state.tenantId, userId: state.userId });
}

function updateIdentityLabels() {
  els.identityLabel.textContent = `${state.tenantId} / ${state.userId}`;
  els.identityLabel.title = `${state.tenantId} / ${state.userId}`;
}

function updateSessionLabel() {
  if (!state.sessionId) {
    els.sessionLabel.textContent = "尚未开始";
    els.sessionLabel.removeAttribute("title");
    return;
  }
  const shortId = state.sessionId.slice(0, 8);
  els.sessionLabel.textContent = `${shortId} · ${state.sessionStatus}`;
  els.sessionLabel.title = state.sessionId;
}

function persistSession() {
  if (!state.sessionId) {
    removeStorage(sessionStorageKey());
    return;
  }
  writeStorage(
    sessionStorageKey(),
    JSON.stringify({ sessionId: state.sessionId, status: state.sessionStatus }),
  );
}

function readPersistedSession() {
  try {
    const parsed = JSON.parse(readStorage(sessionStorageKey()) || "null");
    if (parsed && typeof parsed.sessionId === "string") {
      return parsed;
    }
  } catch {
    // Corrupt local state is treated as no session.
  }
  return null;
}

function setConnection(online, text) {
  els.connectionDot.className = `status-dot ${online ? "online" : "offline"}`;
  els.connectionText.textContent = text;
}

function setStageState(kind, text) {
  els.stageState.className = `stage-state${kind ? ` ${kind}` : ""}`;
  els.stageState.innerHTML = "";
  const dot = document.createElement("span");
  els.stageState.append(dot, document.createTextNode(` ${text}`));
}

function setBusy(busy) {
  state.busy = busy;
  const closed = state.sessionStatus !== "active";
  els.input.disabled = closed;
  els.sendButton.disabled = closed;
  els.stopButton.hidden = !busy;
  els.sendLabel.textContent = busy ? "打断并发送" : "发送";
  setStageState(busy ? "busy" : closed ? "closed" : "", busy ? "处理中" : closed ? "已转人工" : "可对话");
  if (!busy && !closed) {
    els.input.focus({ preventScroll: true });
  }
}

function setNotice(message) {
  if (!message) {
    els.notice.hidden = true;
    els.notice.textContent = "";
    return;
  }
  els.notice.textContent = message;
  els.notice.hidden = false;
}

function showToast(message) {
  clearTimeout(state.toastTimer);
  els.toast.textContent = message;
  els.toast.hidden = false;
  state.toastTimer = setTimeout(() => {
    els.toast.hidden = true;
  }, 3200);
}

function setSessionStatus(status) {
  state.sessionStatus = status || "active";
  updateSessionLabel();
  persistSession();
  if (state.sessionStatus === "handed_off") {
    setNotice("本次自动对话已结束，已为你转接人工。你可以建立新会话重新描述问题。");
  } else if (state.sessionStatus === "closed") {
    setNotice("本次会话已结束。建立新会话即可继续咨询。");
  } else {
    setNotice("");
  }
  setBusy(state.busy);
}

function updateSessionFrom(data) {
  if (data && typeof data.session_id === "string" && data.session_id) {
    state.sessionId = data.session_id;
  }
  if (data && data.status) {
    setSessionStatus(data.status);
  } else {
    updateSessionLabel();
    persistSession();
  }
}

function showEmptyState() {
  els.emptyState.hidden = false;
}

function hideEmptyState() {
  els.emptyState.hidden = true;
}

function clearConversation() {
  for (const article of els.messages.querySelectorAll(".message")) {
    article.remove();
  }
  state.draft = null;
  state.draftText = "";
  state.finalMessageSeen = false;
  state.hadStreamError = false;
  showEmptyState();
}

function startNewSession({ announce = true } = {}) {
  state.runToken += 1;
  if (state.abortController) {
    state.abortController.abort();
    state.abortController = null;
  }
  state.sessionId = null;
  state.sessionStatus = "active";
  persistSession();
  clearConversation();
  setNotice("");
  setBusy(false);
  updateSessionLabel();
  els.input.value = "";
  resizeInput();
  if (announce) {
    showToast("已建立新的本地会话");
  }
  els.input.focus({ preventScroll: true });
}

function scrollToBottom() {
  requestAnimationFrame(() => {
    els.messages.scrollTop = els.messages.scrollHeight;
  });
}

function createMessage(role, label) {
  const article = document.createElement("article");
  article.className = `message ${role}`;
  const meta = document.createElement("div");
  meta.className = "message-meta";
  meta.textContent = label || (role === "user" ? "你" : role === "assistant" ? "客服" : "提示");
  const body = document.createElement("div");
  body.className = "message-body";
  article.append(meta, body);
  els.messages.append(article);
  hideEmptyState();
  scrollToBottom();
  return { article, body };
}

function appendMessage(role, text) {
  const { article, body } = createMessage(role);
  const paragraph = document.createElement("p");
  paragraph.className = "message-text";
  paragraph.textContent = text;
  body.append(paragraph);
  scrollToBottom();
  return article;
}

function appendAssistantText(text) {
  if (!String(text || "").trim()) {
    return;
  }
  appendMessage("assistant", String(text));
  state.finalMessageSeen = true;
}

function beginDraft() {
  if (state.draft) {
    return state.draft;
  }
  const { article, body } = createMessage("assistant", "客服 / 正在输入");
  article.classList.add("is-draft");
  const typing = document.createElement("span");
  typing.className = "typing";
  typing.setAttribute("aria-label", "正在生成回复");
  typing.append(document.createElement("i"), document.createElement("i"), document.createElement("i"));
  body.append(typing);
  state.draft = { article, body };
  state.draftText = "";
  return state.draft;
}

function appendDraftDelta(text) {
  const draft = beginDraft();
  const typing = draft.body.querySelector(".typing");
  if (typing) {
    typing.remove();
  }
  let paragraph = draft.body.querySelector(".message-text");
  if (!paragraph) {
    paragraph = document.createElement("p");
    paragraph.className = "message-text";
    draft.body.append(paragraph);
  }
  state.draftText += text;
  paragraph.textContent = state.draftText;
  scrollToBottom();
}

function removeDraft() {
  if (state.draft) {
    state.draft.article.remove();
  }
  state.draft = null;
}

function finalizeDraft() {
  if (!state.draft) {
    return;
  }
  state.draft.article.classList.remove("is-draft");
  const meta = state.draft.article.querySelector(".message-meta");
  if (meta) {
    meta.textContent = "客服";
  }
  state.draft = null;
}

function renderOutbound(item) {
  if (!item || typeof item !== "object") {
    return;
  }
  if (item.kind === "text") {
    appendAssistantText(item.text || "");
  } else if (item.kind === "card") {
    renderCard(item);
  } else if (item.kind === "select") {
    renderSelect(item);
  } else if (item.kind === "handoff") {
    renderHandoff(item);
  } else if (item.text) {
    appendAssistantText(item.text);
  }
}

function renderCard(item) {
  const payload = item.payload || {};
  const { body } = createMessage("assistant", "客服 / 卡片");
  const card = document.createElement("section");
  card.className = "outbound-card";
  const kicker = document.createElement("span");
  kicker.className = "outbound-kicker";
  kicker.textContent = "CARD / 服务卡片";
  const title = document.createElement("h4");
  title.textContent = payload.title || item.text || "服务信息";
  const text = document.createElement("p");
  text.textContent = payload.body || item.text || "";
  card.append(kicker, title, text);

  const buttons = Array.isArray(payload.buttons) ? payload.buttons : [];
  if (buttons.length) {
    const actions = document.createElement("div");
    actions.className = "outbound-actions";
    for (const rawButton of buttons.slice(0, 4)) {
      const label = typeof rawButton === "string" ? rawButton : rawButton?.label || rawButton?.value;
      if (!label) {
        continue;
      }
      const button = document.createElement("button");
      button.type = "button";
      button.textContent = String(label);
      button.addEventListener("click", () => {
        if (state.busy || state.sessionStatus !== "active") {
          return;
        }
        disableActions(button);
        sendMessage(String(label));
      });
      actions.append(button);
    }
    card.append(actions);
  }
  body.append(card);
  scrollToBottom();
}

function renderSelect(item) {
  const payload = item.payload || {};
  const { body } = createMessage("assistant", "客服 / 选择");
  const select = document.createElement("section");
  select.className = "outbound-select";
  const kicker = document.createElement("span");
  kicker.className = "outbound-kicker";
  kicker.textContent = "SELECT / 请选择一个选项";
  const question = document.createElement("h4");
  question.textContent = payload.question || "你想办理哪项？";
  const hint = document.createElement("p");
  hint.textContent = payload.allow_multiple ? "可以选择多个选项，也可以直接回复。" : "点击一个选项，或直接回复内容。";
  select.append(kicker, question, hint);

  const options = Array.isArray(payload.options) ? payload.options : [];
  if (options.length) {
    const list = document.createElement("div");
    list.className = "outbound-options";
    for (const rawOption of options.slice(0, 6)) {
      const label =
        typeof rawOption === "string" ? rawOption : rawOption?.label || rawOption?.value;
      const value =
        typeof rawOption === "string" ? rawOption : rawOption?.value || rawOption?.label;
      if (!label || !value) {
        continue;
      }
      const button = document.createElement("button");
      button.type = "button";
      button.textContent = String(label);
      button.addEventListener("click", () => {
        if (state.busy || state.sessionStatus !== "active") {
          return;
        }
        disableActions(button);
        sendMessage(String(value));
      });
      list.append(button);
    }
    select.append(list);
  }
  body.append(select);
  scrollToBottom();
}

function renderHandoff(item) {
  const { body } = createMessage("assistant", "客服 / 转人工");
  const payload = item.payload || {};
  const card = document.createElement("section");
  card.className = "handoff-card";
  const mark = document.createElement("span");
  mark.className = "handoff-mark";
  mark.textContent = "!";
  const content = document.createElement("div");
  const title = document.createElement("strong");
  title.textContent = "已转接人工";
  const description = document.createElement("p");
  description.textContent = item.text || "我帮你转接人工同事，请稍等。";
  content.append(title, description);
  if (payload.reason) {
    const reason = document.createElement("p");
    reason.textContent = `原因：${payload.reason}`;
    content.append(reason);
  }
  card.append(mark, content);
  body.append(card);
  scrollToBottom();
}

function resizeInput() {
  els.input.style.height = "auto";
  els.input.style.height = `${Math.min(els.input.scrollHeight, 150)}px`;
}

function disableActions(button) {
  const group = button.closest(".outbound-actions, .outbound-options");
  if (!group) {
    return;
  }
  for (const item of group.querySelectorAll("button")) {
    item.disabled = true;
  }
}

async function sendMessage(rawText) {
  const text = String(rawText || "").trim();
  if (!text || state.sessionStatus !== "active") {
    return;
  }

  if (state.busy && state.abortController) {
    state.abortController.abort();
    if (state.draft) {
      removeDraft();
    }
  }

  hideEmptyState();
  appendMessage("user", text);
  state.finalMessageSeen = false;
  state.hadStreamError = false;
  removeDraft();
  state.draftText = "";
  els.input.value = "";
  resizeInput();

  const runToken = ++state.runToken;
  const controller = new AbortController();
  state.abortController = controller;
  setBusy(true);
  try {
    if (state.streaming) {
      await sendStreaming(text, runToken, controller.signal);
    } else {
      await sendOnce(text, runToken, controller.signal);
    }
  } catch (error) {
    if (runToken !== state.runToken) {
      return;
    }
    removeDraft();
    if (error && error.name === "AbortError") {
      appendMessage("system", "已停止生成本次回复。");
      showToast("已停止生成");
      return;
    }
    const message = error instanceof ApiError && error.message ? error.message : "消息发送失败，请稍后重试。";
    appendMessage("system", message);
    showToast(message);
  } finally {
    if (runToken === state.runToken) {
      state.abortController = null;
      setBusy(false);
    }
  }
}

async function requestJson(path, payload, signal) {
  const response = await fetch(`${API_BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
    signal,
  });
  if (!response.ok) {
    throw new ApiError(await errorDetail(response), response.status);
  }
  if (response.status === 204) {
    return null;
  }
  return response.json();
}

async function sendOnce(text, runToken, signal) {
  const data = await requestJson("/v1/channels/web/events", requestBody(text), signal);
  if (runToken !== state.runToken || !data) {
    return;
  }
  updateSessionFrom(data);
  for (const item of data.messages || []) {
    renderOutbound(item);
  }
  if (!(data.messages || []).length) {
    appendMessage("system", "这次没有拿到有效回复，请重试或建立新会话。");
  }
}

async function sendStreaming(text, runToken, signal) {
  const response = await fetch(`${API_BASE}/v1/chat/stream`, {
    method: "POST",
    headers: { "Content-Type": "application/json", Accept: "text/event-stream" },
    body: JSON.stringify(requestBody(text)),
    signal,
  });
  if (!response.ok) {
    throw new ApiError(await errorDetail(response), response.status);
  }
  if (!response.body) {
    throw new ApiError("浏览器不支持流式响应，请关闭流式回复后重试。", 0);
  }

  beginDraft();
  for await (const event of readSse(response.body)) {
    if (runToken !== state.runToken) {
      return;
    }
    handleStreamEvent(event.name, event.data);
  }

  if (runToken !== state.runToken) {
    return;
  }
  if (state.draft && !state.finalMessageSeen && state.draftText.trim()) {
    finalizeDraft();
  } else if (state.draft && !state.finalMessageSeen && !state.hadStreamError) {
    removeDraft();
    appendMessage("system", "这次没有拿到有效回复，请重试。");
  } else {
    removeDraft();
  }
}

function requestBody(text) {
  return {
    tenant_id: state.tenantId,
    user_id: state.userId,
    session_id: state.sessionId,
    channel: "web",
    message: text,
  };
}

function handleStreamEvent(name, data) {
  if (name === "session") {
    updateSessionFrom(data);
    return;
  }
  if (name === "delta") {
    appendDraftDelta(data.content || "");
    return;
  }
  if (name === "message") {
    if (state.draft) {
      removeDraft();
    }
    appendAssistantText(data.text || "");
    return;
  }
  if (name === "action") {
    renderOutbound(data);
    return;
  }
  if (name === "error") {
    state.hadStreamError = true;
    removeDraft();
    if (data.code === "superseded") {
      showToast("已按最新消息重新处理");
      return;
    }
    appendMessage("system", data.message || "客服暂时无法回复，请稍后重试。");
    showToast(data.message || "回复生成失败");
    return;
  }
  if (name === "done") {
    updateSessionFrom(data);
  }
}

async function* readSse(stream) {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) {
        break;
      }
      buffer += decoder.decode(value, { stream: true });
      let boundary = findEventBoundary(buffer);
      while (boundary) {
        const block = buffer.slice(0, boundary.index);
        buffer = buffer.slice(boundary.index + boundary.length);
        const event = parseSseBlock(block);
        if (event) {
          yield event;
        }
        boundary = findEventBoundary(buffer);
      }
    }
    buffer += decoder.decode();
    if (buffer.trim()) {
      const event = parseSseBlock(buffer);
      if (event) {
        yield event;
      }
    }
  } finally {
    reader.releaseLock();
  }
}

function findEventBoundary(buffer) {
  const match = /\r?\n\r?\n/.exec(buffer);
  return match ? { index: match.index, length: match[0].length } : null;
}

function parseSseBlock(block) {
  let name = "message";
  const dataLines = [];
  for (const line of block.split(/\r?\n/)) {
    if (line.startsWith("event:")) {
      name = line.slice(6).trim();
    } else if (line.startsWith("data:")) {
      dataLines.push(line.slice(5).trimStart());
    }
  }
  if (!dataLines.length) {
    return null;
  }
  try {
    return { name, data: JSON.parse(dataLines.join("\n")) };
  } catch {
    return null;
  }
}

async function errorDetail(response) {
  try {
    const data = await response.json();
    if (typeof data?.detail === "string") {
      return localizedError(data.detail);
    }
    if (data?.detail) {
      return JSON.stringify(data.detail);
    }
  } catch {
    // Fall through to a status-based message.
  }
  if (response.status === 403) {
    return "会话身份校验失败，请建立新会话。";
  }
  if (response.status === 409) {
    return "上一条消息还在处理，或会话已经结束。";
  }
  if (response.status === 404) {
    return "会话不存在或已过期，请建立新会话。";
  }
  return `请求失败（HTTP ${response.status}）`;
}

function localizedError(detail) {
  const messages = {
    "session is busy": "上一条消息还在处理，请稍后再试。",
    "session is closed or handed off": "本次会话已经结束，请建立新会话。",
    "session does not belong to this user": "这个会话不属于当前用户，请建立新会话。",
    "session not found or expired": "会话不存在或已过期，请建立新会话。",
    "generation superseded": "收到新消息，已按最新消息重新处理。",
    "agent invocation failed": "客服暂时无法回复，请稍后重试。",
    "invalid web chat payload": "消息格式不正确，请重新输入。",
  };
  return messages[detail] || detail;
}

async function restoreSession() {
  const persisted = readPersistedSession();
  if (!persisted) {
    return;
  }
  const query = new URLSearchParams({
    tenant_id: state.tenantId,
    user_id: state.userId,
  });
  try {
    const response = await fetch(
      `${API_BASE}/v1/sessions/${encodeURIComponent(persisted.sessionId)}?${query}`,
    );
    if (!response.ok) {
      removeStorage(sessionStorageKey());
      return;
    }
    const data = await response.json();
    state.sessionId = data.session_id;
    state.sessionStatus = data.status || "active";
    hideEmptyState();
    for (const message of data.messages || []) {
      if (message.role === "user") {
        appendMessage("user", message.content);
      } else {
        for (const part of String(message.content || "").split(/\n{2,}/)) {
          if (part.trim()) {
            appendMessage("assistant", part.trim());
          }
        }
      }
    }
    updateSessionLabel();
    setSessionStatus(state.sessionStatus);
    if (!data.messages?.length) {
      showEmptyState();
    }
    scrollToBottom();
  } catch {
    removeStorage(sessionStorageKey());
  }
}

async function checkHealth() {
  try {
    const response = await fetch(`${API_BASE}/health`, { headers: { Accept: "application/json" } });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const data = await response.json();
    setConnection(true, data.store_ok === false ? "服务在线 · 存储异常" : "服务在线");
  } catch {
    setConnection(false, "服务离线");
  }
}

function bindEvents() {
  els.composer.addEventListener("submit", (event) => {
    event.preventDefault();
    sendMessage(els.input.value);
  });

  els.input.addEventListener("input", resizeInput);
  els.input.addEventListener("keydown", (event) => {
    if (event.key === "Enter" && !event.shiftKey && !event.isComposing) {
      event.preventDefault();
      els.composer.requestSubmit();
    }
  });

  els.stopButton.addEventListener("click", () => {
    if (state.abortController) {
      state.abortController.abort();
    }
  });

  els.streamToggle.addEventListener("change", () => {
    state.streaming = els.streamToggle.checked;
    writeStorage(STREAM_KEY, state.streaming ? "true" : "false");
    showToast(state.streaming ? "已开启流式回复" : "已切换为完整回复");
  });

  els.newSessionButton.addEventListener("click", () => startNewSession());
  els.editIdentityButton.addEventListener("click", () => {
    els.tenantInput.value = state.tenantId;
    els.userInput.value = state.userId;
    els.identityDialog.showModal();
    els.tenantInput.focus();
  });
  els.cancelIdentity.addEventListener("click", () => els.identityDialog.close());
  els.identityForm.addEventListener("submit", (event) => {
    event.preventDefault();
    const tenantId = sanitizeTenant(els.tenantInput.value);
    const userId = sanitizeUser(els.userInput.value);
    if (!tenantId || !userId) {
      showToast("租户和用户 ID 不能为空");
      return;
    }
    state.tenantId = tenantId;
    state.userId = userId;
    writeStorage(IDENTITY_KEY, identityStorageValue());
    updateIdentityLabels();
    els.identityDialog.close();
    startNewSession({ announce: false });
    showToast("身份已切换，并建立了新会话");
  });

  for (const button of document.querySelectorAll("[data-suggestion]")) {
    button.addEventListener("click", () => sendMessage(button.dataset.suggestion || ""));
  }
}

async function initialize() {
  els.streamToggle.checked = state.streaming;
  updateIdentityLabels();
  updateSessionLabel();
  bindEvents();
  resizeInput();
  await checkHealth();
  await restoreSession();
  setBusy(false);
  els.input.focus({ preventScroll: true });
}

initialize();
