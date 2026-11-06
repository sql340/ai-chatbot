from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import struct
import time
import xml.etree.ElementTree as ET

import httpx
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from ..config import Settings
from ..models import ChatResponse
from .base import (
    ChannelAuthError,
    ChannelDeliveryError,
    ChannelHttpRequest,
    ChannelPayloadError,
    ChannelReceiveResult,
    InboundMessage,
    render_as_text,
)


class WeChatOfficialAccountAdapter:
    """WeChat Official Account callback adapter (plaintext and safe mode)."""

    name = "wechat"  # type: ignore[assignment]

    def __init__(self, settings: Settings, client: httpx.AsyncClient | None = None) -> None:
        self._settings = settings
        self._client = client or httpx.AsyncClient(timeout=settings.wechat_timeout)
        self._owns_client = client is None
        self._access_token: str | None = None
        self._token_expires_at = 0.0
        self._token_lock = asyncio.Lock()

    async def receive(self, request: ChannelHttpRequest) -> ChannelReceiveResult:
        if request.method.upper() == "GET":
            encrypted_echo = request.query.get("echostr", "")
            if self._settings.wechat_encoding_aes_key:
                if not encrypted_echo:
                    raise ChannelAuthError("missing encrypted WeChat echostr")
                self._verify_signature(request.query, encrypted_echo)
                echo = self._decrypt(encrypted_echo)
            else:
                self._verify_signature(request.query)
                echo = encrypted_echo
            return ChannelReceiveResult(
                response=echo,
                media_type="text/plain",
                process_async=False,
            )

        payload = _parse_wechat_xml(request.body)
        encrypted = payload.get("Encrypt", "")
        if self._settings.wechat_encoding_aes_key:
            if not encrypted:
                raise ChannelAuthError("WeChat callback must be encrypted in safe mode")
            self._verify_signature(request.query, encrypted)
            payload = _parse_wechat_xml(self._decrypt(encrypted).encode())
        else:
            if encrypted:
                raise ChannelAuthError(
                    "encrypted WeChat callback requires WECHAT_ENCODING_AES_KEY"
                )
            self._verify_signature(request.query)

        message = self._parse_message(payload)
        return ChannelReceiveResult(
            message=message,
            response="success",
            media_type="text/plain",
            process_async=True,
        )

    async def send(self, inbound: InboundMessage, response: ChatResponse) -> None:
        open_id = str(inbound.reply.get("open_id") or inbound.user_id)
        for item in response.messages:
            text = render_as_text(item.text, item.payload)
            if text.strip():
                await self._send_text(open_id, text)

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    def _verify_signature(self, query: dict[str, str], encrypted: str | None = None) -> None:
        token = self._settings.wechat_token
        if not token:
            return
        timestamp = query.get("timestamp", "")
        nonce = query.get("nonce", "")
        if encrypted is None:
            signature = query.get("signature", "")
            parts = [token, timestamp, nonce]
        else:
            signature = query.get("msg_signature", "")
            parts = [token, timestamp, nonce, encrypted]
        expected = hashlib.sha1("".join(sorted(parts)).encode()).hexdigest()
        if not signature or not hmac.compare_digest(expected, signature):
            raise ChannelAuthError("invalid WeChat signature")

    def _decrypt(self, encrypted: str) -> str:
        key = self._aes_key()
        try:
            raw = base64.b64decode(encrypted)
            decryptor = Cipher(algorithms.AES(key), modes.CBC(key[:16])).decryptor()
            plain = decryptor.update(raw) + decryptor.finalize()
            padding = plain[-1]
            if not 1 <= padding <= 32:
                raise ValueError("invalid PKCS7 padding")
            plain = plain[:-padding]
            if len(plain) < 20:
                raise ValueError("decrypted payload is too short")
            message_length = struct.unpack("!I", plain[16:20])[0]
            message = plain[20 : 20 + message_length]
            app_id = plain[20 + message_length :].decode("utf-8")
            if len(message) != message_length:
                raise ValueError("invalid message length")
            if app_id != self._settings.wechat_app_id:
                raise ValueError("WeChat app_id mismatch")
            return message.decode("utf-8")
        except Exception as exc:
            raise ChannelAuthError("failed to decrypt WeChat payload") from exc

    def _aes_key(self) -> bytes:
        encoded = self._settings.wechat_encoding_aes_key
        try:
            key = base64.b64decode(encoded.rstrip("=") + "=", validate=True)
        except Exception as exc:
            raise ChannelAuthError("invalid WECHAT_ENCODING_AES_KEY") from exc
        if len(key) != 32:
            raise ChannelAuthError("WECHAT_ENCODING_AES_KEY must decode to 32 bytes")
        return key

    def _parse_message(self, payload: dict[str, str]) -> InboundMessage | None:
        if payload.get("MsgType") != "text":
            return None
        user_id = payload.get("FromUserName", "").strip()
        text = payload.get("Content", "").strip()
        if not user_id or not text:
            return None
        msg_id = payload.get("MsgId") or f"{payload.get('CreateTime', '')}:{user_id}"
        return InboundMessage(
            channel="wechat",
            tenant_id=self._settings.wechat_tenant_id,
            user_id=user_id,
            session_key=user_id,
            text=text,
            event_id=str(msg_id),
            reply={"open_id": user_id, "to_user": payload.get("ToUserName", "")},
            raw=payload,
        )

    async def _send_text(self, open_id: str, text: str) -> None:
        token = await self._get_access_token()
        response = await self._client.post(
            f"{self._settings.wechat_api_base_url}/cgi-bin/message/custom/send",
            params={"access_token": token},
            json={"touser": open_id, "msgtype": "text", "text": {"content": text}},
        )
        if response.is_error:
            raise ChannelDeliveryError(
                f"WeChat send failed: status={response.status_code} body={response.text[:300]}"
            )
        data = response.json()
        if data.get("errcode") not in (None, 0):
            raise ChannelDeliveryError(
                f"WeChat send failed: errcode={data.get('errcode')} errmsg={data.get('errmsg')}"
            )

    async def _get_access_token(self) -> str:
        async with self._token_lock:
            now = time.time()
            if self._access_token and now < self._token_expires_at - 60:
                return self._access_token
            response = await self._client.get(
                f"{self._settings.wechat_api_base_url}/cgi-bin/token",
                params={
                    "grant_type": "client_credential",
                    "appid": self._settings.wechat_app_id,
                    "secret": self._settings.wechat_app_secret,
                },
            )
            response.raise_for_status()
            data = response.json()
            if not data.get("access_token"):
                raise ChannelDeliveryError(
                    f"WeChat access token failed: errcode={data.get('errcode')} "
                    f"errmsg={data.get('errmsg')}"
                )
            self._access_token = str(data["access_token"])
            self._token_expires_at = now + int(data.get("expires_in", 7200))
            return self._access_token


def _parse_wechat_xml(body: bytes) -> dict[str, str]:
    try:
        root = ET.fromstring(body)
    except ET.ParseError as exc:
        raise ChannelPayloadError("invalid WeChat XML payload") from exc
    return {child.tag: child.text or "" for child in root}


__all__ = ["WeChatOfficialAccountAdapter"]

