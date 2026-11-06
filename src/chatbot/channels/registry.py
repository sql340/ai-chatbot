from __future__ import annotations

from ..config import Settings
from .base import ChannelAdapter
from .feishu import FeishuAdapter
from .qq import QQOneBotAdapter
from .web import WebAdapter
from .wechat import WeChatOfficialAccountAdapter


class ChannelRegistry:
    def __init__(self, adapters: dict[str, ChannelAdapter]) -> None:
        self._adapters = adapters

    def get(self, name: str) -> ChannelAdapter | None:
        return self._adapters.get(name)

    @property
    def enabled(self) -> list[str]:
        return sorted(self._adapters)

    async def close(self) -> None:
        for adapter in self._adapters.values():
            await adapter.close()


def build_channel_registry(settings: Settings) -> ChannelRegistry:
    adapters: dict[str, ChannelAdapter] = {}
    if settings.web_enabled:
        adapters["web"] = WebAdapter()
    if settings.feishu_enabled:
        _require(
            settings.feishu_app_id and settings.feishu_app_secret,
            "FEISHU_APP_ID and FEISHU_APP_SECRET are required when FEISHU_ENABLED=true",
        )
        adapters["feishu"] = FeishuAdapter(settings)
    if settings.qq_enabled:
        _require(
            settings.qq_onebot_api_base_url,
            "QQ_ONEBOT_API_BASE_URL is required when QQ_ENABLED=true",
        )
        adapters["qq"] = QQOneBotAdapter(settings)
    if settings.wechat_enabled:
        _require(
            settings.wechat_token and settings.wechat_app_id and settings.wechat_app_secret,
            "WECHAT_TOKEN, WECHAT_APP_ID and WECHAT_APP_SECRET are required when WECHAT_ENABLED=true",
        )
        adapters["wechat"] = WeChatOfficialAccountAdapter(settings)
    return ChannelRegistry(adapters)


def _require(condition: object, message: str) -> None:
    if not condition:
        raise ValueError(message)


__all__ = ["ChannelRegistry", "build_channel_registry"]

