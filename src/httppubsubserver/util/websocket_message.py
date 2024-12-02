from typing import Literal, TypedDict, Union


class WSMessageBytes(TypedDict):
    type: Literal["websocket.receive"]
    bytes: bytes


class WSMessageText(TypedDict):
    type: Literal["websocket.receive"]
    text: str


class WSMessageDisconnect(TypedDict):
    type: Literal["websocket.disconnect"]
    code: int
    reason: str


WSMessage = Union[WSMessageBytes, WSMessageText, WSMessageDisconnect]
"""A better type for Message from websockets"""
