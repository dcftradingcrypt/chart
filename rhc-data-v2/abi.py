#!/usr/bin/env python3
from __future__ import annotations

import json
from typing import Any

from topics import ZERO_ADDRESS

PAYMENT_ITEM_TYPES = {0, 1}
NFT_ITEM_TYPES = {2, 3, 4, 5}


def integer(value: Any, default: int = 0) -> int:
    try:
        text = str(value or "0")
        return int(text, 16) if text.startswith("0x") else int(float(text))
    except Exception:
        return default


def topic_address(topic: str) -> str:
    return "0x" + str(topic)[-40:].lower()


def word_address(word: str) -> str:
    return "0x" + str(word)[-40:].lower()


def words(data: str) -> list[str]:
    raw = data[2:] if data.startswith("0x") else data
    if len(raw) % 64:
        raise ValueError(f"ABI data length not divisible by 32 bytes: {len(raw) // 2}")
    return [raw[index:index + 64] for index in range(0, len(raw), 64)]


def dynamic_array(data_words: list[str], offset_bytes: int, tuple_words: int) -> list[list[str]]:
    index = offset_bytes // 32
    if index < 0 or index >= len(data_words):
        raise ValueError("dynamic array offset outside data")
    length = int(data_words[index], 16)
    start = index + 1
    end = start + length * tuple_words
    if end > len(data_words):
        raise ValueError("dynamic array truncated")
    return [
        data_words[start + item * tuple_words:start + (item + 1) * tuple_words]
        for item in range(length)
    ]


def decode_seadrop(topics: list[str], data: str) -> dict[str, Any]:
    if len(topics) != 4:
        raise ValueError(f"SeaDropMint expected 4 topics, got {len(topics)}")
    data_words = words(data)
    if len(data_words) < 5:
        raise ValueError("SeaDropMint data truncated")
    return {
        "nft_contract": topic_address(topics[1]),
        "minter": topic_address(topics[2]),
        "fee_recipient": topic_address(topics[3]),
        "payer": word_address(data_words[0]),
        "quantity": int(data_words[1], 16),
        "unit_price": int(data_words[2], 16),
        "fee_bps": int(data_words[3], 16),
        "stage_index": int(data_words[4], 16),
    }


def decode_seaport(topics: list[str], data: str) -> dict[str, Any]:
    if len(topics) != 4:
        raise ValueError(f"OrderFulfilled expected 4 topics, got {len(topics)}")
    data_words = words(data)
    if len(data_words) < 3:
        raise ValueError("OrderFulfilled data truncated")
    recipient = word_address(data_words[0])
    offer_rows = dynamic_array(data_words, int(data_words[1], 16), 4)
    consideration_rows = dynamic_array(data_words, int(data_words[2], 16), 5)
    offer = [
        {
            "item_type": int(row[0], 16),
            "token": word_address(row[1]),
            "identifier": int(row[2], 16),
            "amount": int(row[3], 16),
        }
        for row in offer_rows
    ]
    consideration = [
        {
            "item_type": int(row[0], 16),
            "token": word_address(row[1]),
            "identifier": int(row[2], 16),
            "amount": int(row[3], 16),
            "recipient": word_address(row[4]),
        }
        for row in consideration_rows
    ]
    return {
        "order_hash": str(topics[1]).lower(),
        "offerer": topic_address(topics[2]),
        "zone": topic_address(topics[3]),
        "recipient": recipient,
        "offer": offer,
        "consideration": consideration,
    }


def decode_erc721_transfer(topics: list[str]) -> dict[str, Any]:
    if len(topics) != 4:
        raise ValueError("ERC721 Transfer must have four topics")
    return {
        "from": topic_address(topics[1]),
        "to": topic_address(topics[2]),
        "token_id": int(topics[3], 16),
        "amount": 1,
    }


def decode_erc1155_single(topics: list[str], data: str) -> dict[str, Any]:
    if len(topics) != 4:
        raise ValueError("ERC1155 TransferSingle must have four topics")
    data_words = words(data)
    if len(data_words) < 2:
        raise ValueError("ERC1155 TransferSingle data truncated")
    return {
        "operator": topic_address(topics[1]),
        "from": topic_address(topics[2]),
        "to": topic_address(topics[3]),
        "token_id": int(data_words[0], 16),
        "amount": int(data_words[1], 16),
    }


def decode_erc1155_batch(topics: list[str], data: str) -> list[dict[str, Any]]:
    if len(topics) != 4:
        raise ValueError("ERC1155 TransferBatch must have four topics")
    data_words = words(data)
    if len(data_words) < 2:
        raise ValueError("ERC1155 TransferBatch data truncated")
    ids_rows = dynamic_array(data_words, int(data_words[0], 16), 1)
    values_rows = dynamic_array(data_words, int(data_words[1], 16), 1)
    if len(ids_rows) != len(values_rows):
        raise ValueError("ERC1155 batch IDs and values differ in length")
    return [
        {
            "operator": topic_address(topics[1]),
            "from": topic_address(topics[2]),
            "to": topic_address(topics[3]),
            "token_id": int(identifier[0], 16),
            "amount": int(amount[0], 16),
            "batch_item_index": index,
        }
        for index, (identifier, amount) in enumerate(zip(ids_rows, values_rows))
    ]


def decode_erc2309(topics: list[str], data: str) -> dict[str, Any]:
    if len(topics) != 4:
        raise ValueError("ConsecutiveTransfer must have four topics")
    data_words = words(data)
    if not data_words:
        raise ValueError("ConsecutiveTransfer data truncated")
    from_id = int(topics[1], 16)
    to_id = int(data_words[0], 16)
    if to_id < from_id:
        raise ValueError("ConsecutiveTransfer invalid token range")
    return {
        "from": topic_address(topics[2]),
        "to": topic_address(topics[3]),
        "from_token_id": from_id,
        "to_token_id": to_id,
        "amount": to_id - from_id + 1,
    }


def transfer_kind(from_address: str, to_address: str) -> str:
    if from_address == ZERO_ADDRESS:
        return "MINT"
    if to_address == ZERO_ADDRESS:
        return "BURN"
    return "TRANSFER"


def parse_topics(value: str | list[str]) -> list[str]:
    if isinstance(value, list):
        return [str(item).lower() for item in value]
    return [str(item).lower() for item in json.loads(value or "[]")]
