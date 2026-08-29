#!/usr/bin/env python3
from __future__ import annotations

import subprocess
import sys

TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
SEADROP_MINT = "0xe90cf9cc0a552cf52ea6ff74ece0f1c8ae8cc9ad630d3181f55ac43ca076b7d6"
SEAPORT_ORDER_FULFILLED = "0x9d9af8e38d66c62e2c12f0225249fd9d721c54b83f48d9352c97c6cacdcb6f31"
SEADROP_ADDRESS = "0x00005ea00ac477b1030ce78506496e8c2de24bf5"
SEAPORT_ADDRESS = "0x0000000000000068f116a894984e2db1123eb395"
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
ZERO_TOPIC = "0x" + "0" * 64


def keccak_topic(signature: str) -> str:
    try:
        from Crypto.Hash import keccak  # type: ignore
    except ImportError:
        subprocess.check_call([
            sys.executable,
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            "pycryptodome==3.23.0",
        ])
        from Crypto.Hash import keccak  # type: ignore
    digest = keccak.new(digest_bits=256)
    digest.update(signature.encode("ascii"))
    return "0x" + digest.hexdigest()


TRANSFER_SINGLE = keccak_topic("TransferSingle(address,address,address,uint256,uint256)")
TRANSFER_BATCH = keccak_topic("TransferBatch(address,address,address,uint256[],uint256[])")
CONSECUTIVE_TRANSFER = keccak_topic("ConsecutiveTransfer(uint256,uint256,address,address)")


def padded_address(address: str) -> str:
    value = address.lower()
    if not value.startswith("0x") or len(value) != 42:
        raise ValueError(f"invalid address: {address}")
    return "0x" + "0" * 24 + value[2:]
