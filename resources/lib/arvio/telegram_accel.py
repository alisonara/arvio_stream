# -*- coding: utf-8 -*-
"""Conservative Telegram download accelerators for Arvio Stream.

Two independent optimisations live here:
* optional PyCryptodome-backed AES-IGE when Telethon would otherwise use pyaes;
* a small number of real MTProto TCP senders for parallel upload.getFile calls.

Both are fail-open.  If a private Telethon API changes or the optional crypto
module is unavailable, callers can fall back to Telethon's normal downloader.
"""
from __future__ import annotations

import asyncio
import os


def _xor16(a, b):
    """XOR two 16-byte blocks without a Python per-byte loop."""
    return (int.from_bytes(bytes(a), "big") ^ int.from_bytes(bytes(b), "big")).to_bytes(16, "big")


def _ige_decrypt(cipher_text, key, iv, ecb_decrypt):
    data = bytes(cipher_text)
    if len(data) % 16:
        raise ValueError("AES-IGE ciphertext precisa ser múltiplo de 16 bytes")
    iv1 = bytes(iv[:16])
    iv2 = bytes(iv[16:32])
    out = bytearray(len(data))
    for pos in range(0, len(data), 16):
        cblock = data[pos:pos + 16]
        pblock = _xor16(ecb_decrypt(_xor16(cblock, iv2)), iv1)
        out[pos:pos + 16] = pblock
        iv1, iv2 = cblock, pblock
    return bytes(out)


def _ige_encrypt(plain_text, key, iv, ecb_encrypt):
    data = bytes(plain_text)
    padding = len(data) % 16
    if padding:
        data += os.urandom(16 - padding)
    iv1 = bytes(iv[:16])
    iv2 = bytes(iv[16:32])
    out = bytearray(len(data))
    for pos in range(0, len(data), 16):
        pblock = data[pos:pos + 16]
        cblock = _xor16(ecb_encrypt(_xor16(pblock, iv1)), iv2)
        out[pos:pos + 16] = cblock
        iv1, iv2 = cblock, pblock
    return bytes(out)


def install_fast_aes():
    """Install PyCryptodome AES-IGE only when Telethon lacks a native backend.

    Telethon already prefers cryptg and OpenSSL.  We never override either.
    Returns a short backend name suitable for diagnostics.
    """
    try:
        from telethon.crypto import aes as taes  # type: ignore
    except Exception:
        return "unknown"

    try:
        if getattr(taes, "cryptg", None):
            return "cryptg"
        libssl = getattr(taes, "libssl", None)
        if libssl is not None and getattr(libssl, "encrypt_ige", None) and getattr(libssl, "decrypt_ige", None):
            return "libssl"
    except Exception:
        pass

    CryptoAES = None
    try:
        from Crypto.Cipher import AES as CryptoAES  # type: ignore
    except Exception:
        try:
            from Cryptodome.Cipher import AES as CryptoAES  # type: ignore
        except Exception:
            CryptoAES = None
    if CryptoAES is None:
        return "pyaes"

    def decrypt_ige(cipher_text, key, iv):
        cipher = CryptoAES.new(bytes(key), CryptoAES.MODE_ECB)
        return _ige_decrypt(cipher_text, key, iv, cipher.decrypt)

    def encrypt_ige(plain_text, key, iv):
        cipher = CryptoAES.new(bytes(key), CryptoAES.MODE_ECB)
        return _ige_encrypt(plain_text, key, iv, cipher.encrypt)

    try:
        taes.AES.decrypt_ige = staticmethod(decrypt_ige)
        taes.AES.encrypt_ige = staticmethod(encrypt_ige)
        return "pycryptodome"
    except Exception:
        return "pyaes"


class ParallelDownloadError(RuntimeError):
    pass


class ParallelGetFile:
    """Small ordered pool of MTProto senders for one Telegram file/DC.

    This intentionally uses only a few connections (2 for normal accounts,
    up to 4 for Premium) and one request per sender at a time.  Chunks are
    reassembled strictly in offset order so the HTTP/local file is never
    corrupted by completion order.
    """

    def __init__(self, client, location, dc_id, part_size=256 * 1024, connections=2):
        self.client = client
        self.location = location
        self.dc_id = int(dc_id or getattr(client.session, "dc_id", 0) or 0)
        self.part_size = max(4096, min(256 * 1024, int(part_size)))
        self.part_size -= self.part_size % 4096
        self.connections = max(1, min(4, int(connections or 1)))
        self.senders = []
        self._owned = []

    async def _new_sender(self, auth_key):
        from telethon.network import MTProtoSender  # type: ignore
        dc = await self.client._get_dc(self.dc_id)
        sender = MTProtoSender(auth_key, loggers=self.client._log,
                               retries=2, delay=1, auto_reconnect=False,
                               connect_timeout=8)
        await sender.connect(self.client._connection(
            dc.ip_address, dc.port, dc.id,
            loggers=self.client._log, proxy=self.client._proxy,
            local_addr=self.client._local_addr))
        self._owned.append(sender)
        return sender

    async def open(self):
        if self.senders:
            return self
        session_dc = int(getattr(self.client.session, "dc_id", 0) or 0)
        if self.dc_id == session_dc:
            # Reuse the already-connected primary sender as one lane and clone
            # its authorization key for the extra TCP lanes.
            self.senders = [self.client._sender]
            auth_key = getattr(self.client.session, "auth_key", None) or getattr(self.client._sender, "auth_key", None)
        else:
            # Create exactly one exported sender first, then clone its auth key
            # for the remaining lanes. This avoids concurrent auth export.
            first = await self.client._create_exported_sender(self.dc_id)
            self._owned.append(first)
            self.senders = [first]
            auth_key = getattr(first, "auth_key", None)
        if not auth_key:
            raise ParallelDownloadError("Telegram não forneceu auth_key para o DC do arquivo.")
        while len(self.senders) < self.connections:
            self.senders.append(await self._new_sender(auth_key))
        return self

    async def close(self):
        if self._owned:
            await asyncio.gather(*(sender.disconnect() for sender in self._owned), return_exceptions=True)
        self._owned = []
        self.senders = []

    async def fetch_round(self, base_offset, remaining):
        """Fetch one ordered round, one chunk per connection."""
        from telethon.tl.functions.upload import GetFileRequest  # type: ignore
        tasks = []
        expected = []
        cursor = int(base_offset)
        left = max(0, int(remaining))
        for sender in self.senders:
            if left <= 0:
                break
            wanted = min(self.part_size, left)
            # Always request a full protocol-safe part. Telegram returns a
            # shorter final payload at EOF; slicing below keeps caller length.
            req = GetFileRequest(self.location, offset=cursor,
                                 limit=self.part_size, precise=False,
                                 cdn_supported=False)
            tasks.append(asyncio.ensure_future(self.client._call(sender, req)))
            expected.append((cursor, wanted))
            cursor += wanted
            left -= wanted
        if not tasks:
            return []
        try:
            results = await asyncio.gather(*tasks)
        except BaseException:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
        out = []
        for result, (offset, wanted) in zip(results, expected):
            raw = bytes(getattr(result, "bytes", b"") or b"")[:wanted]
            out.append((offset, raw, wanted))
        return out


__all__ = ["install_fast_aes", "ParallelGetFile", "ParallelDownloadError",
           "_ige_encrypt", "_ige_decrypt"]
