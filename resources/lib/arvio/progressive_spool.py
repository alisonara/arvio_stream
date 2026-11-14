# -*- coding: utf-8 -*-
"""Disk-backed progressive media spool for Telegram playback.

The downloader is intentionally independent from Kodi's HTTP reads: it keeps
writing the source sequentially until EOF/cancel, while the loopback proxy only
consumes bytes already committed to disk.  This keeps large buffers out of RAM.
"""
from __future__ import annotations

import os
import threading
import time


class ProgressiveSpool:
    def __init__(self, path, total, producer, prefix_size=0):
        self.path = str(path)
        self.total = max(0, int(total or 0))
        self.prefix_size = max(0, min(self.total, int(prefix_size or 0)))
        self._producer = producer
        self._cancel = threading.Event()
        self._condition = threading.Condition()
        self._thread = None
        self.downloaded = 0
        self.rate_mbps = 0.0
        self.connections = 1
        self.done = False
        self.error = None
        self.started_at = 0.0

    def start(self):
        if self._thread is not None:
            return self
        self.started_at = time.monotonic()
        self._thread = threading.Thread(target=self._run, name="ArvioTelegramSpool", daemon=True)
        self._thread.start()
        return self

    def _run(self):
        try:
            self._producer(self)
        except BaseException as exc:
            if not self._cancel.is_set():
                self.error = exc
        finally:
            with self._condition:
                try:
                    self.downloaded = max(self.downloaded, os.path.getsize(self.path))
                except Exception:
                    pass
                self.done = True
                self._condition.notify_all()

    def report(self, done, total=None, mbps=0.0, connections=1):
        with self._condition:
            self.downloaded = max(0, min(self.total, int(done or 0)))
            self.rate_mbps = max(0.0, float(mbps or 0.0))
            self.connections = max(1, int(connections or 1))
            self._condition.notify_all()

    def cancelled(self):
        return self._cancel.is_set()

    def cancel(self):
        self._cancel.set()
        with self._condition:
            self._condition.notify_all()

    def status(self):
        with self._condition:
            return {
                "path": self.path,
                "total": self.total,
                "downloaded": self.downloaded,
                "rate_mbps": self.rate_mbps,
                "connections": self.connections,
                "done": self.done,
                "cancelled": self._cancel.is_set(),
                "error": self.error,
            }

    def wait_for(self, target_bytes, cancel_check=None, progress_callback=None, timeout=None):
        target = max(0, min(self.total, int(target_bytes or 0)))
        started = time.monotonic()
        while True:
            status = self.status()
            if progress_callback is not None:
                progress_callback(status, target)
            if status["downloaded"] >= target:
                return status
            if status["error"] is not None:
                raise status["error"]
            if status["done"]:
                if status["downloaded"] < target:
                    raise RuntimeError("Spool Telegram terminou antes do alvo (%d/%d)." %
                                       (status["downloaded"], target))
                return status
            if cancel_check is not None and cancel_check():
                self.cancel()
                raise RuntimeError("Pré-buffer Telegram cancelado.")
            if timeout is not None and time.monotonic() - started >= float(timeout):
                raise TimeoutError("Pré-buffer Telegram excedeu o tempo limite.")
            with self._condition:
                self._condition.wait(timeout=0.25)

    def wait_for_growth(self, offset, timeout=0.5):
        wanted = max(0, int(offset or 0))
        with self._condition:
            if self.downloaded > wanted or self.done or self.error is not None or self._cancel.is_set():
                return self.status()
            self._condition.wait(timeout=max(0.01, float(timeout or 0.5)))
        return self.status()

    def join(self, timeout=None):
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=timeout)
        return not (thread is not None and thread.is_alive())
