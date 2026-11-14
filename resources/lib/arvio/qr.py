# -*- coding: utf-8 -*-
"""QR rendering and a responsive Kodi dialog."""

from __future__ import annotations

import os
import time

try:
    import qrcode
    from qrcode.image.pure import PyPNGImage
except Exception:
    qrcode = None
    PyPNGImage = None

try:
    import xbmc
    import xbmcgui
except ImportError:
    xbmc = None  # type: ignore
    xbmcgui = None  # type: ignore

from .kodi_utils import Kodi

ADDON_TITLE = "Arvio Stream"


if xbmcgui is not None:  # pragma: no cover - Kodi only
    class QRDialog(xbmcgui.WindowDialog):
        def __init__(self, title, hint, image_path):
            super(QRDialog, self).__init__()
            self._closed = False
            self._setup_ok = False
            try:
                self._setup(title, hint, image_path)
                self._setup_ok = True
            except Exception as exc:
                Kodi.log("QR", "Falha ao montar diálogo: %s" % exc, level="error")
                self.close()

        def _setup(self, title, hint, image_path):
            width = max(int(self.getWidth() or 1280), 640)
            height = max(int(self.getHeight() or 720), 480)
            margin = max(20, int(min(width, height) * 0.035))
            reserved = max(160, int(height * 0.22))
            box = int(min(width * 0.58, height - reserved - 2 * margin, 620))
            box = max(220, box)
            x = int((width - box) / 2)
            qr_y = max(margin + 54, int((height - box) / 2) - 15)
            if qr_y + box + 105 > height:
                qr_y = max(margin + 48, height - box - 115)

            self.addControl(xbmcgui.ControlImage(0, 0, width, height,
                                                  filename="solidcolor:FF101010"))
            self.addControl(xbmcgui.ControlLabel(
                margin, margin, width - 2 * margin, 44, title,
                font="font24", textColor="FFFFFFFF", alignment=0x00000002))
            self.addControl(xbmcgui.ControlImage(x, qr_y, box, box, image_path))

            hint_y = qr_y + box + 10
            hint_h = max(50, height - hint_y - 48)
            try:
                text = xbmcgui.ControlTextBox(margin, hint_y, width - 2 * margin, hint_h,
                                              font="font13", textColor="FFEEEEEE")
                text.setText(hint)
                self.addControl(text)
            except Exception:
                self.addControl(xbmcgui.ControlLabel(
                    margin, hint_y, width - 2 * margin, min(90, hint_h), hint,
                    font="font13", textColor="FFEEEEEE", alignment=0x00000002))

            self.addControl(xbmcgui.ControlLabel(
                margin, height - 38, width - 2 * margin, 28,
                "Back / Esc para fechar", font="font12",
                textColor="FF888888", alignment=0x00000002))

        def onAction(self, action):
            try:
                action_id = action.getId()
            except Exception:
                try:
                    action_id = int(action)
                except Exception:
                    action_id = -1
            if action_id in (9, 10, 92, 216, 247):
                self.close()

        def close(self):
            self._closed = True
            try:
                super(QRDialog, self).close()
            except Exception:
                pass
else:
    class QRDialog(object):  # type: ignore
        def __init__(self, *args, **kwargs):
            self._closed = True
            self._setup_ok = False
        def show(self):
            return None
        def close(self):
            self._closed = True


def render_qr_png(data, dest_path, size=512):
    """Render a QR to PNG using the vendored pure-Python PNG backend."""
    try:
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        if qrcode is None or PyPNGImage is None:
            return None
        qr = qrcode.QRCode(error_correction=qrcode.constants.ERROR_CORRECT_M,
                           box_size=10, border=4)
        qr.add_data(data)
        qr.make(fit=True)
        img = qr.make_image(image_factory=PyPNGImage)
        img.save(dest_path)
        return dest_path
    except Exception as exc:
        Kodi.log("QR", "Falha ao gerar QR: %s" % exc, level="error")
        Kodi.notify("Falha ao gerar o QR code.")
        return None


def show_qr_dialog(title, hint, payload, completion_event=None, timeout_s=None):
    """Show the QR non-modally while keeping a small Kodi event loop alive.

    ``completion_event`` lets pairing/login close the QR automatically as soon
    as the phone has completed the operation.
    """
    if xbmcgui is None or xbmc is None:
        return False
    profile = Kodi.profile_path()
    path = os.path.join(profile, "qr.png")
    image = render_qr_png(payload, path)
    if not image:
        Kodi.ok_dialog(ADDON_TITLE, "Não foi possível gerar o QR code:\n%s" % payload)
        return False
    dialog = QRDialog(title, hint, image)
    if not getattr(dialog, "_setup_ok", True):
        return False
    started = time.time()
    try:
        dialog.show()
        while not dialog._closed:
            if completion_event is not None and completion_event.is_set():
                dialog.close()
                break
            if timeout_s is not None and time.time() - started >= float(timeout_s):
                dialog.close()
                break
            try:
                if xbmc.Monitor().abortRequested():
                    dialog.close()
                    break
            except Exception:
                pass
            xbmc.sleep(100)
    finally:
        dialog.close()
        try:
            del dialog
        except Exception:
            pass
    return True


__all__ = ["render_qr_png", "QRDialog", "show_qr_dialog"]
