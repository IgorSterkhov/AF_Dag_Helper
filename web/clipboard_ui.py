"""Client-side clipboard helpers for NiceGUI buttons."""

import json

from nicegui import ui
from nicegui import core


CLIPBOARD_SCRIPT = """
<script>
window.afDagsHelperClipboard = window.afDagsHelperClipboard || {
  texts: {},
  setText(key, value) {
    this.texts[key] = value || '';
  },
  async copy(key) {
    const text = this.texts[key] || '';
    if (!text) {
      return {ok: false, reason: 'empty'};
    }
    if (window.isSecureContext && navigator.clipboard && navigator.clipboard.writeText) {
      try {
        await navigator.clipboard.writeText(text);
        return {ok: true, method: 'clipboard-api'};
      } catch (error) {
        const fallback = this.copyWithSelection(text);
        if (fallback.ok) {
          return fallback;
        }
        return {
          ok: false,
          reason: 'error',
          message: error && error.message ? error.message : String(error),
        };
      }
    }
    return this.copyWithSelection(text);
  },
  copyWithSelection(text) {
    const textarea = document.createElement('textarea');
    textarea.value = text;
    textarea.setAttribute('readonly', '');
    textarea.style.position = 'fixed';
    textarea.style.left = '-9999px';
    textarea.style.top = '0';
    textarea.style.opacity = '0';
    document.body.appendChild(textarea);
    textarea.focus();
    textarea.select();
    textarea.setSelectionRange(0, textarea.value.length);
    try {
      const copied = document.execCommand('copy');
      return copied
        ? {ok: true, method: 'selection-fallback'}
        : {ok: false, reason: 'fallback-failed'};
    } catch (error) {
      return {
        ok: false,
        reason: 'fallback-error',
        message: error && error.message ? error.message : String(error),
      };
    } finally {
      document.body.removeChild(textarea);
    }
  },
};
</script>
"""


def install_clipboard_script() -> None:
    ui.add_head_html(CLIPBOARD_SCRIPT)


def set_clipboard_text(key: str, text: str) -> None:
    if core.loop is None:
        return
    ui.run_javascript(
        "window.afDagsHelperClipboard && "
        f"window.afDagsHelperClipboard.setText({json.dumps(key)}, {json.dumps(text or '')})"
    )


def bind_copy_button(
    button,
    *,
    key: str,
    success_message: str,
    empty_message: str,
    failure_message: str,
) -> None:
    def notify_copy_result(event):
        result = event.args
        if isinstance(result, list) and result:
            result = result[0]
        if not isinstance(result, dict):
            ui.notify(failure_message, type="negative", close_button=True)
            return
        if result.get("ok"):
            ui.notify(success_message, type="positive")
            return
        if result.get("reason") == "empty":
            ui.notify(empty_message, type="warning")
            return
        detail = result.get("message") or result.get("reason") or "unknown error"
        ui.notify(f"{failure_message}: {detail}", type="negative", close_button=True)

    js_key = json.dumps(key)
    button.on(
        "click",
        notify_copy_result,
        js_handler=f"""
        async () => {{
          const result = window.afDagsHelperClipboard
            ? await window.afDagsHelperClipboard.copy({js_key})
            : {{ok: false, reason: 'helper-missing'}};
          emit(result);
        }}
        """,
    )
