"""Small NiceGUI clipboard test page used for browser-level verification."""

import argparse

from nicegui import ui

from web.clipboard_ui import bind_copy_button, install_clipboard_script, set_clipboard_text


SAMPLE_TEXT = "AF DAGs Helper clipboard probe: generated OMEntity text"
CLIPBOARD_KEY = "clipboard_lab_sample"


@ui.page("/")
def index():
    install_clipboard_script()
    ui.label("Clipboard Lab").classes("text-h5")
    ui.label("This page exercises the same NiceGUI copy helper as the main app.").classes("text-caption")
    ui.textarea("Text to copy", value=SAMPLE_TEXT).props("readonly rows=4").classes("w-full max-w-2xl")
    copy_button = ui.button("Copy", icon="content_copy").props("data-testid=clipboard-lab-copy")
    status = ui.label("Waiting for copy").classes("text-caption")

    def mark_clicked():
        status.set_text("Copy handler returned")

    bind_copy_button(
        copy_button,
        key=CLIPBOARD_KEY,
        success_message="Clipboard lab text copied",
        empty_message="Clipboard lab text is empty",
        failure_message="Clipboard lab copy failed",
    )
    copy_button.on_click(mark_clicked)
    ui.timer(0.1, lambda: set_clipboard_text(CLIPBOARD_KEY, SAMPLE_TEXT), once=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8021)
    args = parser.parse_args()
    ui.run(host=args.host, port=args.port, title="Clipboard Lab", reload=False)


if __name__ in {"__main__", "__mp_main__"}:
    main()
