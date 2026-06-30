# NiceGUI: корректное копирование в буфер обмена по кнопке

Эта инструкция описывает рабочий паттерн для текущего стека FastAPI + NiceGUI. Используйте его, если нужно сделать кнопку `Copy`, которая действительно пишет текст в clipboard, а не только показывает уведомление.

## Проблема

Не используйте такой подход:

```python
def copy_text():
    ui.run_javascript(f"navigator.clipboard.writeText({json.dumps(text)})")
    ui.notify("Copied", type="positive")

copy_btn.on_click(copy_text)
```

Почему это ненадежно:

- `ui.run_javascript` выполняется после server roundtrip, а не внутри исходного browser click event.
- Браузер может отклонить запись в clipboard, потому что потеряна user activation.
- `navigator.clipboard.writeText` доступен только в secure context: `https` или `localhost`; на обычном `http://<ip>:<port>` он часто недоступен.
- Уведомление `Copied` показывается сразу и не связано с реальным результатом записи.

## Правильный принцип

Запись в clipboard должна выполняться в клиентском JS handler кнопки:

```python
copy_btn.on(
    "click",
    notify_copy_result,
    js_handler="""
    async () => {
      const result = await window.myClipboard.copy('generated');
      emit(result);
    }
    """,
)
```

Серверный Python handler должен только принять результат JS и показать уведомление после фактической попытки копирования.

## Рекомендуемая реализация в этом проекте

В проекте уже есть reusable helper:

- `web/clipboard_ui.py`
- функция `install_clipboard_script()`
- функция `set_clipboard_text(key, text)`
- функция `bind_copy_button(button, key=..., success_message=..., empty_message=..., failure_message=...)`

Подключение на странице NiceGUI:

```python
from web.clipboard_ui import bind_copy_button, install_clipboard_script, set_clipboard_text


def create_ui():
    install_clipboard_script()
    generated_clipboard_key = "generated_omentity"

    copy_btn = ui.button("Copy", icon="content_copy")

    bind_copy_button(
        copy_btn,
        key=generated_clipboard_key,
        success_message="Generated OMEntity copied",
        empty_message="Nothing to copy",
        failure_message="Clipboard copy failed",
    )
```

Когда текст готов или обновился, синхронизируйте его с JS-состоянием страницы:

```python
state.generated_text = result.generated_text
set_clipboard_text(generated_clipboard_key, result.generated_text)
```

## Как работает helper

Кнопка вызывает клиентский JS прямо внутри click event.

Helper сначала пробует современный API:

```javascript
await navigator.clipboard.writeText(text)
```

Если API недоступен или упал, используется fallback:

```javascript
document.execCommand('copy')
```

Fallback создает временный скрытый `textarea`, выделяет в нем текст и копирует выделение. Это важно для случаев, когда приложение открыто по обычному HTTP/IP адресу, например:

```text
http://172.17.128.1:8025
```

## Уведомления

Не показывайте `Copied` заранее. Уведомление должно зависеть от результата JS:

- `{ok: true}` -> positive notify.
- `{ok: false, reason: "empty"}` -> warning notify.
- другая ошибка -> negative notify с причиной.

Именно поэтому `bind_copy_button` вызывает `emit(result)` из JS handler и обрабатывает результат на Python-стороне.

## Отдельный стенд для проверки

Для изолированной проверки рядом с основным сервисом есть стенд:

```bash
python -m web.clipboard_lab --host 127.0.0.1 --port 8021
```

Если Playwright запускается из WSL, а сервер из Windows venv, может понадобиться:

```bash
python -m web.clipboard_lab --host 0.0.0.0 --port 8021
```

и открывать адрес Windows host, видимый из WSL, например:

```text
http://172.17.128.1:8021
```

## Как проверять

Недостаточно проверить, что появился popup `Copied`. Нужно проверить фактическую вставку.

Минимальный Playwright-сценарий:

```javascript
async (page) => {
  await page.goto('http://admin:secret@172.17.128.1:8025');
  const sample = 'COPY PROBE';

  await page.waitForFunction(() => Boolean(window.afDagsHelperClipboard));
  await page.evaluate(
    sample => window.afDagsHelperClipboard.setText('generated_omentity', sample),
    sample,
  );

  await page.getByRole('button', {name: 'Copy'}).click();

  await page.evaluate(() => {
    const textarea = document.createElement('textarea');
    textarea.id = 'paste-target';
    document.body.appendChild(textarea);
    textarea.focus();
  });

  await page.keyboard.press('Control+V');
  await page.waitForTimeout(300);

  return await page.evaluate(() => document.getElementById('paste-target').value);
}
```

Ожидаемый результат:

```text
COPY PROBE
```

## Regression test

В unit-тестах проверяйте, что HTML страницы содержит клиентский clipboard helper:

```python
self.assertIn("afDagsHelperClipboard", response.text)
self.assertIn("navigator.clipboard.writeText", response.text)
self.assertIn("document.execCommand('copy')", response.text)
```

Такой тест не заменяет browser smoke, но защищает от возврата к server-side `ui.run_javascript(...writeText...)`.

## Чеклист для агента

1. Не использовать `ui.run_javascript("navigator.clipboard.writeText(...)")` как основной механизм Copy.
2. Выполнять запись в clipboard внутри `js_handler` кнопки.
3. Синхронизировать копируемый текст в JS-состояние страницы до клика.
4. Показывать `Copied` только после успешного результата JS handler.
5. Поддерживать fallback через `document.execCommand('copy')` для HTTP/IP окружений.
6. Проверять не popup, а фактическую вставку через `Ctrl+V`.
7. Для UI/UX изменений обновлять справку в интерфейсе.
