# browse-api-client

Install from GitHub:

```bash
pip install git+https://github.com/CapellaSoftware/browse-api-client.git
```

Install locally for development:

```bash
pip install -e /Users/rrabata/Workspace/app-agents/libs/browse-api-client
```

Usage:

```python
from browse_api_client import RemoteBrowserSession

async with RemoteBrowserSession(
    base_url="http://localhost:8000",
    viewport_width=1440,
    viewport_height=900,
) as session:
    page = await session.new_page()
    await page.goto("https://example.com")
    screenshot = await session.screenshot()
    clicked = await session.click_at(100, 200)
```

Convenience methods:

- `await session.screenshot(...)` captures the current page by default and returns bytes.
- `await session.click_at(x, y, ...)` clicks viewport coordinates and returns metadata for the clicked element.
- `await session.set_viewport_size(width, height)` changes the active page size and sets the default for future pages.
- Browser helper scripts are installed automatically when the session starts and when new pages are created.
