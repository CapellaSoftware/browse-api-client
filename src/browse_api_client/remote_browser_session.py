"""Client wrapper for the remote browser HTTP/CDP lifecycle."""

from __future__ import annotations

import asyncio
import os
import logging
from pathlib import Path
from typing import Any, Optional

import aiohttp
from patchright.async_api import Browser, BrowserContext, Page, Playwright, async_playwright

_HELPER_SCRIPT = """
(() => {
  if (window.__browseApiClientInstalled) {
    return;
  }

  window.__browseApiClientInstalled = true;
  window.__browseApiClient = {
    escapeCss(value) {
      if (window.CSS && typeof window.CSS.escape === "function") {
        return window.CSS.escape(value);
      }
      return String(value).replace(/[^a-zA-Z0-9_-]/g, "\\$&");
    },
    getElementSelectors(element) {
      const selectors = [];
      if (!element || !(element instanceof Element)) {
        return selectors;
      }

      const escapedTag = element.tagName.toLowerCase();
      const add = (value) => {
        if (value && !selectors.includes(value)) {
          selectors.push(value);
        }
      };

      if (element.id) {
        add(`#${window.__browseApiClient.escapeCss(element.id)}`);
      }

      const name = element.getAttribute("name");
      if (name) {
        add(`${escapedTag}[name="${name.replace(/"/g, '\\"')}"]`);
      }

      const type = element.getAttribute("type");
      if (type) {
        add(`${escapedTag}[type="${type.replace(/"/g, '\\"')}"]`);
      }

      const ariaLabel = element.getAttribute("aria-label");
      if (ariaLabel) {
        add(`${escapedTag}[aria-label="${ariaLabel.replace(/"/g, '\\"')}"]`);
      }

      const placeholder = element.getAttribute("placeholder");
      if (placeholder) {
        add(`${escapedTag}[placeholder="${placeholder.replace(/"/g, '\\"')}"]`);
      }

      const role = element.getAttribute("role");
      if (role) {
        add(`${escapedTag}[role="${role.replace(/"/g, '\\"')}"]`);
      }

      const classNames = Array.from(element.classList || []).slice(0, 3);
      if (classNames.length) {
        add(`${escapedTag}.${classNames.map((name) => window.__browseApiClient.escapeCss(name)).join(".")}`);
      }

      const path = [];
      let current = element;
      while (current && current.nodeType === Node.ELEMENT_NODE && path.length < 5) {
        let segment = current.tagName.toLowerCase();
        if (current.id) {
          segment += `#${window.__browseApiClient.escapeCss(current.id)}`;
          path.unshift(segment);
          break;
        }
        const siblingIndex =
          current.parentElement
            ? Array.from(current.parentElement.children).filter((child) => child.tagName === current.tagName).indexOf(current) + 1
            : 1;
        segment += `:nth-of-type(${siblingIndex})`;
        path.unshift(segment);
        current = current.parentElement;
      }
      if (path.length) {
        add(path.join(" > "));
      }

      return selectors.slice(0, 8);
    },
    markClickAt(x, y) {
      const existing = document.getElementById("__browse_api_client_click_marker");
      if (existing) {
        existing.remove();
      }

      const marker = document.createElement("div");
      marker.id = "__browse_api_client_click_marker";
      marker.setAttribute("aria-hidden", "true");
      Object.assign(marker.style, {
        position: "fixed",
        left: `${x - 16}px`,
        top: `${y - 16}px`,
        width: "32px",
        height: "32px",
        border: "3px solid #ff2d20",
        borderRadius: "9999px",
        background: "rgba(255, 45, 32, 0.12)",
        boxSizing: "border-box",
        pointerEvents: "none",
        zIndex: "2147483647",
        boxShadow: "0 0 0 2px rgba(255,255,255,0.9)",
      });
      document.documentElement.appendChild(marker);
    },
    describeElementAtPoint(x, y) {
      const element = document.elementFromPoint(x, y);
      if (!element) {
        return null;
      }

      const rect = element.getBoundingClientRect();
      const text = (element.innerText || element.textContent || "").trim().slice(0, 500);
      const attrs = {};
      for (const name of ["id", "class", "name", "type", "role", "aria-label", "href", "src", "title"]) {
        const value = element.getAttribute(name);
        if (value) {
          attrs[name] = value;
        }
      }

      return {
        tagName: element.tagName,
        text,
        attributes: attrs,
        selectorCandidates: window.__browseApiClient.getElementSelectors(element),
        outerHtml: element.outerHTML.slice(0, 1000),
        boundingBox: {
          x: rect.x,
          y: rect.y,
          width: rect.width,
          height: rect.height,
        },
      };
    },
  };
})();
"""

DEFAULT_BROWSER_SERVICE_URL = os.getenv("BASE_BROWSER_SERVICE_URL", "http://localhost:8000")
DEFAULT_PLAYWRIGHT_TIMEOUT_MS = 5000
logger = logging.getLogger(__name__)
logger.warning("browse_api_client.remote_browser_session.module_loaded")


class RemoteBrowserSession:
    """Async context manager for a remote browser session."""

    def __init__(
        self,
        base_url: str = DEFAULT_BROWSER_SERVICE_URL,
        guid: Optional[str] = None,
        request_timeout: int = 30,
        viewport_width: int = 1600,
        viewport_height: int = 900,
    ) -> None:
        fallback_base_url = os.getenv("BASE_BROWSER_SERVICE_URL", DEFAULT_BROWSER_SERVICE_URL)
        normalized_base_url = (base_url or fallback_base_url).strip() or fallback_base_url
        if not normalized_base_url.startswith(("http://", "https://")):
            normalized_base_url = fallback_base_url
        self.base_url = normalized_base_url.rstrip("/")
        self.guid = guid
        self.request_timeout = request_timeout
        self.connect_timeout = request_timeout
        self.playwright_timeout_ms = DEFAULT_PLAYWRIGHT_TIMEOUT_MS
        self.viewport_width = viewport_width
        self.viewport_height = viewport_height
        self.ws_url: Optional[str] = None
        self._connect_headers: Optional[dict] = None

        self._http_session: Optional[aiohttp.ClientSession] = None
        self._playwright_cm: Any = None
        self._playwright: Optional[Playwright] = None
        self.browser: Optional[Browser] = None
        self.page: Optional[Page] = None

    async def __aenter__(self) -> "RemoteBrowserSession":
        logger.warning(
            "browse_api_client.remote_browser_session.enter.start base_url=%s guid=%s viewport=%sx%s timeout=%s",
            self.base_url,
            self.guid,
            self.viewport_width,
            self.viewport_height,
            self.request_timeout,
        )
        timeout = aiohttp.ClientTimeout(total=self.request_timeout)
        self._http_session = aiohttp.ClientSession(timeout=timeout)

        payload = {"guid": self.guid} if self.guid else None
        logger.warning(
            "browse_api_client.remote_browser_session.enter.create_request url=%s/api/remote-browser/create",
            self.base_url,
        )
        try:
            async with self._http_session.post(
                f"{self.base_url}/api/remote-browser/create",
                json=payload,
            ) as response:
                response.raise_for_status()
                data = await response.json()
        except Exception:
            logger.exception("browse_api_client.remote_browser_session.enter.create_request_failed")
            raise

        self.guid = data["guid"]
        self.ws_url = data["ws_url"]
        logger.warning(
            "browse_api_client.remote_browser_session.enter.create_request_done guid=%s ws_url=%s",
            self.guid,
            self.ws_url,
        )
        try:
            self._connect_headers = await self._mint_connect_headers()
        except Exception:
            logger.exception("browse_api_client.remote_browser_session.enter.connect_token_failed")
            raise

        self._playwright_cm = async_playwright()
        logger.warning("browse_api_client.remote_browser_session.enter.playwright_start")
        try:
            self._playwright = await self._playwright_cm.__aenter__()
        except Exception:
            logger.exception("browse_api_client.remote_browser_session.enter.playwright_start_failed")
            raise

        logger.warning(
            "browse_api_client.remote_browser_session.enter.connect_over_cdp_start ws_url=%s timeout=%s",
            self.ws_url,
            self.connect_timeout,
        )
        try:
            connect_options = {"headers": self._connect_headers} if self._connect_headers else {}
            self.browser = await asyncio.wait_for(
                self._playwright.chromium.connect_over_cdp(self.ws_url, **connect_options),
                timeout=self.connect_timeout,
            )
        except Exception:
            logger.exception("browse_api_client.remote_browser_session.enter.connect_over_cdp_failed")
            raise
        logger.warning("browse_api_client.remote_browser_session.enter.connect_over_cdp_done")

        logger.warning("browse_api_client.remote_browser_session.enter.install_scripts_start")
        try:
            await self.install_scripts()
        except Exception:
            logger.exception("browse_api_client.remote_browser_session.enter.install_scripts_failed")
            raise
        if self.browser is not None and self.browser.contexts:
            pages = self.browser.contexts[0].pages
            if pages:
                self.page = pages[-1]
                logger.warning("browse_api_client.remote_browser_session.enter.bound_existing_page page_count=%s", len(pages))
        logger.warning("browse_api_client.remote_browser_session.enter.done guid=%s", self.guid)
        return self

    async def _mint_connect_headers(self) -> Optional[dict]:
        """Single-use CDP handshake bearer from the Browser API mux; None when the mux is off (409)."""
        assert self._http_session is not None
        async with self._http_session.post(
            f"{self.base_url}/api/remote-browser/{self.guid}/connect-token"
        ) as response:
            if response.status == 409:
                return None
            response.raise_for_status()
            data = await response.json()
        token = str(data.get("token") or "").strip()
        if not token:
            raise RuntimeError("Browser API returned an invalid connect token response.")
        return {"Authorization": f"Bearer {token}"}

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def close(self) -> None:
        logger.warning("browse_api_client.remote_browser_session.close.start guid=%s", self.guid)
        self.page = None

        if self.browser is not None:
            logger.warning("browse_api_client.remote_browser_session.close.browser_close_start guid=%s", self.guid)
            await self.browser.close()
            self.browser = None
            logger.warning("browse_api_client.remote_browser_session.close.browser_close_done")

        if self._playwright_cm is not None:
            logger.warning("browse_api_client.remote_browser_session.close.playwright_stop_start")
            await self._playwright_cm.__aexit__(None, None, None)
            self._playwright_cm = None
            self._playwright = None
            logger.warning("browse_api_client.remote_browser_session.close.playwright_stop_done")

        if self._http_session is not None:
            if self.guid is not None:
                try:
                    logger.warning(
                        "browse_api_client.remote_browser_session.close.delete_request_start url=%s/api/remote-browser/%s",
                        self.base_url,
                        self.guid,
                    )
                    async with self._http_session.delete(
                        f"{self.base_url}/api/remote-browser/{self.guid}"
                    ) as response:
                        if response.status not in (200, 404):
                            response.raise_for_status()
                    logger.warning("browse_api_client.remote_browser_session.close.delete_request_done")
                finally:
                    self.guid = None
                    self.ws_url = None

            await self._http_session.close()
            self._http_session = None
        logger.warning("browse_api_client.remote_browser_session.close.done")

    async def new_page(self) -> Page:
        logger.warning("browse_api_client.remote_browser_session.new_page.start")
        page = await self.context.new_page()
        self._apply_page_timeouts(page)
        await self.set_viewport_size(self.viewport_width, self.viewport_height, page=page)
        await self._install_page_scripts(page)
        self.page = page
        logger.warning("browse_api_client.remote_browser_session.new_page.done")
        return page

    @property
    def context(self) -> BrowserContext:
        if self.browser is None:
            raise RuntimeError("RemoteBrowserSession is not connected. Use 'async with RemoteBrowserSession(...)'.")
        contexts = self.browser.contexts
        if not contexts:
            raise RuntimeError("Remote browser has no active contexts.")
        return contexts[0]

    async def browser_info(self) -> dict[str, Any]:
        if self._http_session is None or self.guid is None:
            raise RuntimeError("RemoteBrowserSession is not connected. Use 'async with RemoteBrowserSession(...)'.")

        async with self._http_session.get(
            f"{self.base_url}/api/remote-browser/{self.guid}"
        ) as response:
            response.raise_for_status()
            return await response.json()

    async def install_scripts(self) -> None:
        """Install helper scripts into the remote browser context."""
        logger.warning("browse_api_client.remote_browser_session.install_scripts.start")
        await self.context.add_init_script(script=_HELPER_SCRIPT)
        for page in self.context.pages:
            self._apply_page_timeouts(page)
            await self.set_viewport_size(self.viewport_width, self.viewport_height, page=page)
            await self._install_page_scripts(page)
        logger.warning("browse_api_client.remote_browser_session.install_scripts.done page_count=%s", len(self.context.pages))

    async def set_viewport_size(
        self,
        width: int,
        height: int,
        page: Optional[Page] = None,
    ) -> dict[str, int]:
        """Set the viewport size for the selected page or the current session page."""
        if width <= 0 or height <= 0:
            raise ValueError("Viewport width and height must be positive integers.")

        self.viewport_width = width
        self.viewport_height = height
        target_page = self._require_page(page) if page is not None or self.page is not None else None

        if target_page is not None:
            await target_page.set_viewport_size({"width": width, "height": height})

        return {"width": width, "height": height}

    async def screenshot(
        self,
        page: Optional[Page] = None,
        path: Optional[str] = None,
        full_page: bool = True,
        image_type: str = "png",
    ) -> bytes:
        """Capture a screenshot from the selected page or the current session page."""
        target_page = self._require_page(page)
        screenshot_kwargs: dict[str, Any] = {
            "full_page": full_page,
            "type": image_type,
            "scale": "css",
        }
        data = await target_page.screenshot(**screenshot_kwargs)
        if path is not None:
            output_path = Path(path)
            await asyncio.to_thread(output_path.parent.mkdir, parents=True, exist_ok=True)
            await asyncio.to_thread(output_path.write_bytes, data)
        return data

    async def click_at(
        self,
        x: float,
        y: float,
        page: Optional[Page] = None,
        button: str = "left",
        click_count: int = 1,
        delay: float = 0,
    ) -> dict[str, Any]:
        """Click viewport coordinates and return a description of the clicked element."""
        target_page = self._require_page(page)
        await self._install_page_scripts(target_page)
        viewport_size = target_page.viewport_size or {"width": self.viewport_width, "height": self.viewport_height}
        element_info = await target_page.evaluate(
            """
            ({ x, y }) => {
              if (!window.__browseApiClient) {
                return null;
              }
              return window.__browseApiClient.describeElementAtPoint(x, y);
            }
            """,
            {"x": x, "y": y},
        )
        await target_page.mouse.click(x, y, button=button, click_count=click_count, delay=delay)
        await target_page.evaluate(
            """
            ({ x, y }) => {
              if (window.__browseApiClient?.markClickAt) {
                window.__browseApiClient.markClickAt(x, y);
              }
            }
            """,
            {"x": x, "y": y},
        )
        return {
            "x": x,
            "y": y,
            "input_x": x,
            "input_y": y,
            "button": button,
            "click_count": click_count,
            "viewport": viewport_size,
            "element": element_info,
        }

    async def scroll_page(
        self,
        direction: str = "down",
        page_lengths: float = 1.0,
        overlap_ratio: float = 0.15,
        page: Optional[Page] = None,
    ) -> dict[str, Any]:
        """Scroll by one or more viewport lengths with configurable overlap."""
        target_page = self._require_page(page)
        normalized_direction = direction.strip().lower()
        if normalized_direction not in {"up", "down"}:
            raise ValueError("direction must be 'up' or 'down'.")
        if page_lengths <= 0:
            raise ValueError("page_lengths must be positive.")
        if not 0 <= overlap_ratio < 1:
            raise ValueError("overlap_ratio must be between 0 and 1.")

        viewport_size = target_page.viewport_size or {"width": self.viewport_width, "height": self.viewport_height}
        viewport_height = viewport_size["height"]
        delta = viewport_height * page_lengths * (1 - overlap_ratio)
        if normalized_direction == "up":
            delta *= -1

        position = await target_page.evaluate(
            """
            async ({ deltaY }) => {
              window.scrollBy({ top: deltaY, left: 0, behavior: "instant" });
              await new Promise((resolve) => setTimeout(resolve, 200));
              return {
                scrollX: window.scrollX,
                scrollY: window.scrollY,
                innerWidth: window.innerWidth,
                innerHeight: window.innerHeight,
                documentHeight: document.documentElement.scrollHeight,
              };
            }
            """,
            {"deltaY": delta},
        )
        return {
            "direction": normalized_direction,
            "page_lengths": page_lengths,
            "overlap_ratio": overlap_ratio,
            "deltaY": delta,
            "position": position,
        }

    def _require_page(self, page: Optional[Page]) -> Page:
        target_page = page or self.page
        if target_page is None:
            raise RuntimeError("No active page. Call 'await session.new_page()' first or pass a page explicitly.")
        return target_page

    async def _install_page_scripts(self, page: Page) -> None:
        logger.warning("browse_api_client.remote_browser_session.install_page_scripts.start")
        await page.evaluate(_HELPER_SCRIPT)
        logger.warning("browse_api_client.remote_browser_session.install_page_scripts.done")

    def _apply_page_timeouts(self, page: Page) -> None:
        page.set_default_timeout(self.playwright_timeout_ms)
        page.set_default_navigation_timeout(self.playwright_timeout_ms)
