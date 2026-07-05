import asyncio
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

# WALLET FINDER edition — output to local file
OUT_PATH = Path(__file__).resolve().parent / "leaderboard_wallets.txt"
ADDR_RE = re.compile(r"0x[a-fA-F0-9]{40}")
VALID_RE = re.compile(r"^0x[a-f0-9]{40}$")
TARGET_COUNT = 2000
TIME_LIMIT_SECONDS = 300
NETWORK_HINTS = ("leaderboard", "rpc", "info", "explorer")


def add_address(addr, seen, ordered):
    addr = addr.lower()
    if VALID_RE.fullmatch(addr) and addr not in seen:
        seen.add(addr)
        ordered.append(addr)


def walk_json(obj, seen, ordered):
    if isinstance(obj, dict):
        for key, value in obj.items():
            if isinstance(value, str):
                if key.lower() in {"address", "wallet", "user", "account", "walletaddress"}:
                    add_address(value, seen, ordered)
                for match in ADDR_RE.findall(value):
                    add_address(match, seen, ordered)
            else:
                walk_json(value, seen, ordered)
    elif isinstance(obj, list):
        for item in obj:
            walk_json(item, seen, ordered)
    elif isinstance(obj, str):
        for match in ADDR_RE.findall(obj):
            add_address(match, seen, ordered)


def extract_from_text(text, seen, ordered):
    for match in ADDR_RE.findall(text or ""):
        add_address(match, seen, ordered)
    try:
        walk_json(json.loads(text), seen, ordered)
    except Exception:
        pass


def request_json(url, params, token):
    query = urllib.parse.urlencode(params)
    headers = {
        "accept": "application/json,text/plain,*/*",
        "user-agent": "Mozilla/5.0",
    }
    if token:
        headers["authorization"] = f"Bearer {token}"
    req = urllib.request.Request(f"{url}?{query}", headers=headers)
    with urllib.request.urlopen(req, timeout=25) as resp:
        return resp.read().decode("utf-8", "replace")


def primary_route():
    seen = set()
    ordered = []
    token = (
        os.environ.get("HYPERTRACKER_API_TOKEN")
        or os.environ.get("CMM_API_TOKEN")
        or os.environ.get("COINMARKETMAN_API_TOKEN")
    )
    if not token:
        return [], "no_token"
    base = "https://ht-api.coinmarketman.com/api/external/leaderboards/perp-pnl"
    last_error = None
    for offset in range(0, TARGET_COUNT, 100):
        params = {
            "offset": offset, "limit": 100, "order": "desc",
            "orderBy": "pnlAllTime", "rankBy": "pnlAllTime",
        }
        try:
            text = request_json(base, params, token)
        except urllib.error.HTTPError as exc:
            last_error = f"http_{exc.code}"
            if exc.code in {401, 403}:
                return [], last_error
            break
        except Exception as exc:
            last_error = type(exc).__name__
            break
        before = len(ordered)
        extract_from_text(text, seen, ordered)
        if len(ordered) >= TARGET_COUNT:
            break
        if len(ordered) == before:
            break
    return ordered[:TARGET_COUNT], last_error


async def secondary_route():
    from playwright.async_api import async_playwright
    seen = set()
    ordered = []
    started = time.monotonic()
    response_errors = []
    response_tasks = set()

    def time_left():
        return time.monotonic() - started < TIME_LIMIT_SECONDS

    def target_left():
        return len(ordered) < TARGET_COUNT

    def can_continue():
        return time_left() and target_left()

    async def settle(page, millis=1200):
        try:
            await page.wait_for_load_state("networkidle", timeout=5000)
        except Exception:
            pass
        await page.wait_for_timeout(millis)

    async def click_first_visible(page, locators, max_clicks=1):
        clicks = 0
        for locator in locators:
            if not can_continue() or clicks >= max_clicks:
                break
            try:
                count = await locator.count()
            except Exception:
                continue
            for i in range(min(count, 8)):
                if not can_continue() or clicks >= max_clicks:
                    break
                item = locator.nth(i)
                try:
                    if await item.is_visible() and await item.is_enabled():
                        await item.click(timeout=1500)
                        clicks += 1
                        await settle(page, 800)
                except Exception:
                    continue
        return clicks

    async def click_label(page, label):
        safe = re.escape(label)
        locators = [
            page.get_by_role("tab", name=re.compile(safe, re.I)),
            page.get_by_role("button", name=re.compile(safe, re.I)),
            page.get_by_text(re.compile(rf"^\s*{safe}\s*$", re.I)),
            page.locator(f"[aria-label*='{label}' i], [title*='{label}' i]"),
        ]
        return await click_first_visible(page, locators)

    async def open_period_menus(page):
        labels = ("Period", "Time", "All-time", "All Time", "30D", "7D", "24H")
        for label in labels:
            if not can_continue():
                break
            await click_label(page, label)

    async def scroll_and_paginate(page):
        last_count = len(ordered)
        stale_pages = 0
        for page_index in range(11):
            if not can_continue():
                break
            for _ in range(6):
                if not can_continue():
                    break
                await page.mouse.wheel(0, 2200)
                await page.wait_for_timeout(450)
                try:
                    await page.evaluate(
                        """
                        () => {
                          window.scrollTo(0, document.body.scrollHeight);
                          for (const el of document.querySelectorAll('*')) {
                            if (el.scrollHeight > el.clientHeight + 100) {
                              el.scrollTop = el.scrollHeight;
                            }
                          }
                        }
                        """
                    )
                except Exception:
                    pass
                await page.wait_for_timeout(550)
            await settle(page, 700)
            if len(ordered) == last_count:
                stale_pages += 1
            else:
                stale_pages = 0
                last_count = len(ordered)
            if page_index >= 10 or stale_pages >= 2:
                break
            next_like = [
                page.get_by_role("button", name=re.compile(r"next|>", re.I)),
                page.get_by_role("link", name=re.compile(r"next|>", re.I)),
                page.locator(
                    "button[aria-label*='next' i], button[title*='next' i], "
                    "a[aria-label*='next' i], a[title*='next' i]"
                ),
                page.locator("button:has-text('Next'), a:has-text('Next')"),
            ]
            clicked = await click_first_visible(page, next_like)
            if not clicked:
                break

    async def capture_view(page, name):
        before = len(ordered)
        await settle(page, 1200)
        await scroll_and_paginate(page)
        return len(ordered) - before

    async def fetch_frontend_leaderboard(page):
        url = "https://stats-data.hyperliquid.xyz/Mainnet/leaderboard"
        try:
            text = await page.evaluate(
                """
                async (url) => {
                  const response = await fetch(url, { cache: "no-store" });
                  if (!response.ok) throw new Error(`status_${response.status}`);
                  return await response.text();
                }
                """,
                url,
            )
            extract_from_text(text, seen, ordered)
            return True
        except Exception as exc:
            response_errors.append(f"frontend_leaderboard:{type(exc).__name__}")
            return False

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page(
            viewport={"width": 1440, "height": 1200},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125 Safari/537.36"
            ),
        )

        async def handle_response(response):
            url = response.url.lower()
            request_body = ""
            try:
                request_body = response.request.post_data or ""
            except Exception:
                pass
            request_body_lower = request_body.lower()
            if not any(part in url or part in request_body_lower for part in NETWORK_HINTS):
                return
            try:
                body = await response.text()
            except Exception as exc:
                response_errors.append(f"{type(exc).__name__}:{response.url}")
                return
            extract_from_text(body, seen, ordered)

        def track_response(response):
            task = asyncio.create_task(handle_response(response))
            response_tasks.add(task)
            task.add_done_callback(response_tasks.discard)

        page.on("response", track_response)
        await page.goto("https://app.hyperliquid.xyz/leaderboard", wait_until="domcontentloaded", timeout=60000)
        await fetch_frontend_leaderboard(page)
        await capture_view(page, "default")
        periods = ("All-time", "All Time", "30D", "30d", "Month", "7D", "7d", "Week", "24H", "24h", "Day")
        for period in periods:
            if not can_continue():
                break
            await open_period_menus(page)
            clicked = await click_label(page, period)
            if clicked:
                await capture_view(page, period)
        sortable_columns = ("PNL", "ROI", "Volume", "Account Value")
        for column in sortable_columns:
            if not can_continue():
                break
            clicked = await click_label(page, column)
            if clicked:
                await capture_view(page, column)
        if response_tasks:
            await asyncio.wait(response_tasks, timeout=30)
        await page.wait_for_timeout(1000)
        await browser.close()
    if not ordered and response_errors:
        return [], response_errors[0]
    return ordered[:TARGET_COUNT], None


def write_output(addresses):
    bad = [addr for addr in addresses if not VALID_RE.fullmatch(addr)]
    if bad:
        raise ValueError(f"validation_failed:{bad[0]}")
    if len(addresses) != len(set(addresses)):
        raise ValueError("validation_failed:duplicates")
    OUT_PATH.write_text("\n".join(addresses) + ("\n" if addresses else ""), encoding="utf-8")
    if not OUT_PATH.exists():
        raise ValueError("validation_failed:missing_output")
    lines = OUT_PATH.read_text(encoding="utf-8").splitlines()
    if lines != addresses:
        raise ValueError("validation_failed:write_mismatch")


async def main():
    addresses, primary_error = primary_route()
    route = "primary"
    if not addresses:
        route = "secondary"
        try:
            addresses, secondary_error = await secondary_route()
        except Exception as exc:
            secondary_error = type(exc).__name__
            addresses = []
    else:
        secondary_error = None
    if not addresses:
        reason = secondary_error or primary_error or "no_addresses_found"
        print(f"RESULT::HL_LEADERBOARD_EXTRACT_FAIL reason={reason}")
        return 1
    try:
        write_output(addresses)
    except Exception as exc:
        print(f"RESULT::HL_LEADERBOARD_EXTRACT_FAIL reason={type(exc).__name__}")
        return 1
    for addr in addresses[:20]:
        print(addr)
    print(f"total={len(addresses)}")
    print(f"route={route}")
    print(f"RESULT::HL_LEADERBOARD_EXTRACT_OK addresses={len(addresses)} output={OUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
