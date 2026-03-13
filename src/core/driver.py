import os
import platform
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

from config.settings import DEFAULT_TIMEOUT, DOWNLOAD_DIR, HEADLESS


def _default_chrome_candidates() -> tuple[str, ...]:
    system = platform.system().lower()
    if system == "linux":
        return (
            "/usr/bin/google-chrome",
            "/usr/bin/google-chrome-stable",
        )
    if system == "windows":
        return (
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
        )
    return ()


def _resolve_chrome_binary() -> str | None:
    chrome_binary = os.environ.get("CHROME_BINARY")
    if chrome_binary:
        chrome_binary = chrome_binary.strip()
        if chrome_binary:
            return chrome_binary

    for candidate in _default_chrome_candidates():
        if Path(candidate).exists():
            return candidate
    return None


def _add_argument_once(options: webdriver.ChromeOptions, argument: str) -> None:
    if argument not in options.arguments:
        options.add_argument(argument)


def criar_driver():
    download_dir = Path(DOWNLOAD_DIR)
    download_dir.mkdir(parents=True, exist_ok=True)

    options = webdriver.ChromeOptions()
    prefs = {
        "download.default_directory": str(download_dir),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    options.add_experimental_option("prefs", prefs)

    is_linux = platform.system().lower() == "linux"
    if is_linux:
        # Flags extras para estabilidade do Chrome em VPS Linux/headless.
        for arg in (
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--window-size=1920,1080",
            "--disable-software-rasterizer",
            "--disable-extensions",
            "--remote-debugging-port=9222",
        ):
            _add_argument_once(options, arg)

    if HEADLESS:
        _add_argument_once(options, "--headless=new")
        _add_argument_once(options, "--disable-gpu")

    chrome_binary = _resolve_chrome_binary()
    if chrome_binary:
        options.binary_location = chrome_binary

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    wait = WebDriverWait(driver, DEFAULT_TIMEOUT)
    return driver, wait
