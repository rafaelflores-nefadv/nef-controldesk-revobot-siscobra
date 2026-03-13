import os
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

from config.settings import DEFAULT_TIMEOUT, DOWNLOAD_DIR, HEADLESS


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
    if HEADLESS:
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")

    chrome_binary = os.environ.get("CHROME_BINARY")
    if not chrome_binary:
        for candidate in (
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
        ):
            if Path(candidate).exists():
                chrome_binary = candidate
                break
    if chrome_binary:
        options.binary_location = chrome_binary

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    wait = WebDriverWait(driver, DEFAULT_TIMEOUT)
    return driver, wait
