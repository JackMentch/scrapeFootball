from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup, Comment
import pandas as pd
import time

options = webdriver.ChromeOptions()
# Keep headless OFF for now so PFR thinks it's a real user
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option("useAutomationExtension", False)

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

url = "https://www.pro-football-reference.com/players/A/AvilSt00.htm"
driver.get(url)
time.sleep(4)  # let page fully load

soup = BeautifulSoup(driver.page_source, "html.parser")
driver.quit()

# Unhide comment-wrapped tables
for comment in soup.find_all(string=lambda t: isinstance(t, Comment)):
    if "table" in comment:
        soup.append(BeautifulSoup(comment, "html.parser"))

# Print all table IDs
for table in soup.find_all("table"):
    print(table.get("id", "no-id"))