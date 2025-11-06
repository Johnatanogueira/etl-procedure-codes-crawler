from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from utils.chrome_config import get_headless_chrome_driver
import re

def get_download_url_dict_per_year(url: str, driver: webdriver.Chrome = None) -> dict:
  close_driver = False
  if(driver is None):
    driver = get_headless_chrome_driver()
    close_driver = True
  driver.get(url)
  wait = WebDriverWait(driver, 10)

  icd_files_container = wait.until(EC.visibility_of_element_located((By.CLASS_NAME, 'ckeditor-accordion-container')))
  icd_files_li_list = icd_files_container.find_elements(By.CSS_SELECTOR, 'dl > dd')

  regex_extract_year = r"\b(2[0-9]{3})\b" # Hint: Regex para encontrar números de 4 digitos iniciando por "2". Ex: 2025, 2099, 2001, 2015...

  year_download_urls = {}
  for icd_li in icd_files_li_list:
    wait = WebDriverWait(icd_li, 10)

    download_buttons = icd_li.find_elements(By.TAG_NAME, 'a')

    icd_year_text = download_buttons[0].get_property('href')
    icd_year = str( re.search(regex_extract_year, icd_year_text).group() ) # Hint: Se der no regex, não foi possível extrair o ANO desse elemento.

    year_download_urls[icd_year] = []

    for download_button in download_buttons:
      button_href = download_button.get_property('href')
      year_download_urls[icd_year].append(button_href)
      
  if(close_driver):
    driver.quit()
  return year_download_urls
