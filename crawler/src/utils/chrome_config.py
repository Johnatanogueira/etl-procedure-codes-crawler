from selenium import webdriver

def get_headless_chrome_driver() -> webdriver.Chrome:
    """
    Retorna uma instância do Chrome WebDriver com configurações headless.
    Ideal para uso em ambientes como servidores ou containers.
    """
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-browser-side-navigation")
    chrome_options.add_argument("--disable-gpu")

    driver = webdriver.Chrome(options=chrome_options)
    return driver
