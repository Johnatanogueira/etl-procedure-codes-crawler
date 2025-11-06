import unittest
from unittest.mock import patch, MagicMock
from src.utils.chrome_config import get_headless_chrome_driver
from selenium.webdriver.chrome.webdriver import WebDriver

    
class TestChromeConfig(unittest.TestCase):
    
    @patch("src.utils.chrome_config.webdriver.Chrome")
    def test_get_headless_chrome_driver_adds_headless_argument(self, mock_chrome):
        mock_instance = MagicMock()
        mock_chrome.return_value = mock_instance

        driver = get_headless_chrome_driver()
        driver.quit()

        called_args, called_kwargs = mock_chrome.call_args
        chrome_options = called_kwargs["options"]
        arguments = chrome_options.arguments if hasattr(chrome_options, "arguments") else chrome_options._arguments

        self.assertIn("--headless", arguments)
    
    def test_get_headless_chrome_driver_returns_driver(self):
        driver = get_headless_chrome_driver()
        self.assertIsNotNone(driver)
        self.assertIn("chrome", driver.capabilities["browserName"].lower())
        driver.quit()

if __name__ == '__main__':
    unittest.main()
