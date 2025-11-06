import unittest
from unittest.mock import MagicMock
from bs4 import BeautifulSoup
from src.procedure_code import extract_prodecure_codes

LOGICAL_DATE = '2025-05-05'

class TestExtractProcedureCodes(unittest.TestCase):
    def test_extract_valid_cpt_code(self):
        # Simula HTML da página
        html = """
        <html>
            <div class="layout2_code"><h1>12345, Short Description Example</h1></div>
            <div class="sub_head_detail">Long Description Text Here</div>
            <div class="tab-pane active">Summary content</div>
            <div class="div newbread"><span></span><span></span><span></span><span></span><span>Main Interval</span><a href="/codes/cpt-codes/00100-00200">Link</a></div>
        </html>
        """

        # Mocka o Selenium WebDriver
        mock_driver = MagicMock()
        mock_driver.get = MagicMock()
        mock_driver.current_url = "https://www.aapc.com/codes/cpt-codes/12345"
        mock_driver.page_source = html

        result = extract_prodecure_codes(mock_driver, "12345", "https://fakeurl.com/12345")

        self.assertEqual(result['code'], "12345")
        self.assertEqual(result['code_type'], "CPT")
        self.assertEqual(result['short_description'], "Short Description Example")
        self.assertEqual(result['long_description'], "Long Description Text Here")
        self.assertEqual(result['main_interval'], "00100-00200")

    def test_handle_404_page(self):
        html = """
        <html><div class="hding404">Page not found</div></html>
        """
        mock_driver = MagicMock()
        mock_driver.get = MagicMock()
        mock_driver.current_url = "https://www.aapc.com/codes/cpt-codes/00000"
        mock_driver.page_source = html

        result = extract_prodecure_codes(mock_driver, "00000", "https://fakeurl.com/00000")

        self.assertIsNone(result)

if __name__ == '__main__':
    unittest.main()
