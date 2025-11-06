import unittest
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from src.utils.chrome_config import get_headless_chrome_driver  # Função que retorna o driver do Chrome configurado.
from src.utils.login import aapc_login  # Função que realiza o login no site.
import os
import time

AAPC_EMAIL = os.environ['AAPC_EMAIL']
AAPC_PW = os.environ['AAPC_PASSWORD']
BASE_SITE = "https://www.aapc.com/codes/cpt-codes/"
URL_LOGIN = "https://aapclogin.b2clogin.com/aapclogin.onmicrosoft.com/oauth2/v2.0/authorize?p=b2c_1a_signin_usernameoremail&client_id=313cd8f4-0aea-4bbf-93b1-1476c5067538&redirect_uri=https%3A%2F%2Fwww.aapc.com&response_type=id_token%20token&scope=openid%20offline_access%20https%3A%2F%2Faapclogin.onmicrosoft.com%2Fidentityframework%2Ftask.read&state=OpenIdConnect.AuthenticationProperties%3D-kyeLVvvjd99txVXofBIU0GTRBOgZuDz6Chl0JN0_8HkZJqTgAhRwzkpwM0l8xH1V-j7owfiq2xTP00EgwUHJVa0lHmHPn46RxwDhs0fR0leVBVuzncl134G4_QzwvEOGRK5hWUk6abbvMZDST5mKcPxR9m4gJ_qEhYDO9e2KjTScr0-Kv0oZDal8NA6-9N03k2FoQ&response_mode=form_post&nonce=638506186637938531.OTI2MGUyYTktYTQ3YS00Y2VmLWFhMTUtMzM3NDNhOGZiNzhiNjJkOGU5MDctNTQ1MS00NDQ1LWE4YTUtZTYwYmZkOGJlNDM5&x-client-SKU=ID_NET461&x-client-ver=5.3.0.0"

class TestProcedureExtraction(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.driver = get_headless_chrome_driver()

    def test_01_extract_procedure_codes(self):
        """Teste simples para verificar a extração de códigos de procedimento não autenticado."""
        code = '00102'  # Código de exemplo
        url = BASE_SITE + code
        driver = self.driver
        driver.get(url)

        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

        html_content = driver.page_source
        description = self._get_short_description(html_content)
        
        self.assertTrue(description, "A descrição do código não foi encontrada.")
        print(f"Descrição do código {code}: {description}")
           
        try:
            sub_head_detail_elem = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "body > section > div > div > div:nth-child(4) > div > div > div.sub_head_detail"))
            )
            sub_head_detail_text = sub_head_detail_elem.text.strip()
            
            if not sub_head_detail_text:
                raise ValueError("O elemento foi encontrado, mas está vazio.")
            
            print(f"Texto extraído do sub_head_detail do código {code}: {sub_head_detail_text}")
            self.assertTrue(sub_head_detail_text, "O conteúdo do sub_head_detail está vazio.")
        except Exception as e:
            self.fail(f"Erro ao extrair sub_head_detail do código {code}: {str(e)}")

    def test_02_extract_logged_procedure_codes(self):
        """Teste para verificar a extração de dados após login no site."""
        driver = self.driver

        aapc_login(
            driver=driver,
            url_login=URL_LOGIN,
            aapc_email=AAPC_EMAIL,
            aapc_pw=AAPC_PW,
            username_field_id='signInName',
            password_field_id='password',
            login_button_selector='button[type=submit]',
            second_login_button_id='btnSignIn',
            subscription_menu_selector='#ctl00_Body_ctl00_mnuCodifySubscription'
        )


        code = '00102'
        url = BASE_SITE + code
        driver.get(url)

        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

        try:
            extra_info_elem = WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "#cpt_mod_ascp_cross > div.modcross_list > p:nth-child(1) > strong"))
            )
            extra_info_text = extra_info_elem.text.strip()
            print(f"Texto extraído após login (descrição cruzada): {extra_info_text}")
            self.assertTrue(extra_info_text, "Texto da seção logada está vazio.")
        except Exception as e:
            self.fail(f"Erro ao extrair texto logado do código {code}: {str(e)}")
            
            
    @staticmethod
    def _get_short_description(html_content):
        """Método auxiliar para extrair uma descrição curta do código (simula a extração)."""
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')
        description_div = soup.find('div', class_='layout2_code')
        if description_div:
            h1_tag = description_div.find('h1')
            if h1_tag:
                return h1_tag.get_text().strip()
        return ""

    @classmethod
    def tearDownClass(cls):
        cls.driver.quit()

if __name__ == '__main__':
    unittest.main()
