import pandas as pd
import time
import re
import os
import sys
import logging
import json

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
from datetime import datetime
from utils.chrome_config import get_headless_chrome_driver
from utils.s3 import s3_athena_load_table_parquet_snappy
from utils.athena import athena_get_generator
from utils.login import aapc_login
from utils.config import PROJECT_PATH
from utils.logger import get_logger
from utils.secret_manager import get_secret

logger = get_logger('procedure_codes')


LOGICAL_DATE = os.environ['LOGICAL_DATE']
AAPC_SECRET_ID = os.environ['AAPC_SECRET_ID']

ATHENA_QUERY_OUTPUT_LOCATION = os.environ['ATHENA_QUERY_OUTPUT_LOCATION']
ATHENA_OUTPUT_PROCEDURE_CODES_TABLE_SCHEMA = os.environ['ATHENA_OUTPUT_PROCEDURE_CODES_TABLE_SCHEMA']
ATHENA_OUTPUT_PROCEDURE_CODES_TABLE_NAME = os.environ['ATHENA_OUTPUT_PROCEDURE_CODES_TABLE_NAME']
ATHENA_OUTPUT_PROCEDURE_CODES_TABLE_LOCATION = os.environ['ATHENA_OUTPUT_PROCEDURE_CODES_TABLE_LOCATION']
ATHENA_OUTPUT_PROCEDURE_MODIFIERS_TABLE_SCHEMA = os.environ['ATHENA_OUTPUT_PROCEDURE_MODIFIERS_TABLE_SCHEMA']
ATHENA_OUTPUT_PROCEDURE_MODIFIERS_TABLE_NAME = os.environ['ATHENA_OUTPUT_PROCEDURE_MODIFIERS_TABLE_NAME']
ATHENA_OUTPUT_PROCEDURE_MODIFIERS_TABLE_LOCATION = os.environ['ATHENA_OUTPUT_PROCEDURE_MODIFIERS_TABLE_LOCATION']
ATHENA_OUTPUT_PROCEDURE_NDC_TABLE_SCHEMA = os.environ['ATHENA_OUTPUT_PROCEDURE_NDC_TABLE_SCHEMA']
ATHENA_OUTPUT_PROCEDURE_NDC_TABLE_NAME = os.environ['ATHENA_OUTPUT_PROCEDURE_NDC_TABLE_NAME']
ATHENA_OUTPUT_PROCEDURE_NDC_TABLE_LOCATION = os.environ['ATHENA_OUTPUT_PROCEDURE_NDC_TABLE_LOCATION']

ATHENA_PROCEDURE_CODES_COLUMNS = ['code', 'code_type', 'main_interval', 'main_interval_name', 'modifiers', 'short_description', 'long_description', 'description', 'summary', 'date_deleted', 'betos_code', 'betos_description', 'guidelines', 'advice', 'lay_term', 'report', 'revenue_lookup', 'icd10_cm', 'ndc_alternate_id', 'icd_10_pcs_x', 'cpt_code_symbols']
ATHENA_PROCEDURE_CODE_MODIFIER_COLUMNS = ['modifier', 'description']
ATHENA_PROCEDURE_CODE_NDC_COLUMNS = ['ndc_alternate_id', 'drug_name', 'labeler_name', 'hcpcs_dosage', 'bill_unit']

logger.info(f"Running on date: {LOGICAL_DATE}")

QUERY_DQL_PROCEDURE_CODE = 'src/queries/dql_procedure_code.sql'
QUERY_DQL_PROCEDURE_CODE_MODIFIER = 'src/queries/dql_procedure_code_modifiers.sql'
QUERY_DQL_PROCEDURE_CODE_NDC= 'src/queries/dql_procedure_code_ndc.sql'
BASE_SITE="xxxxxxxxxxxxxxxxxxxxxxxx"
URL_LOGIN="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

def is_error_404_page(soup):
    return bool(soup.find('div', class_='container404'))

def is_deleted_hcpcs_page(soup):
    h1_tag = soup.find('h1')
    return h1_tag and 'Deleted HCPCS Codes' in h1_tag.get_text(strip=True)

def get_deleted(driver, soup):

    deleted_span = soup.find('span', string=re.compile(r'\bDeleted\b', re.IGNORECASE))
    if not deleted_span:
        return None  

    date_deleted = None
    alert_div = soup.find('div', class_='alert alert-danger')
    if alert_div:
        date_deleted = alert_div.get_text(separator=' ', strip=True)
        date_deleted = ' '.join(date_deleted.split())

    advice = None
    for div in soup.find_all('div'):
        text = div.get_text(separator=' ', strip=True)
        if 'Advice:' in text:
            p = div.find('p')
            if p:
                adv_text = p.get_text(strip=True)
            else:
                parts = text.split('Advice:', 1)
                adv_text = parts[1].strip() if len(parts) > 1 else text.strip()
            advice = ' '.join(adv_text.split())
            break

    lay_term = None
    layterm_divs = soup.find_all('div', class_='panel-body tab-pane')
    for div in layterm_divs:
        text = div.get_text(separator=' ', strip=True)
        if 'The provider administers the first dose' in text and 'COVID–19' in text:
            lay_term = text.strip()
            break

    guidelines = None
    for div in layterm_divs:
        text = div.get_text(separator=' ', strip=True)
        if 'Guidelines found' in text or 'No CPT' in text or 'No HCPCS' in text:
            guidelines = text.strip()
            break
        
    description = None
    panels = soup.find_all('div', class_='panel panel-default')
    for panel in panels:
        heading = panel.find('div', class_='panel-heading')
        if heading and 'Code Descriptor' in heading.get_text():
            body = panel.find('div', class_='panel-body tab-pane')
            if body:
                raw_text = body.get_text(strip=True, separator=' ')
                description = re.sub(r'\s+', ' ', raw_text)
                break

    return date_deleted, advice, lay_term, guidelines, description

def get_short_description(soup, is_cpt):
    short_description = ''
    description_div = soup.find('div', class_='layout2_code')
    if description_div:
        h1_tag = description_div.find('h1')
        if h1_tag:
            full_text = h1_tag.get_text().strip()
            parts = full_text.split(',', 1)
            if len(parts) > 1:
                short_description = parts[1].strip()
            else:
                short_description = full_text
    return short_description

def get_long_description(soup):
    long_description = ''
    second_text_div = soup.find('div', class_='sub_head_detail')
    if second_text_div:
        long_description = second_text_div.get_text().strip()
    else:
        h2_tag = soup.find('h2', class_='sub_head_detail')
        if h2_tag:
            long_description = h2_tag.get_text().strip()
    return long_description

def get_main_interval_name(soup):
    main_interval_name = []

    breadcrumbs_div = soup.find('div', class_='div newbread')
    if not breadcrumbs_div:
        breadcrumbs_div = soup.find('div', class_='newbread logout-header')

    if breadcrumbs_div:
        all_divs = breadcrumbs_div.find_all('div', class_='div')
        
        index_start = -1
        for i, div in enumerate(all_divs):
            a_tag = div.find('a')
            if a_tag and a_tag.get_text(strip=True) in ["CPT Codes", "HCPCS Codes"]:
                index_start = i

        if index_start != -1:
            for div in all_divs[index_start + 1:]:
                if div.find('a'):
                    span = div.find('span')
                    if span:
                        main_interval_name.append(span.get_text(strip=True))
                else:
                    break

    return main_interval_name if main_interval_name else None

def get_main_interval(soup, is_cpt):
    main_interval = ''
    breadcrumbs_div = soup.find('div', class_='div newbread')

    if breadcrumbs_div:
        if is_cpt:
            links = breadcrumbs_div.find_all('a', href=True)
            for link in links:
                href = link['href']
                match = re.search(r'/cpt-codes-range/(\d{4,5}T?-\d{4,5}T?)/', href)
                if match:
                    main_interval = match.group(1)
                    break
        else:
            span_elements = breadcrumbs_div.find_all('span')
            for span in span_elements:
                text = span.get_text().strip()
                match = re.search(r'\b([A-Z]\d{4}-[A-Z]\d{4})\b', text)
                if match:
                    main_interval = match.group(1)
                    break
    return main_interval

def get_modifier_description(soup):
    data = []
    modifier_codes = []
    
    divDescription = soup.find('div', class_='modcross_list')
    if divDescription:
        table = divDescription.find('tbody')
        if table:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all('td')
                if cells and len(cells) >= 2:
                    modifier = cells[0].get_text().strip()
                    modifier_description = cells[1].get_text().strip()
                    data.append([modifier, modifier_description])
                    modifier_codes.append(modifier)
                    
    return data, modifier_codes

def get_betos(driver):
    betos_code = None
    betos_description = None
    
    betos_div = extract_tab_content_with_fallback(
        driver,
        tab_selectors=['a[href="#cpt_betos"]', 'a[href="#hcpcs_betos"]'],
        div_ids=['cpt_betos', 'hcpcs_betos']
    )

    if betos_div:
        for inner_div in betos_div.find_all('div'):
            strong_tag = inner_div.find('strong')
            if strong_tag:
                if 'Code:' in strong_tag.text:
                    betos_code = inner_div.get_text().replace('Code:', '').strip()
                elif 'Description:' in strong_tag.text:
                    betos_description = inner_div.get_text().replace('Description:', '').strip()
                    
    return betos_code, betos_description

def get_guidelines(driver):
    guidelines = None
    if safe_click_tab(driver, 'a[href="#cpt_guidelines"]'):
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        div = soup.find('div', id='cpt_guidelines')
        if div:
            guidelines = div.get_text(separator=' ', strip=True)
            
    return guidelines

def get_advice(driver):
    advice = None
    if safe_click_tab(driver, 'a[href="#cpt_advice"]'):
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        div = soup.find('div', id='cpt_advice')
        if div:
            advice = div.get_text(separator=' ', strip=True)
    return advice

def get_lay_term(driver):
    lay_term = None
    summary = None

    tab_clicked = safe_click_tab(driver, 'a[href="#cpt_layterm"]') or safe_click_tab(driver, 'a[href="#hcpcs_layterm"]')
    if not tab_clicked:
        logger.info("Aba 'Lay Term' não disponível.")
        return None, None

    time.sleep(0.5)

    try:
        read_more = WebDriverWait(driver, 2).until(
            EC.element_to_be_clickable((By.XPATH, '//a[contains(text(), "Read More")]'))
        )
        driver.execute_script("arguments[0].click();", read_more)
        time.sleep(1.0)
    except TimeoutException:
        logger.debug("Botão 'Read More' não encontrado.")
    except Exception as e:
        logger.warning(f"Erro ao clicar em 'Read More': {e}")

    try:
        WebDriverWait(driver, 3).until(
            EC.presence_of_element_located((By.ID, 'fullLayterm'))
        )
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        full_div = soup.find('div', id='fullLayterm')

        if full_div:
            first_p = full_div.find('p')
            if first_p:
                summary = first_p.get_text(strip=True)

            read_less_link = full_div.find('a', string=re.compile(r'Read Less', re.IGNORECASE))
            if read_less_link:
                read_less_link.decompose()

            lay_term = full_div.get_text(separator=' ', strip=True)

            if lay_term.lower().endswith("read less"):
                lay_term = lay_term[:-len("Read Less")].strip()

    except Exception as e:
        logger.error(f"Erro ao extrair conteúdo do Lay Term: {e}")

    return summary, lay_term

def get_report(driver):
    report = None
    if safe_click_tab(driver, 'a[href="#cpt_report"]'):
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        div = soup.find('div', id='cpt_report')
        if div:
            report = div.get_text(separator=' ', strip=True)
    return report

def get_revenue_code_lookup(driver):
    revenue_lookup_array = None

    if safe_click_tab(driver, 'a[href="#cpt_revenue_lookup"]'):
        try:
            WebDriverWait(driver, 10).until(
                lambda d: "loading" not in d.find_element(By.ID, "cpt_revenue_cross").text.lower()
            )
            time.sleep(0.5)
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            revenue_div = soup.find('div', id='cpt_revenue_cross')

            if revenue_div:
                if "Data Not Available" in revenue_div.get_text():
                    logger.info("Revenue lookup: Dados não disponíveis.")
                    revenue_lookup_array = None 
                else:
                    table = revenue_div.select_one('table.points_table')
                    if table:
                        rows = table.find_all('tr')
                        extracted_codes = []
                        for row in rows[1:]:  
                            cols = row.find_all('td')
                            if len(cols) >= 1:
                                rev_code = cols[0].get_text(strip=True)
                                if rev_code:
                                    extracted_codes.append(rev_code)

                        revenue_lookup_array = extracted_codes if extracted_codes else []
                        if not revenue_lookup_array:
                            logger.debug("Revenue Code Lookup: Nenhum código extraído.")
                    else:
                        logger.debug("Revenue Code Lookup: Tabela não encontrada.")
            else:
                logger.debug("Revenue Code Lookup: Div não encontrada.")
        except Exception as e:
            logger.error(f"Erro no carregamento da aba Revenue Code Lookup: {e}")
    return revenue_lookup_array

def get_icd10_cm(driver):
    icd10_results = []
    logger.info("Abrindo aba ICD-10 CM X...")

    try:
        icd10_tab = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//a[contains(text(), "ICD-10 CM X")]'))
        )
        driver.execute_script("arguments[0].scrollIntoView(true);", icd10_tab)
        driver.execute_script("arguments[0].click();", icd10_tab)
        time.sleep(1.0)
    except TimeoutException:
        logger.warning("Aba 'ICD-10 CM X' não encontrada.")
        return None

    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'a.ab_links'))
        )
    except TimeoutException:
        logger.warning("Botões de letras ICD-10 CM não encontrados.")
        return None

    letter_buttons = driver.find_elements(By.CSS_SELECTOR, 'a.ab_links')
    available_letters = [btn.text.strip() for btn in letter_buttons if btn.text.strip()]

    if not available_letters:
        logger.info("Nenhuma letra encontrada na aba ICD-10 CM.")
        return None
    else:
        logger.info(f"Letras ICD-10 CM disponíveis: {available_letters}")
        for letter in available_letters:
            logger.info(f"Processando letra: {letter}")
            try:
                letter_button = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((
                        By.XPATH,
                        f'//a[contains(@class, "ab_links") and normalize-space(text())="{letter}"]'
                    ))
                )
                if 'selected' not in letter_button.get_attribute('class'):
                    driver.execute_script("arguments[0].scrollIntoView(true);", letter_button)
                    driver.execute_script("arguments[0].click();", letter_button)
                    time.sleep(0.5)

                WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "table.points_table tbody tr td"))
                )

                rows = driver.find_elements(By.CSS_SELECTOR, "table.points_table tbody tr")
                for row in rows:
                    cols = row.find_elements(By.TAG_NAME, "td")
                    texts = [col.text.strip() for col in cols]
                    if len(texts) >= 1 and texts[0]:
                        code_cm = texts[0].replace('.', '')
                        icd10_results.append(code_cm)

            except TimeoutException:
                logger.warning(f"Tabela não encontrada para a letra: {letter}. Pulando...")

    return icd10_results if icd10_results else None

def get_ndc(driver):
    ndc_full_extracted_data = None
    ndc_full = None
    alternate_ids = []

    if safe_click_tab(driver, 'a[href="#ndc"]'):
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        div = soup.find('div', id='ndc')
        if div:
            table = div.find('table')
            if table:
                rows = table.select('tbody tr')
                ndc_rows = []
                for row in rows:
                    cols = row.find_all('td')
                    if cols:
                        values = [col.text.strip() for col in cols]
                        if any(values):  # ao menos uma célula com conteúdo
                            ndc_rows.append(values)
                ndc_full = ndc_rows if ndc_rows else None
            else:
                logger.info("Tabela NDC não encontrada.")
        else:
            logger.info("Div #ndc não encontrada.")
    else:
        logger.info("Aba NDC não disponível ou não clicável.")

    if ndc_full:
        ndc_full_extracted_data = []
        for row in ndc_full:
            if len(row) >= 5:
                alternate_ids.append(row[0])
                ndc_full_extracted_data.append({
                    'ndc_alternate_id': row[0],
                    'drug_name': row[1],
                    'labeler_name': row[2],
                    'hcpcs_dosage': row[3],
                    'bill_unit': row[4].strip() if row[4] else ''
                })

    return alternate_ids if alternate_ids else None, ndc_full_extracted_data

def get_icd_pcs_x(driver):
    pcs = None
    if safe_click_tab(driver, 'a[href="#PCS"]'):
        try:
            WebDriverWait(driver, 10).until(
                lambda d: "loading" not in d.find_element(By.ID, "pcsdata").text.lower()
            )
            time.sleep(0.5)
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            div = soup.find('div', id='pcsdata')
            if div:
                table = div.select_one('table.points_table')
                if table:
                    pcs_codes = []
                    rows = table.select('tbody tr')
                    for row in rows:
                        cols = row.find_all('td')
                        if len(cols) >= 1:
                            pcs_code = cols[0].get_text(strip=True)
                            if pcs_code:
                                pcs_codes.append(pcs_code)
                    pcs = pcs_codes if pcs_codes else None
                    if not pcs:
                        logger.debug("PCS: Tabela encontrada, mas nenhum código foi extraído.")
                else:
                    logger.debug("PCS: Tabela não encontrada na div.")
            else:
                logger.debug("PCS: Div #pcsdata não encontrada.")
        except Exception as e:
            logger.error(f"Erro ao aguardar carregamento da aba PCS: {e}")
      
    return pcs

def get_cpt_code_symbols(driver):
    cpt_code_symbols = None
    current_url = driver.current_url.lower()
    soup = BeautifulSoup(driver.page_source, 'html.parser')

    if 'cpt-codes' in current_url:
        cpt_symbol_div = soup.find('div', id='cpt_symbol_div')
        if cpt_symbol_div:
            icon_divs = cpt_symbol_div.find_all('div', class_='icon-dic-o')
            extracted_symbols = []
            for icon_div in icon_divs:
                full_text = icon_div.get_text(separator=' ', strip=True)
                parts = full_text.split(':', 1)
                if len(parts) == 2:
                    description = parts[1].strip()
                    if description:
                        extracted_symbols.append(description)
            if extracted_symbols:
                cpt_code_symbols = extracted_symbols

    elif 'hcpcs-codes' in current_url:
        hcpcs_title = soup.find('p', class_='box-detail-head', string='HCPCS Code Symbols')
        if hcpcs_title:
            box_detail_div = hcpcs_title.find_parent('div', class_='box-detail box-blue')
            if box_detail_div:
                icon_divs = box_detail_div.find_all('div', class_='icon-dic-o')
                extracted_symbols = []
                for icon_div in icon_divs:
                    for img_tag in icon_div.find_all('img'):
                        img_tag.decompose()
                    full_text = icon_div.get_text(separator=' ', strip=True)
                    parts = full_text.split(':', 1)
                    if len(parts) == 2:
                        description = parts[1].strip()
                        if description:
                            extracted_symbols.append(description)
                if extracted_symbols:
                    cpt_code_symbols = extracted_symbols
                    
    return cpt_code_symbols

def get_official_descriptor(driver):
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'div.tab-pane'))
        )

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        descriptor_div = soup.select_one('div.tab-pane')

        if descriptor_div:
            descriptor_text = ' '.join(descriptor_div.stripped_strings)
            return descriptor_text if descriptor_text else None

        return None

    except Exception as e:
        logger.error(f"Erro ao extrair o Official Descriptor: {e}")
        return None

def extracted_procedure_modifiers_v2(driver, code):
  url = BASE_SITE + code.strip()

  logger.info(f"Extracting procedure modifiers : {url}")
  try:
    driver.get(url)
    WebDriverWait(driver, 10).until(
      EC.presence_of_element_located((By.TAG_NAME, "body"))
    )

    is_cpt = 'cpt' in driver.current_url.lower()
    html_content = driver.page_source
    soup = BeautifulSoup(html_content, 'html.parser')
    
    date_deleted = None

    if is_error_404_page(soup):
        logger.warning(f"Código {code} ignorado por retornar página de erro 404.")
        return (
            pd.DataFrame([], columns=ATHENA_PROCEDURE_CODES_COLUMNS),
            pd.DataFrame([], columns=ATHENA_PROCEDURE_CODE_MODIFIER_COLUMNS),
            pd.DataFrame([], columns=ATHENA_PROCEDURE_CODE_NDC_COLUMNS)
        )

    if is_deleted_hcpcs_page(soup):
        logger.info(f'Código {code} ignorado por ser página genérica de Deleted HCPCS Codes.')
        return (
            pd.DataFrame([], columns=ATHENA_PROCEDURE_CODES_COLUMNS),
            pd.DataFrame([], columns=ATHENA_PROCEDURE_CODE_MODIFIER_COLUMNS),
            pd.DataFrame([], columns=ATHENA_PROCEDURE_CODE_NDC_COLUMNS)
        )

    deleted_check = get_deleted(driver, soup)
    if deleted_check:
        date_deleted, advice, lay_term, guidelines, description = deleted_check

        procedure_code = pd.DataFrame([[
            code,
            'CPT' if is_cpt else 'HCPCS',
            None,  
            None,  
            None,  
            None,  
            None,  
            description,
            None, 
            date_deleted,
            None, 
            None, 
            guidelines,
            advice,
            lay_term,
            None,  
            None,  
            None,  
            None,  
            None,  
            None   
        ]], columns=ATHENA_PROCEDURE_CODES_COLUMNS)
        return procedure_code, pd.DataFrame([], columns=ATHENA_PROCEDURE_CODE_MODIFIER_COLUMNS), pd.DataFrame([], columns=ATHENA_PROCEDURE_CODE_NDC_COLUMNS)

            
    code_type = 'CPT' if is_cpt else 'HCPCS'
    main_interval = get_main_interval(soup, is_cpt)
    short_description = get_short_description(soup, is_cpt) 
    long_description = get_long_description(soup) 
    main_interval_name = get_main_interval_name(soup)
    data, modifiers = get_modifier_description(soup)
    betos_code, betos_description = get_betos(driver)
    guidelines = get_guidelines(driver)
    advice = get_advice(driver)
    summary, lay_term = get_lay_term(driver)
    report = get_report(driver)
    revenue_lookup = get_revenue_code_lookup(driver)
    icd10_cm = get_icd10_cm(driver)
    ndc_alternate_id, ndc_all = get_ndc(driver)   
    icd_10_pcs_x = get_icd_pcs_x(driver)
    cpt_code_symbols = get_cpt_code_symbols(driver)
    description = get_official_descriptor(driver)
    
    procedure_code = pd.DataFrame( [ 
        [
            code,
            code_type,
            main_interval,
            main_interval_name,
            modifiers,
            short_description,
            long_description,
            description,
            summary,
            date_deleted,
            betos_code,
            betos_description,
            guidelines,
            advice,
            lay_term,
            report,
            revenue_lookup,
            icd10_cm,
            ndc_alternate_id,
            icd_10_pcs_x,
            cpt_code_symbols
        ]
    ], columns = ATHENA_PROCEDURE_CODES_COLUMNS )
    
    df_modifier = pd.DataFrame(data, columns=ATHENA_PROCEDURE_CODE_MODIFIER_COLUMNS)
    df_ndc = pd.DataFrame(ndc_all, columns=ATHENA_PROCEDURE_CODE_NDC_COLUMNS)
  
    return procedure_code, df_modifier, df_ndc
  except Exception as e:
      logger.error(f"Erro ao acessar a página {url} para o código {code}: {e}")
    
def extract_tab_content_with_fallback(driver, tab_selectors, div_ids):

    for tab_selector, div_id in zip(tab_selectors, div_ids):
        if safe_click_tab(driver, tab_selector):
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            div = soup.find('div', id=div_id)
            if div:
                return div
    return None

def safe_click_tab(driver, css_selector: str, timeout: int = 10) -> bool:
    try:
        tab = WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, css_selector))
        )
        driver.execute_script("arguments[0].scrollIntoView(true);", tab)
        driver.execute_script("arguments[0].click();", tab)
        return True
    except TimeoutException:
        logger.debug(f"Aba com seletor '{css_selector}' não encontrada ou não carregada dentro de {timeout}s.")  
    except Exception as e:
        logger.error(f"Erro ao clicar em {css_selector}: {e}")
    return False

if __name__ == "__main__":
    logger.info("Início do processo")
    try:
        secret_json = get_secret(secret_name=AAPC_SECRET_ID)
        secret_dict = json.loads(secret_json)

        aapc_email = secret_dict['aapc']['email']
        aapc_pw = secret_dict['aapc']['password']

        with open(os.path.join(PROJECT_PATH, QUERY_DQL_PROCEDURE_CODE), 'r') as f:
            qry_dql_procedure_code_table = ''.join(f.readlines()).format(
                LOGICAL_DATE=LOGICAL_DATE,
                ATHENA_OUTPUT_PROCEDURE_CODES_TABLE_SCHEMA=ATHENA_OUTPUT_PROCEDURE_CODES_TABLE_SCHEMA,
                ATHENA_OUTPUT_PROCEDURE_CODES_TABLE_NAME=ATHENA_OUTPUT_PROCEDURE_CODES_TABLE_NAME
            )
        with open(os.path.join(PROJECT_PATH, QUERY_DQL_PROCEDURE_CODE_MODIFIER), 'r') as f:
            qry_dql_procedure_code_modifier_table = ''.join(f.readlines()).format(
                ATHENA_OUTPUT_PROCEDURE_MODIFIERS_TABLE_SCHEMA=ATHENA_OUTPUT_PROCEDURE_MODIFIERS_TABLE_SCHEMA,
                ATHENA_OUTPUT_PROCEDURE_MODIFIERS_TABLE_NAME=ATHENA_OUTPUT_PROCEDURE_MODIFIERS_TABLE_NAME
            )
        with open(os.path.join(PROJECT_PATH, QUERY_DQL_PROCEDURE_CODE_NDC), 'r') as f:
            qry_dql_procedure_code_ndc_table = ''.join(f.readlines()).format(
                ATHENA_OUTPUT_PROCEDURE_NDC_TABLE_SCHEMA=ATHENA_OUTPUT_PROCEDURE_NDC_TABLE_SCHEMA,
                ATHENA_OUTPUT_PROCEDURE_NDC_TABLE_NAME=ATHENA_OUTPUT_PROCEDURE_NDC_TABLE_NAME
            )

        logger.info("Consultas carregadas com sucesso")

        df_procedure_codes = athena_get_generator(
            athena_query=qry_dql_procedure_code_table,
            athena_database=ATHENA_OUTPUT_PROCEDURE_CODES_TABLE_SCHEMA,
            s3_output=ATHENA_QUERY_OUTPUT_LOCATION
        )
        df_procedure_modifiers = athena_get_generator(
            athena_query=qry_dql_procedure_code_modifier_table,
            athena_database=ATHENA_OUTPUT_PROCEDURE_MODIFIERS_TABLE_SCHEMA,
            s3_output=ATHENA_QUERY_OUTPUT_LOCATION
        )
        df_procedure_ndc = athena_get_generator(
            athena_query=qry_dql_procedure_code_ndc_table,
            athena_database=ATHENA_OUTPUT_PROCEDURE_NDC_TABLE_SCHEMA,
            s3_output=ATHENA_QUERY_OUTPUT_LOCATION
        )

        df_procedure_codes.loc[df_procedure_codes['code'].str.strip() == '', 'code'] = None
        df_procedure_codes.loc[df_procedure_codes['code'].str.strip().str.lower() == 'false', 'code'] = None
        df_procedure_codes.dropna(inplace=True, ignore_index=True)

        chunk_size = 200
        total_codes = df_procedure_codes.shape[0]

        DRIVER_LOGGED = get_headless_chrome_driver()

        aapc_login(
          driver=DRIVER_LOGGED,
          url_login=URL_LOGIN,
          aapc_email=aapc_email,
          aapc_pw=aapc_pw,
          primary_login='next',
          second_login='continue',
          username_field_id='userProvidedSignInName',
          password_field_id='password',
          second_login_button_id='btnSignIn',
          subscription_menu_selector='#ctl00_Body_ctl00_mnuCodifySubscription'
        )
        
        logger.info("Login realizado para extração logada")

        for start_idx in range(0, total_codes, chunk_size):
            end_idx = min(start_idx + chunk_size, total_codes)
            chunk_codes = df_procedure_codes['code'].iloc[start_idx:end_idx]

            list_data_descriptions_modifiers = {}

            df_chunk_procedure_codes = pd.DataFrame(columns = ATHENA_PROCEDURE_CODES_COLUMNS)
            df_modifiers = pd.DataFrame(columns = ATHENA_PROCEDURE_CODE_MODIFIER_COLUMNS)
            df_new_procedure_ndc = pd.DataFrame(columns=ATHENA_PROCEDURE_CODE_NDC_COLUMNS)
            for code in chunk_codes:
              procedure_code, df_modifier, ndc_all = extracted_procedure_modifiers_v2(DRIVER_LOGGED, code)
  
              df_chunk_procedure_codes = pd.concat([df_chunk_procedure_codes, procedure_code], ignore_index=True)
              
              df_modifiers = pd.concat([df_modifiers, df_modifier], ignore_index=True)

              df_new_procedure_ndc = pd.concat([df_new_procedure_ndc, ndc_all], ignore_index=True)

              if not df_new_procedure_ndc.empty and not df_new_procedure_ndc.empty:
                  if 'ndc_alternate_id' in df_procedure_ndc.columns and 'ndc_alternate_id' in df_new_procedure_ndc.columns:
                      df_new_procedure_ndc = df_new_procedure_ndc[
                          ~df_new_procedure_ndc['ndc_alternate_id'].isin(df_procedure_ndc['ndc_alternate_id'])
                      ]

              if 'modifier' in df_modifiers.columns and 'modifier' in df_procedure_modifiers.columns:
                  df_modifiers = df_modifiers[
                      ~df_modifiers['modifier'].isin(df_procedure_modifiers['modifier'])
                  ]

            if not df_chunk_procedure_codes.empty:
                s3_athena_load_table_parquet_snappy(
                    df=df_chunk_procedure_codes,
                    database=ATHENA_OUTPUT_PROCEDURE_CODES_TABLE_SCHEMA,
                    table_name=ATHENA_OUTPUT_PROCEDURE_CODES_TABLE_NAME,
                    table_location=ATHENA_OUTPUT_PROCEDURE_CODES_TABLE_LOCATION,
                    s3_file_prefix=f'{datetime.now().strftime("%Y%m%d")}_',
                    insert_mode='append'
                )
                logger.info(f"Códigos inseridos para o chunk {start_idx}-{end_idx - 1}")
            else:
                logger.info(f"Nenhum código novo para inserir no chunk {start_idx}-{end_idx - 1}")

            if not df_modifiers.empty:
                s3_athena_load_table_parquet_snappy(
                    df=df_modifiers,
                    database=ATHENA_OUTPUT_PROCEDURE_MODIFIERS_TABLE_SCHEMA,
                    table_name=ATHENA_OUTPUT_PROCEDURE_MODIFIERS_TABLE_NAME,
                    table_location=ATHENA_OUTPUT_PROCEDURE_MODIFIERS_TABLE_LOCATION,
                    s3_file_prefix=f'{datetime.now().strftime("%Y%m%d")}_',
                    insert_mode='append'
                )
                logger.info(f"Modifiers inseridos para o chunk {start_idx}-{end_idx - 1}")
            else:
                logger.info(f"Nenhum modifier novo para inserir no chunk {start_idx}-{end_idx - 1}")

            if not df_new_procedure_ndc.empty:
                s3_athena_load_table_parquet_snappy(
                    df=df_new_procedure_ndc,
                    database=ATHENA_OUTPUT_PROCEDURE_NDC_TABLE_SCHEMA,
                    table_name=ATHENA_OUTPUT_PROCEDURE_NDC_TABLE_NAME,
                    table_location=ATHENA_OUTPUT_PROCEDURE_NDC_TABLE_LOCATION,
                    s3_file_prefix=f'{datetime.now().strftime("%Y%m%d")}_',
                    insert_mode='append'
                )
                logger.info(f"NDCs inseridos para o chunk {start_idx}-{end_idx - 1}")
            else:
                logger.info(f"Nenhum NDC novo para inserir no chunk {start_idx}-{end_idx - 1}")

        DRIVER_LOGGED.quit()
    finally:
        logger.info("Processo finalizado.")
