from time import sleep
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from utils.logger import get_logger

logger = get_logger('login')


def aapc_login(driver, url_login, aapc_email, aapc_pw, username_field_id, password_field_id, primary_login, second_login, second_login_button_id, subscription_menu_selector):
    logger.info("Login on AAPC")
    login_url = url_login
    driver.get(login_url)
    sleep(5)

    login_input = WebDriverWait(driver, 50).until(
        EC.presence_of_element_located((By.ID, username_field_id))
    )
    login_input.send_keys(aapc_email)

    continue_button = WebDriverWait(driver, 20).until(
        EC.element_to_be_clickable((By.ID, primary_login))
    )
    continue_button.click()

    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, password_field_id))
    )

    password_input = driver.find_element(By.ID, password_field_id)
    password_input.send_keys(aapc_pw)

    signin_button = WebDriverWait(driver, 20).until(
        EC.element_to_be_clickable((By.ID, second_login))
    )
    signin_button.click()
    

    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.ID, second_login_button_id))
    )
        
    login2 = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, f"#{second_login_button_id}"))
    )
    login2.click()
    
    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.ID, second_login_button_id))
    )
        
    login3 = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, f"#{second_login_button_id}"))
    )
    login3.click()
        
    
    login_input = WebDriverWait(driver, 50).until(
        EC.presence_of_element_located((By.ID, username_field_id))
    )
    login_input.send_keys(aapc_email)

    continue_button = WebDriverWait(driver, 20).until(
        EC.element_to_be_clickable((By.ID, primary_login))
    )
    continue_button.click()

    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, password_field_id))
    )

    password_input = driver.find_element(By.ID, password_field_id)
    password_input.send_keys(aapc_pw)

    signin_button = WebDriverWait(driver, 20).until(
        EC.element_to_be_clickable((By.ID, second_login))
    )
    signin_button.click()


    codify_link = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, "#ctl00_Body_ctl00_mnuCodifySubscription a"))
    )

    codify_link.click()

    logger.info("Usuário logado com sucesso! Elemento de logout encontrado:")