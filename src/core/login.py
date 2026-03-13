from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from time import monotonic

from config.settings import BASE_URL, PASSWORD, USERNAME


def realizar_login(driver, wait):
    driver.get(BASE_URL)

    campo_login = wait.until(EC.visibility_of_element_located((By.NAME, "login")))
    campo_senha = wait.until(EC.visibility_of_element_located((By.NAME, "password")))
    def preencher_input(elemento, valor):
        elemento.click()
        elemento.clear()
        elemento.send_keys(valor)
        driver.execute_script(
            "arguments[0].dispatchEvent(new Event('input', { bubbles: true }));",
            elemento,
        )
        driver.execute_script(
            "arguments[0].dispatchEvent(new Event('change', { bubbles: true }));",
            elemento,
        )
        wait.until(lambda d: elemento.get_attribute("value") == valor)

    preencher_input(campo_login, USERNAME)
    preencher_input(campo_senha, PASSWORD)
    campo_senha.send_keys(Keys.TAB)

    form_login = wait.until(
        EC.presence_of_element_located(
            (By.CSS_SELECTOR, "form.style_form-ctn-login__jr3S6")
        )
    )
    def aguardar_intervalo(segundos):
        inicio = monotonic()
        wait.until(lambda d: (monotonic() - inicio) >= segundos)

    aguardar_intervalo(5.0)

    botao_entrar = wait.until(
        EC.element_to_be_clickable(
            (By.CSS_SELECTOR, 'button[type="submit"].style_button-form__WBP_6')
        )
    )
    wait.until(lambda d: not botao_entrar.get_attribute("disabled"))
    try:
        botao_entrar.click()
    except Exception:
        try:
            driver.execute_script("arguments[0].click();", botao_entrar)
        except Exception:
            driver.execute_script("arguments[0].submit();", form_login)

    try:
        wait.until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    "//*[contains(translate(normalize-space(.),"
                    " 'ABCDEFGHIJKLMNOPQRSTUVWXYZ',"
                    " 'abcdefghijklmnopqrstuvwxyz'),"
                    " 'control desk')]",
                )
            )
        )
    except Exception as exc:
        raise RuntimeError("Falha no login: texto 'Control desk' nao encontrado.") from exc
