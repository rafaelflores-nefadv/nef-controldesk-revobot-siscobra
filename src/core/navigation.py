from time import monotonic

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC


def _xpath_menu_por_aria_label(texto):
    texto_esc = texto.replace("'", "\\'")
    return (
        f"//*[@role='treeitem' and @aria-label='{texto_esc}']"
        "//*[contains(@class,'dx-treeview-item-content')]"
    )


def _xpath_menu_por_texto(texto):
    texto_esc = texto.replace("'", "\\'")
    return f"//span[normalize-space(.)='{texto_esc}']/ancestor::*[contains(@class,'dx-treeview-item-content')]"


def _aguardar_intervalo(wait, segundos):
    inicio = monotonic()
    wait.until(lambda d: (monotonic() - inicio) >= segundos)


def click_menu_item(driver, wait, texto):
    locator_aria = (By.XPATH, _xpath_menu_por_aria_label(texto))
    locator_texto = (By.XPATH, _xpath_menu_por_texto(texto))

    try:
        elemento = wait.until(EC.element_to_be_clickable(locator_aria))
    except Exception:
        elemento = wait.until(EC.element_to_be_clickable(locator_texto))
    driver.execute_script(
        "arguments[0].scrollIntoView({block: 'center'});", elemento
    )
    try:
        elemento.click()
    except Exception:
        driver.execute_script("arguments[0].click();", elemento)


def acessar_transferencia_arquivos(driver, wait):
    _aguardar_intervalo(wait, 2.0)
    click_menu_item(driver, wait, "Control desk")
    _aguardar_intervalo(wait, 2.0)
    click_menu_item(driver, wait, "Importação e exportação de arquivos")
    _aguardar_intervalo(wait, 2.0)
    click_menu_item(driver, wait, "Transferência de arquivos")

    wait.until(
        EC.presence_of_element_located(
            (
                By.XPATH,
                "//*[contains(translate(normalize-space(.),"
                " 'ABCDEFGHIJKLMNOPQRSTUVWXYZ',"
                " 'abcdefghijklmnopqrstuvwxyz'),"
                " 'transferência de arquivos')]",
            )
        )
    )
