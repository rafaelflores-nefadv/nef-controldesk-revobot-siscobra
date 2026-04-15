import logging
import unicodedata

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from time import monotonic

from config.settings import BASE_URL, PASSWORD, USERNAME

logger = logging.getLogger(__name__)


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

    form_login = wait.until(
        EC.presence_of_element_located(
            (
                By.XPATH,
                "//form[.//input[@name='login'] and .//input[@name='password']]",
            )
        )
    )

    botao_entrar_xpath = (
        "//form[.//input[@name='login'] and .//input[@name='password']]"
        "//button[@type='submit']"
    )

    def aguardar_intervalo(segundos):
        inicio = monotonic()
        wait.until(lambda d: (monotonic() - inicio) >= segundos)

    def normalizar_texto(valor):
        return (
            unicodedata.normalize("NFKD", valor or "")
            .encode("ascii", "ignore")
            .decode("ascii")
            .lower()
        )

    def login_confirmado():
        current_url = (driver.current_url or "").lower()
        if "/login" in current_url:
            return False
        pagina = (driver.page_source or "").lower()
        return (
            "control-desk" in current_url
            or "control desk" in pagina
            or "file manager" in pagina
            or not driver.find_elements(By.NAME, "login")
        )

    def obter_botao_entrar():
        return wait.until(EC.presence_of_element_located((By.XPATH, botao_entrar_xpath)))

    def submeter_form_login():
        botao_entrar = obter_botao_entrar()
        wait.until(
            lambda d: (
                botao_entrar.is_enabled() and not botao_entrar.get_attribute("disabled")
            )
        )
        try:
            driver.execute_script(
                "arguments[0].scrollIntoView({block: 'center', inline: 'center'});",
                botao_entrar,
            )
            botao_entrar.click()
        except Exception:
            try:
                driver.execute_script("arguments[0].click();", botao_entrar)
            except Exception:
                try:
                    campo_senha.send_keys(Keys.ENTER)
                except Exception:
                    driver.execute_script("arguments[0].requestSubmit();", form_login)

    def fechar_popup_conexao_se_aparecer():
        popups = driver.find_elements(
            By.CSS_SELECTOR, "div.dx-overlay-content.dx-popup-normal[role='dialog']"
        )
        for popup in popups:
            try:
                if not popup.is_displayed():
                    continue
                popup_texto = normalizar_texto(popup.text)
                if (
                    "aguardando conexao com o servidor" not in popup_texto
                    and "tente novamente em instantes" not in popup_texto
                ):
                    continue

                botoes_ok = popup.find_elements(
                    By.XPATH,
                    ".//*[@role='button' and ("
                    " translate(@aria-label,'OK','ok')='ok'"
                    " or normalize-space(.)='Ok'"
                    " or .//span[normalize-space()='Ok']"
                    " )]",
                )

                if not botoes_ok:
                    continue

                botao_ok = botoes_ok[0]
                click_ok = False
                try:
                    botao_ok.click()
                    click_ok = True
                except Exception:
                    pass

                if not click_ok:
                    try:
                        driver.execute_script("arguments[0].click();", botao_ok)
                        click_ok = True
                    except Exception:
                        pass

                if not click_ok:
                    try:
                        driver.execute_script(
                            """
                            const el = arguments[0];
                            ['pointerdown','mousedown','mouseup','click'].forEach((evt) => {
                              el.dispatchEvent(new MouseEvent(evt, { bubbles: true, cancelable: true, view: window }));
                            });
                            """,
                            botao_ok,
                        )
                        click_ok = True
                    except Exception:
                        pass

                if not click_ok:
                    try:
                        botao_ok.send_keys(Keys.ENTER)
                        click_ok = True
                    except Exception:
                        pass

                if not click_ok:
                    continue

                WebDriverWait(driver, 3).until(
                    lambda d: (
                        not popup.is_displayed()
                        or "aguardando conexao com o servidor"
                        not in normalizar_texto(popup.text)
                    )
                )
                logger.warning(
                    "Popup de conexao com o servidor detectado no login e confirmado no botao Ok."
                )
                return True
            except Exception:
                continue
        return False

    aguardar_intervalo(0.4)
    submeter_form_login()

    timeout_login = float(getattr(wait, "_timeout", 30))
    deadline = monotonic() + timeout_login
    ultimo_submit = monotonic()

    while monotonic() < deadline:
        if login_confirmado():
            return

        popup_fechado = fechar_popup_conexao_se_aparecer()
        if popup_fechado:
            aguardar_intervalo(0.5)
            submeter_form_login()
            ultimo_submit = monotonic()
            continue

        if "/login" in (driver.current_url or "").lower() and (monotonic() - ultimo_submit) >= 4.0:
            submeter_form_login()
            ultimo_submit = monotonic()

        aguardar_intervalo(0.25)

    raise RuntimeError(
        f"Falha no login: autenticacao nao confirmada. URL atual: {driver.current_url}"
    )
