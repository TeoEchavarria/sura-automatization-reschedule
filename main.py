# main.py
from functions import (
    firefox_driver, DriverState, ScrapingAction, By,
    run_block, pipe, navigate, url_to_be
)
import logging
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
logger = logging.getLogger("functional-scraper")

if __name__ == "__main__":
    CC = os.getenv("CC")
    PASS4 = os.getenv("PASSWORD")  # contraseña numérica para el teclado virtual

    logger.info(f"CC: {CC}")
    logger.info(f"PASSWORD: {PASS4}")

    LOGIN_URL = "https://login.sura.com/sso/servicelogin.aspx?continueTo=https%3A%2F%2Fsucursal.segurossura.com.co&service=clienteseguros"

    with firefox_driver(logger, driver_path="/Users/teoechavarria/Downloads/geckodriver", download_folder="./downloads") as drv:
        state = DriverState(driver=drv)

        # Pipeline inicial: navega y valida URL
        state = pipe(
            state,
            navigate(LOGIN_URL),
            url_to_be(LOGIN_URL, timeout=30),
            logger=logger
        )

        # Bloque de acciones
        state, result = run_block(state, [
            ScrapingAction(
                action_type="wait_visible",
                description="Formulario principal",
                locator_by=By.ID,
                locator_path="aspnetForm",
                timeout=25
            ),
            ScrapingAction(
                action_type="wait_visible",
                description="Tab Asesor visible",
                locator_by=By.ID,
                locator_path="tabInternet",
                timeout=25
            ),
            ScrapingAction(
                action_type="select_option",
                description="Tipo de documento",
                locator_by=By.ID,
                locator_path="ctl00_ContentMain_suraType",
                timeout=15,
                keys_to_send="C",  # CEDULA
            ),
            ScrapingAction(
                action_type="safe_send_keys",
                description="Número de identificación",
                locator_by=By.ID,
                locator_path="suraName",
                timeout=20,
                keys_to_send=CC,
            ),
            ScrapingAction(
                action_type="focus_input",
                description="Enfocar contraseña (abre teclado virtual)",
                locator_by=By.ID,
                locator_path="suraPassword",
                timeout=20,
            ),
            ScrapingAction(
                action_type="keyboard_type",
                description="Teclear contraseña por teclado virtual",
                locator_by=None,
                locator_path=None,
                timeout=20,
                keys_to_send=PASS4,
            ),
            ScrapingAction(
                action_type="keyboard_accept",
                description="Aceptar teclado virtual",
                locator_by=None,
                locator_path=None,
                timeout=15,
            ),
            ScrapingAction(
                action_type="click",
                description="Iniciar sesión",
                locator_by=By.ID,
                locator_path="session-internet",
                timeout=60,
            ),
        ], logger)

        logger.info(f"Resultado bloque -> OK={result.successful}, error={result.error}, warnings={result.warnings}")

        state, result = run_block(state, [
            ScrapingAction(
                action_type="wait_visible",
                description="Formulario principal",
                locator_by=By.ID,
                locator_path="navbarNavs",
                timeout=25000
            ),
        ], logger)