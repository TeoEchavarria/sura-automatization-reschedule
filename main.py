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

         # --- BLOQUE 2: SALUD -> CITAS DE SALUD -> PENDIENTES + REPROGRAMAR ---

        state, citas_result = run_block(state, [
            # 1) Esperar a que aparezca el sura-modal
            ScrapingAction(
                action_type="wait_visible",
                description="Modal principal de portal (sura-modal)",
                locator_by=By.XPATH,
                locator_path="/html/body/app-root/app-portal/sura-modal",
                timeout=30
            ),
            # 2) Click en Salud
            ScrapingAction(
                action_type="click",
                description="Selección módulo Salud",
                locator_by=By.XPATH,
                locator_path="//li[contains(@class,'list')]//button[.//span[contains(@class,'title') and normalize-space()='Salud']]",
                timeout=30
            ),
            # 3) Click en botón 'Citas de salud'
            ScrapingAction(
                action_type="click",
                description="Ir a Citas de salud",
                locator_by=By.XPATH,
                locator_path="//button[contains(@class,'item') and .//span[contains(normalize-space(),'Citas de salud')]]",
                timeout=30
            ),
            # 4) Click en 'Citas pendientes'
            ScrapingAction(
                action_type="click",
                description="Ir a Citas pendientes",
                locator_by=By.ID,
                locator_path="irCitasPendientes",
                timeout=30
            ),
            # 5) Extraer fecha de la primera cita pendiente
            #    Usamos el contenedor .tarjetaCita__fecha
            ScrapingAction(
                action_type="extract_appointment_date",
                description="Extraer fecha de primera cita pendiente",
                locator_by=By.CSS_SELECTOR,
                locator_path="div.tarjetaCita__fecha",
                timeout=30
            ),
            # 6) Click en 'Reprogramar' de esa cita
            ScrapingAction(
                action_type="click",
                description="Click en Reprogramar cita",
                locator_by=By.ID,
                locator_path="reagendarCita",
                timeout=30
            ),
            # 7) Extraer la fecha del TAB activo (reprogramación)
            ScrapingAction(
                action_type="extract_tab_date",
                description="Extraer fecha del día seleccionado (tab activo)",
                locator_by=By.CSS_SELECTOR,
                locator_path="div.mdc-tab.mdc-tab--active[role='tab']",
                timeout=30
            ),
        ], logger)

        logger.info(f"Citas bloque OK={citas_result.successful}, error={citas_result.error}")

        # El resultado de la acción 5 (extract_appointment_date) y 7 (extract_tab_date)
        # queda en citas_result.last_result (la ÚLTIMA acción, o sea el tab).
        # Pero la fecha de la cita original se guardó en state.last justo después de la acción 5,
        # así que es mejor hacer dos bloques o leerlo en el momento.
        # Aquí, como ejemplo, lo recogemos desde la penúltima ejecución usando otra estrategia:
        # más práctico: mirar en logs o, si prefieres, hacer dos bloques y usar ambas last_result.

        logger.info(f"Último valor extraído (tab activo): {citas_result.last_result}")