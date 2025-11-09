# functions.py
from __future__ import annotations

import os
import time
import shutil
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, List, Optional, Tuple, Dict
from logging import Logger

from selenium import webdriver
from selenium.webdriver.firefox.service import Service as FFService
from selenium.webdriver.firefox.options import Options as FFOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    StaleElementReferenceException,
    ElementClickInterceptedException,
    ElementNotInteractableException,
    TimeoutException,
    NoSuchElementException,
)

# -------------------------------------------------------------------
# MODELOS
# -------------------------------------------------------------------

@dataclass(frozen=True)
class ScrapingAction:
    action_type: str
    description: str
    locator_by: Optional[By] = None
    locator_path: Optional[str] = None
    timeout: int = 30
    target_element: Any = None
    keys_to_send: Optional[str] = None


@dataclass(frozen=True)
class DriverState:
    driver: webdriver.Firefox
    scope: Optional[Any] = None
    last: Optional[Any] = None
    warnings: Tuple[str, ...] = field(default_factory=tuple)

    def with_updates(self, **updates) -> "DriverState":
        return DriverState(
            driver=updates.get("driver", self.driver),
            scope=updates.get("scope", self.scope),
            last=updates.get("last", self.last),
            warnings=tuple(updates.get("warnings", self.warnings)),
        )


@dataclass(frozen=True)
class ScrapingResult:
    duration: float
    successful: bool
    error: Optional[str]
    warnings: List[str]
    last_result: Optional[Any]


Step = Callable[[DriverState, Logger], DriverState]


# -------------------------------------------------------------------
# PIPELINE FUNCIONAL
# -------------------------------------------------------------------

def pipe(state: DriverState, *steps: Step, logger: Logger) -> DriverState:
    for step in steps:
        state = step(state, logger)
    return state


# -------------------------------------------------------------------
# CONTEXT MANAGER: DRIVER FIREFOX
# -------------------------------------------------------------------

@contextmanager
def firefox_driver(
    logger: Logger,
    driver_path: str = "geckodriver",
    download_folder: Optional[str] = None,
) -> Iterable[webdriver.Firefox]:
    if not os.path.isabs(driver_path):
        driver_path = os.path.join(os.getcwd(), driver_path)
    if download_folder is not None and not os.path.isabs(download_folder):
        download_folder = os.path.join(os.getcwd(), download_folder)

    options = FFOptions()
    options.set_preference("network.http.http3.enabled", False)
    options.set_preference("security.tls.version.min", 1)
    options.set_preference("security.tls.version.max", 4)
    options.set_preference("dom.disable_beforeunload", True)
    options.set_preference("dom.disable_open_during_load", False)
    options.set_preference("browser.link.open_newwindow", 1)
    options.set_preference("browser.link.open_newwindow.restriction", 0)

    if download_folder is not None:
        os.makedirs(download_folder, exist_ok=True)
        logger.info(f"Download folder: {download_folder}")
        options.set_preference("browser.download.dir", download_folder)
    options.set_preference("browser.download.folderList", 2)
    options.set_preference(
        "browser.helperApps.neverAsk.saveToDisk",
        "application/zip,application/pdf",
    )
    options.set_preference("pdfjs.disabled", True)

    # headless_mode = os.getenv("HEADLESS_MODE", "true").lower() == "true"
    # if headless_mode:
    #     options.add_argument("--headless")

    service = FFService(driver_path)
    driver = webdriver.Firefox(service=service, options=options)
    logger.info("Firefox Selenium driver iniciado")

    try:
        yield driver
    finally:
        driver.quit()
        logger.info("Firefox Selenium driver cerrado")


# -------------------------------------------------------------------
# DECORADOR DE REINTENTOS
# -------------------------------------------------------------------

def with_retries(
    attempts: int = 4,
    base_delay: float = 0.6,
    retry_on: Tuple[type, ...] = (
        StaleElementReferenceException,
        ElementClickInterceptedException,
        ElementNotInteractableException,
        TimeoutException,
        NoSuchElementException,
    ),
) -> Callable[[Step], Step]:
    """
    Reintenta un Step ante errores transientes típicos de Selenium.
    Usa backoff exponencial: base_delay * (2 ** (intento - 1)).
    """
    def _decorator(step_fn: Step) -> Step:
        def _wrapped(state: DriverState, logger: Logger) -> DriverState:
            last_exc: Optional[Exception] = None
            for i in range(1, attempts + 1):
                try:
                    return step_fn(state, logger)
                except retry_on as e:
                    last_exc = e
                    logger.warning(
                        f"[{step_fn.__name__}] retry {i}/{attempts}: {e.__class__.__name__}"
                    )
                    time.sleep(base_delay * (2 ** (i - 1)))
            logger.error(f"[{step_fn.__name__}] failed after {attempts} attempts: {last_exc}")
            raise last_exc
        return _wrapped
    return _decorator


# -------------------------------------------------------------------
# HELPERS DE ESPERA / LOCALIZACIÓN
# -------------------------------------------------------------------

def _ctx(state: DriverState) -> Any:
    return state.scope if state.scope is not None else state.driver


def _wait(
    state: DriverState,
    by: By,
    path: str,
    timeout: int,
    cond: str = "presence",  # presence|visible|clickable
) -> Any:
    context = _ctx(state)
    wait = WebDriverWait(context, timeout)
    locator = (by, path)
    if cond == "visible":
        return wait.until(EC.visibility_of_element_located(locator))
    if cond == "clickable":
        return wait.until(EC.element_to_be_clickable(locator))
    return wait.until(EC.presence_of_element_located(locator))


def _find_all(state: DriverState, by: By, path: str) -> List[Any]:
    parent = _ctx(state)
    return parent.find_elements(by, path)


def _update_last(state: DriverState, el: Any) -> DriverState:
    return state.with_updates(last=el)


# -------------------------------------------------------------------
# DICCIONARIO DEL SELECT (TIPO DOCUMENTO)
# -------------------------------------------------------------------

DOCUMENT_TYPES: Dict[str, str] = {
    "": "",
    "C": "CEDULA",
    "E": "CEDULA EXTRANJERIA",
    "D": "DIPLOMATICO",
    "X": "DOC.IDENT. DE EXTRANJEROS",
    "F": "IDENT. FISCAL PARA EXT.",
    "A": "NIT",
    "CA": "NIT PERSONAS NATURALES",
    "N": "NUIP",
    "P": "PASAPORTE",
    "R": "REGISTRO CIVIL",
    "T": "TARJ.IDENTIDAD",
    "TC": "CERTIFICADO NACIDO VIVO",
    "TP": "PASAPORTE ONU",
    "TE": "PERMISO ESPECIAL PERMANENCIA",
    "TS": "SALVOCONDUCTO DE PERMANENCIA",
    "TF": "PERMISO ESPECIAL FORMACN PEPFF",
    "TT": "PERMISO POR PROTECCION TEMPORL",
}


# -------------------------------------------------------------------
# STEPS BÁSICOS (TODOS CON @with_retries CUANDO APLICA)
# -------------------------------------------------------------------

def navigate(url: str) -> Step:
    def _step(state: DriverState, logger: Logger) -> DriverState:
        state.driver.get(url)
        logger.info(f"Navigate -> {url}")
        return state
    return _step


def url_to_be(url: str, timeout: int = 30) -> Step:
    @with_retries()
    def _step(state: DriverState, logger: Logger) -> DriverState:
        WebDriverWait(state.driver, timeout).until(EC.url_to_be(url))
        logger.info(f"URL to be: {url}")
        return state
    return _step


def wait_visible(action: ScrapingAction) -> Step:
    @with_retries()
    def _step(state: DriverState, logger: Logger) -> DriverState:
        el = _wait(state, action.locator_by, action.locator_path, action.timeout, cond="visible")
        logger.info(f"Visible: {action.description}")
        return _update_last(state, el)
    return _step


def wait_invisible(action: ScrapingAction) -> Step:
    @with_retries()
    def _step(state: DriverState, logger: Logger) -> DriverState:
        WebDriverWait(state.driver, action.timeout).until(
            EC.invisibility_of_element_located((action.locator_by, action.locator_path))
        )
        logger.info(f"Invisible: {action.description}")
        return state
    return _step


def wait_clickable(action: ScrapingAction, *, click: bool = False) -> Step:
    @with_retries()
    def _step(state: DriverState, logger: Logger) -> DriverState:
        _wait(state, action.locator_by, action.locator_path, action.timeout, cond="clickable")
        els = _find_all(state, action.locator_by, action.locator_path)
        if not els:
            raise NoSuchElementException(f"No encontrado: {action.description}")
        el = els[0]
        if click:
            try:
                state.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
            except Exception:
                pass
            el.click()
            logger.info(f"Click: {action.description}")
        else:
            logger.info(f"Clickable: {action.description}")
        return _update_last(state, el)
    return _step


def safe_send_keys(action: ScrapingAction) -> Step:
    @with_retries()
    def _step(state: DriverState, logger: Logger) -> DriverState:
        s2 = wait_clickable(action, click=False)(state, logger)
        el = s2.last
        el.clear()
        el.send_keys(action.keys_to_send or "")
        logger.info(f"Send keys: {action.description}")
        return s2
    return _step


def select_option_by_value(action: ScrapingAction) -> Step:
    @with_retries()
    def _step(state: DriverState, logger: Logger) -> DriverState:
        el = _wait(state, By.ID, action.locator_path, action.timeout, cond="visible")
        Select(el).select_by_value(action.keys_to_send)
        label = DOCUMENT_TYPES.get(action.keys_to_send, action.keys_to_send)
        logger.info(f"Select '{label}' ({action.keys_to_send}) en #{action.locator_path}")
        return _update_last(state, el)
    return _step


def switch_to_iframe(action: ScrapingAction) -> Step:
    @with_retries()
    def _step(state: DriverState, logger: Logger) -> DriverState:
        WebDriverWait(state.driver, action.timeout).until(
            EC.frame_to_be_available_and_switch_to_it((action.locator_by, action.locator_path))
        )
        logger.info(f"Iframe: {action.description}")
        return state
    return _step


# -------------------------------------------------------------------
# TECLADO VIRTUAL (PASSWORD)
# -------------------------------------------------------------------

def focus_input(action: ScrapingAction) -> Step:
    """
    Enfoca el input que dispara el teclado virtual (ej: #suraPassword).
    """
    @with_retries()
    def _step(state: DriverState, logger: Logger) -> DriverState:
        el = _wait(state, action.locator_by, action.locator_path, action.timeout, cond="visible")
        try:
            state.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
        except Exception:
            pass
        el.click()
        logger.info(f"Focus input: {action.description}")
        return _update_last(state, el)
    return _step


def keyboard_type_digits(action: ScrapingAction) -> Step:
    """
    Escribe una secuencia de dígitos usando el teclado virtual:
    - keyset visible: div.ui-keyboard-keyset.ui-keyboard-keyset-default[style*='display: block']
    - botón: button.ui-keyboard-button[data-value='X']
    """
    @with_retries(attempts=5, base_delay=0.5)
    def _step(state: DriverState, logger: Logger) -> DriverState:
        keyset = WebDriverWait(state.driver, action.timeout).until(
            EC.visibility_of_element_located((
                By.CSS_SELECTOR,
                "div.ui-keyboard-keyset.ui-keyboard-keyset-default[style*='display: block']"
            ))
        )
        digits = list(action.keys_to_send or "")
        if not digits:
            return state

        for d in digits:
            btn = WebDriverWait(keyset, action.timeout).until(
                EC.element_to_be_clickable((
                    By.CSS_SELECTOR,
                    f"button.ui-keyboard-button[data-value='{d}']"
                ))
            )
            try:
                state.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            except Exception:
                pass
            btn.click()
            logger.info(f"[keyboard] click '{d}'")
            time.sleep(0.05)

        return state
    return _step


def keyboard_accept(action: Optional[ScrapingAction] = None) -> Step:
    """
    Pulsa el botón 'Aceptar' del teclado virtual.
    """
    @with_retries(attempts=5, base_delay=0.5)
    def _step(state: DriverState, logger: Logger) -> DriverState:
        keyset = WebDriverWait(state.driver, 15).until(
            EC.visibility_of_element_located((
                By.CSS_SELECTOR,
                "div.ui-keyboard-keyset.ui-keyboard-keyset-default[style*='display: block']"
            ))
        )
        accept_btn = WebDriverWait(keyset, 10).until(
            EC.element_to_be_clickable((
                By.CSS_SELECTOR,
                "button.ui-keyboard-button.ui-keyboard-accept, button[name='accept']"
            ))
        )
        accept_btn.click()
        logger.info("[keyboard] Accept")
        return state
    return _step

# -------------------------------------------------------------------
# EXTRACCIONES ESPECÍFICAS: CITA PENDIENTE Y TAB DE FECHA
# -------------------------------------------------------------------

def extract_first_pending_appointment(action: ScrapingAction) -> Step:
    """
    Toma la PRIMERA cita pendiente que encuentre (primer .tarjetaCita__fecha)
    y guarda en state.last un dict: {"date": "...", "time": "..."}.
    """
    @with_retries()
    def _step(state: DriverState, logger: Logger) -> DriverState:
        # Localiza el contenedor de fecha de la cita
        el = _wait(
            state,
            action.locator_by,
            action.locator_path,
            action.timeout,
            cond="visible",
        )
        spans = el.find_elements(By.CSS_SELECTOR, "span")
        date_text = spans[0].text.strip() if len(spans) > 0 else ""
        time_text = spans[1].text.strip() if len(spans) > 1 else ""
        data = {"date": date_text, "time": time_text}
        logger.info(f"Cita pendiente encontrada: {data['date']} - {data['time']}")
        return state.with_updates(last=data)
    return _step


def extract_tab_date(action: ScrapingAction) -> Step:
    """
    Lee la fecha del TAB activo de reprogramación.
    - Primero intenta aria-label (ej: '2025-11-15')
    - Si no hay, usa el texto visible del tab.
    Guarda en state.last un string.
    """
    @with_retries()
    def _step(state: DriverState, logger: Logger) -> DriverState:
        el = _wait(
            state,
            action.locator_by,
            action.locator_path,
            action.timeout,
            cond="visible",
        )
        label = el.get_attribute("aria-label") or el.text
        label = label.strip()
        logger.info(f"Fecha de tab activo: {label}")
        return state.with_updates(last=label)
    return _step


# -------------------------------------------------------------------
# MAPEADOR DE ACTION_TYPE -> STEP
# -------------------------------------------------------------------

def step_from_action(action: ScrapingAction) -> Step:
    at = action.action_type
    if at == "wait_visible":
        return wait_visible(action)
    if at == "wait_invisible":
        return wait_invisible(action)
    if at == "click":
        return wait_clickable(action, click=True)
    if at == "safe_send_keys":
        return safe_send_keys(action)
    if at == "switch_to_iframe":
        return switch_to_iframe(action)
    if at == "select_option":
        return select_option_by_value(action)
    if at == "focus_input":
        return focus_input(action)
    if at == "keyboard_type":
        return keyboard_type_digits(action)
    if at == "keyboard_accept":
        return keyboard_accept(action)
    if at == "extract_appointment_date":
        return extract_first_pending_appointment(action)
    if at == "extract_tab_date":
        return extract_tab_date(action)
    raise ValueError(f"Action type no soportado: {at}")


# -------------------------------------------------------------------
# EJECUCIÓN DE BLOQUES (run_block)
# -------------------------------------------------------------------

def run_block(
    state: DriverState,
    actions: List[ScrapingAction],
    logger: Logger,
    before_retry_block: Optional[Callable[[], None]] = None,
    attempts: int = 3,
    delay: float = 5.0,
) -> Tuple[DriverState, ScrapingResult]:
    """
    Ejecuta una lista de ScrapingAction como un bloque.
    - Si algún Step lanza excepción después de sus reintentos internos, reintenta TODO el bloque.
    """
    start = time.time()
    last_state = state
    last_error: Optional[str] = None
    ok = True

    for i in range(1, attempts + 1):
        try:
            s = last_state
            for act in actions:
                step = step_from_action(act)
                s = step(s, logger)
            last_state = s
            ok = True
            last_error = None
            break
        except Exception as e:
            ok = False
            last_error = str(e)
            logger.error(f"[run_block retry {i}/{attempts}] {e}")
            if before_retry_block:
                try:
                    before_retry_block()
                except Exception as be:
                    logger.warning(f"before_retry_block error: {be}")
            time.sleep(delay)

    result = ScrapingResult(
        duration=time.time() - start,
        successful=ok,
        error=last_error,
        warnings=list(last_state.warnings),
        last_result=last_state.last,
    )
    return last_state, result


# -------------------------------------------------------------------
# OPCIONAL: ESPERAR DESCARGA (POR SI LO NECESITAS)
# -------------------------------------------------------------------

def wait_for_download(
    download_dir: str,
    timeout: int,
    file_name: Optional[str] = None,
    move_to: Optional[str] = None,
    logger: Optional[Logger] = None,
) -> str:
    """
    Espera a que un archivo en download_dir termine de descargarse (sin .part y tamaño estable).
    """
    if move_to is not None and not os.path.isdir(move_to):
        os.makedirs(move_to)

    t0 = time.time()
    while time.time() - t0 <= timeout:
        files = [
            f for f in os.listdir(download_dir)
            if os.path.isfile(os.path.join(download_dir, f)) and not f.endswith(".part")
        ]
        if file_name:
            files = [f for f in files if file_name in f]

        for fname in files:
            src = os.path.join(download_dir, fname)
            size1 = os.path.getsize(src)
            time.sleep(1)
            size2 = os.path.getsize(src)
            if size1 == size2 and size2 > 0:
                if move_to:
                    dst = os.path.join(move_to, fname)
                    try:
                        shutil.move(src, dst)
                        if logger:
                            logger.info(f"Descarga movida a: {dst}")
                        return dst
                    except Exception as e:
                        if logger:
                            logger.warning(f"No se pudo mover {fname}: {e}")
                        return src
                else:
                    if logger:
                        logger.info(f"Descarga lista: {src}")
                    return src

        time.sleep(1)

    raise TimeoutException("Timeout esperando descarga.")