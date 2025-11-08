# functional_scraper.py
# -*- coding: utf-8 -*-
"""
Scraping con Selenium en estilo funcional:
- Estado explícito (DriverState) que fluye entre pasos
- Pasos (= funciones) composables (pipe/compose)
- Efectos a los bordes (crear/cerrar driver); transformaciones puras en el centro
- Decorador retry funcional para resiliencia
"""

from __future__ import annotations

import os, time, shutil
from dataclasses import dataclass, field
from typing import Callable, Iterable, List, Optional, Tuple, Any, Union
from contextlib import contextmanager
from logging import Logger

# Selenium
from selenium import webdriver
from selenium.webdriver.firefox.service import Service as FFService
from selenium.webdriver.firefox.options import Options as FFOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# --------------------------------------------------------------------------------------
# Modelos opcionales (descomenta si no importas desde tu paquete mvmes.*)
# --------------------------------------------------------------------------------------
# from mvmes.scraping_helper.models.scrapping_action import ScrapingAction
# from mvmes.scraping_helper.models.scrapping_result_model import ScrapingResult

@dataclass(frozen=True)
class ScrapingAction:  # Compatibilidad simple
    action_type: str
    description: str
    locator_by: Optional[By] = None
    locator_path: Optional[str] = None
    timeout: int = 30
    target_element: Any = None
    keys_to_send: Optional[str] = None

@dataclass(frozen=True)
class ScrapingResult:  # Compatibilidad simple
    duration: float
    successful: bool
    error: Optional[str]
    warnings: List[str]
    last_result: Optional[Any]

# --------------------------------------------------------------------------------------
# Estado que viaja por el pipeline
# --------------------------------------------------------------------------------------

@dataclass(frozen=True)
class DriverState:
    driver: webdriver.Firefox
    scope: Optional[Any] = None           # WebElement para búsquedas anidadas
    last: Optional[Any] = None            # último elemento encontrado/usado
    warnings: Tuple[str, ...] = field(default_factory=tuple)

    def with_updates(self, **updates) -> "DriverState":
        # Crea una nueva copia inmutable con cambios
        return DriverState(
            driver=updates.get("driver", self.driver),
            scope=updates.get("scope", self.scope),
            last=updates.get("last", self.last),
            warnings=tuple(updates.get("warnings", self.warnings)),
        )

# --------------------------------------------------------------------------------------
# Helpers funcionales
# --------------------------------------------------------------------------------------

Step = Callable[[DriverState, Logger], DriverState]

def pipe(state: DriverState, *steps: Step, logger: Logger) -> DriverState:
    for step in steps:
        state = step(state, logger)
    return state

def compose(*steps: Step) -> Step:
    def _composed(state: DriverState, logger: Logger) -> DriverState:
        for step in steps:
            state = step(state, logger)
        return state
    return _composed

def _append_warning(state: DriverState, msg: str) -> DriverState:
    return state.with_updates(warnings=state.warnings + (msg,))

def retry(attempts: int = 3, delay: float = 2.0) -> Callable[[Step], Step]:
    """Decorador de alto orden para reintentar pasos del pipeline."""
    def _decorator(step_fn: Step) -> Step:
        def _wrapped(state: DriverState, logger: Logger) -> DriverState:
            last_exc: Optional[Exception] = None
            for i in range(1, attempts + 1):
                try:
                    return step_fn(state, logger)
                except Exception as e:
                    last_exc = e
                    logger.warning(f"[retry {i}/{attempts}] {e}")
                    time.sleep(delay)
            # Si falla definitivamente, agregamos warning y relanzamos
            state2 = _append_warning(state, f"Retry limit reached for step: {step_fn.__name__}")
            raise last_exc  # deja rastro y permite manejo arriba si se desea
        return _wrapped
    return _decorator

# --------------------------------------------------------------------------------------
# Creación y cierre de driver (efectos en los bordes)
# --------------------------------------------------------------------------------------

@contextmanager
def firefox_driver(logger: Logger,
                   driver_path: str = "geckodriver",
                   download_folder: Optional[str] = None) -> Iterable[webdriver.Firefox]:
    """Context manager: crea/cierra el driver (efecto de E/S)."""
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
    # Añade los mime-types que necesites
    options.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/zip,application/pdf")
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

# --------------------------------------------------------------------------------------
# Pasos atómicos (componibles)
# --------------------------------------------------------------------------------------

def navigate(url: str) -> Step:
    def _step(state: DriverState, logger: Logger) -> DriverState:
        state.driver.get(url)
        logger.info(f"Navigate -> {url}")
        return state
    return _step

def refresh() -> Step:
    def _step(state: DriverState, logger: Logger) -> DriverState:
        state.driver.refresh()
        logger.info("Refresh")
        return state
    return _step

def url_to_be(url: str, timeout: int = 30) -> Step:
    @retry(attempts=3, delay=2)
    def _step(state: DriverState, logger: Logger) -> DriverState:
        WebDriverWait(state.driver, timeout).until(EC.url_to_be(url))
        logger.info(f"URL to be: {url}")
        return state
    return _step

def _wait_ctx(state: DriverState, action: ScrapingAction) -> Any:
    """
    Hack útil: Selenium permite usar WebElement en WebDriverWait para buscar relativo.
    """
    return state.driver if action.target_element is None else action.target_element

def wait_visible(action: ScrapingAction) -> Step:
    @retry(attempts=3, delay=2)
    def _step(state: DriverState, logger: Logger) -> DriverState:
        ctx = _wait_ctx(state, action)
        element = WebDriverWait(ctx, action.timeout).until(
            EC.visibility_of_element_located((action.locator_by, action.locator_path))
        )
        logger.info(f"Visible: {action.description}")
        return state.with_updates(last=element)
    return _step

def wait_clickable(action: ScrapingAction, click: bool = False) -> Step:
    @retry(attempts=3, delay=2)
    def _step(state: DriverState, logger: Logger) -> DriverState:
        ctx = _wait_ctx(state, action)
        WebDriverWait(ctx, action.timeout).until(
            EC.element_to_be_clickable((action.locator_by, action.locator_path))
        )
        # Buscar el elemento relativo a scope o driver
        parent = state.scope if state.scope is not None else state.driver
        el_list = parent.find_elements(action.locator_by, action.locator_path)
        if not el_list:
            raise RuntimeError(f"Elemento no encontrado: {action.description}")
        el = el_list[0]
        if click:
            el.click()
        logger.info(f"Clickable: {action.description}" + (" (clicked)" if click else ""))
        return state.with_updates(last=el)
    return _step

def wait_invisible(action: ScrapingAction) -> Step:
    @retry(attempts=3, delay=5)
    def _step(state: DriverState, logger: Logger) -> DriverState:
        WebDriverWait(state.driver, action.timeout).until(
            EC.invisibility_of_element_located((action.locator_by, action.locator_path))
        )
        logger.info(f"Invisible: {action.description}")
        return state
    return _step

def switch_to_iframe(action: ScrapingAction) -> Step:
    @retry(attempts=5, delay=8)
    def _step(state: DriverState, logger: Logger) -> DriverState:
        WebDriverWait(state.driver, action.timeout).until(
            EC.frame_to_be_available_and_switch_to_it((action.locator_by, action.locator_path))
        )
        logger.info(f"Switch iframe: {action.description}")
        return state  # el driver cambia de contexto; el estado lo refleja implícitamente
    return _step

def safe_send_keys(action: ScrapingAction) -> Step:
    @retry(attempts=3, delay=5)
    def _step(state: DriverState, logger: Logger) -> DriverState:
        # Reutilizamos wait_clickable para obtener el elemento
        next_state = wait_clickable(action, click=False)(state, logger)
        el = next_state.last
        el.clear()
        el.send_keys(action.keys_to_send or "")
        logger.info(f"Send keys: {action.description}")
        return next_state
    return _step

def set_scope_from_last() -> Step:
    """Establece el último elemento como 'scope' (búsquedas anidadas posteriores)."""
    def _step(state: DriverState, logger: Logger) -> DriverState:
        if state.last is None:
            return _append_warning(state, "No hay 'last' para fijar como 'scope'")
        return state.with_updates(scope=state.last)
    return _step

def clear_scope() -> Step:
    def _step(state: DriverState, logger: Logger) -> DriverState:
        return state.with_updates(scope=None)
    return _step

# --------------------------------------------------------------------------------------
# Ejecutor funcional de acciones de alto nivel (si quieres mapear tipos a pasos)
# --------------------------------------------------------------------------------------

def step_from_action(action: ScrapingAction) -> Step:
    if action.action_type == "click":
        return wait_clickable(action, click=True)
    if action.action_type == "wait_visible":
        return wait_visible(action)
    if action.action_type == "wait_invisible":
        return wait_invisible(action)
    if action.action_type == "safe_send_keys":
        return safe_send_keys(action)
    if action.action_type == "switch_to_iframe":
        return switch_to_iframe(action)
    raise ValueError(f"Action type no soportado: {action.action_type}")

def run_action(state: DriverState, action: ScrapingAction, logger: Logger) -> Tuple[DriverState, ScrapingResult]:
    t0 = time.time()
    try:
        state2 = step_from_action(action)(state, logger)
        return state2, ScrapingResult(
            duration=time.time() - t0,
            successful=True,
            error=None,
            warnings=list(state2.warnings),
            last_result=state2.last,
        )
    except Exception as e:
        state_err = _append_warning(state, f"Error ejecutando: {action.description}")
        logger.error(f"Error: {action.description} -> {e}")
        return state_err, ScrapingResult(
            duration=time.time() - t0,
            successful=False,
            error=str(e),
            warnings=list(state_err.warnings),
            last_result=state_err.last,
        )

def run_block(state: DriverState,
              actions: List[ScrapingAction],
              logger: Logger,
              before_retry_block: Optional[Callable[[], None]] = None,
              attempts: int = 3,
              delay: float = 5.0) -> Tuple[DriverState, ScrapingResult]:
    """
    Ejecuta una lista de acciones con reintentos del BLOQUE completo (no solo pasos individuales).
    """
    t0 = time.time()
    last_state = state
    err: Optional[str] = None
    ok = True
    for i in range(1, attempts + 1):
        try:
            s = last_state
            for act in actions:
                s, _ = run_action(s, act, logger)
            last_state = s
            ok = True
            err = None
            break
        except Exception as e:
            ok = False
            err = str(e)
            logger.error(f"[block retry {i}/{attempts}] {e}")
            if before_retry_block:
                try:
                    before_retry_block()
                except Exception as be:
                    logger.warning(f"before_retry_block error: {be}")
            time.sleep(delay)

    return last_state, ScrapingResult(
        duration=time.time() - t0,
        successful=ok,
        error=err,
        warnings=list(last_state.warnings),
        last_result=last_state.last,
    )

# --------------------------------------------------------------------------------------
# Descargas (funcional con efectos mínimos y retornos explícitos)
# --------------------------------------------------------------------------------------

def wait_for_download(download_dir: str,
                      timeout: int,
                      file_name: Optional[str] = None,
                      move_to: Optional[str] = None,
                      logger: Optional[Logger] = None) -> str:
    """
    Espera un archivo descargado "estable". Retorna la ruta final (o nombre movido).
    """
    if move_to is not None and not os.path.isdir(move_to):
        os.makedirs(move_to, exist_ok=True)

    t0 = time.time()
    while time.time() - t0 <= timeout:
        files = [f for f in os.listdir(download_dir)
                 if os.path.isfile(os.path.join(download_dir, f)) and not f.endswith(".part")]

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
                        if logger: logger.info(f"Descarga movida a: {dst}")
                        return dst
                    except Exception as e:
                        if logger: logger.warning(f"No se pudo mover {fname}: {e}")
                        return src
                else:
                    if logger: logger.info(f"Descarga lista: {src}")
                    return src
        time.sleep(1)
    raise TimeoutError("Timeout esperando descarga estable.")
