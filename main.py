from functions import firefox_driver, DriverState, ScrapingAction, By, pipe, navigate, url_to_be, step_from_action, run_block
import logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
logger = logging.getLogger("functional-scraper")

if __name__ == "__main__":
    with firefox_driver(logger, driver_path="/Users/teoechavarria/Downloads/geckodriver", download_folder="./downloads") as drv:
        # Estado inicial explícito
        state = DriverState(driver=drv)

        state.driver.get("https://login.sura.com/sso/servicelogin.aspx?continueTo=https%3A%2F%2Fportaleps.epssura.com%2FServiciosUnClick%2F&service=epssura")

