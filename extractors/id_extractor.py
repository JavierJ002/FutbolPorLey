import asyncio
import json
import random
import time
import logging
from playwright.async_api import async_playwright
from typing import List, Dict, Any, Optional, Union
from datetime import datetime, timezone

# Import database utility functions
from database_utils.db_utils import upsert_tournament, upsert_season, upsert_team, upsert_match

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
]
_BASE_SOFASCORE_URL = "https://www.sofascore.com/"

# --- Default values (Consider making these configurable or extracting from API if possible) ---
_DEFAULT_TOURNAMENT_ID = 8
_DEFAULT_TOURNAMENT_NAME = "LaLiga"
_DEFAULT_TOURNAMENT_COUNTRY = "Spain" # Or extract if available
_DEFAULT_SEASON_ID = 32501
_DEFAULT_SEASON_NAME = "2020/2021" # Or extract if available

async def _process_event_data(event: Dict[str, Any], round_num: int) -> Optional[int]:
    """
    Processes a single event from the API response, extracts relevant data,
    and upserts it into the database. Returns the match_id if successful.
    """
    try:
        status_info = event.get("status", {})
        if not isinstance(status_info, dict) or status_info.get("code") != 100:
            # logging.debug(f"Skipping event due to status: {status_info.get('description', 'N/A')}")
            return None # Skip if match not finished

        match_id = event.get("id")
        tournament_info = event.get("tournament", {})
        season_info = event.get("season", {})
        round_info = event.get("roundInfo", {})
        home_team_info = event.get("homeTeam", {})
        away_team_info = event.get("awayTeam", {})
        home_score_info = event.get("homeScore", {})
        away_score_info = event.get("awayScore", {})
        timestamp_unix = event.get("startTimestamp")

        if not all([match_id, home_team_info.get("id"), away_team_info.get("id"), timestamp_unix]):
            logging.warning(f"Skipping event {match_id or 'N/A'} due to missing critical IDs or timestamp.")
            return None

        # --- Extract and Prepare Data ---
        tournament_id = tournament_info.get("uniqueTournament", {}).get("id", _DEFAULT_TOURNAMENT_ID)
        tournament_name = tournament_info.get("uniqueTournament", {}).get("name", _DEFAULT_TOURNAMENT_NAME)
        tournament_country = tournament_info.get("uniqueTournament", {}).get("category", {}).get("name", _DEFAULT_TOURNAMENT_COUNTRY)

        season_id = season_info.get("id", _DEFAULT_SEASON_ID)
        season_name = season_info.get("name", _DEFAULT_SEASON_NAME)

        round_number = round_info.get("round", round_num) # Use passed round_num as fallback
        round_name = round_info.get("name") # Can be null

        home_team_id = home_team_info["id"]
        home_team_name = home_team_info.get("name")
        home_team_country = home_team_info.get("country", {}).get("name") # Can be null

        away_team_id = away_team_info["id"]
        away_team_name = away_team_info.get("name")
        away_team_country = away_team_info.get("country", {}).get("name") # Can be null

        # Convert Unix timestamp to timezone-aware datetime object (UTC)
        match_datetime_utc = datetime.fromtimestamp(timestamp_unix, tz=timezone.utc)

        # Scores (handle potential missing keys gracefully)
        home_score_final = home_score_info.get("current")
        away_score_final = away_score_info.get("current")
        home_score_ht = home_score_info.get("period1") # Halftime score
        away_score_ht = away_score_info.get("period1") # Halftime score

        # --- Upsert Data into Database (Order matters due to Foreign Keys) ---
        await upsert_tournament(tournament_id, tournament_name, tournament_country)
        await upsert_season(season_id, tournament_id, season_name)
        await upsert_team(home_team_id, home_team_name, home_team_country)
        await upsert_team(away_team_id, away_team_name, away_team_country)
        await upsert_match(
            match_id=match_id,
            season_id=season_id,
            round_num=round_number,
            round_name=round_name,
            dt_utc=match_datetime_utc,
            home_id=home_team_id,
            away_id=away_team_id,
            home_score=home_score_final,
            away_score=away_score_final,
            ht_home=home_score_ht,
            ht_away=away_score_ht
        )
        # logging.info(f"Successfully processed and upserted data for Match ID: {match_id}")
        return match_id

    except Exception as e:
        logging.error(f"Error processing event data for Match ID {event.get('id', 'N/A')}: {type(e).__name__} - {e}", exc_info=False)
        # Consider logging traceback if needed: exc_info=True
        return None


async def scrape_round_match_ids(num_rounds=1) -> List[int]:
    """
    Scrapes match data for the specified number of rounds, upserts tournament,
    season, team, and match info into the database, and returns a list of
    successfully processed match IDs.
    """
    processed_match_ids = []
    print(f"--- Iniciando scrapeo y guardado de datos básicos (Torneo, Temporada, Equipos, Partidos) ---")
    print(f"Procesando Rondas: 1 a {num_rounds}")

    # Upsert default tournament/season once (can be moved inside loop if needed)
    try:
        await upsert_tournament(_DEFAULT_TOURNAMENT_ID, _DEFAULT_TOURNAMENT_NAME, _DEFAULT_TOURNAMENT_COUNTRY)
        await upsert_season(_DEFAULT_SEASON_ID, _DEFAULT_TOURNAMENT_ID, _DEFAULT_SEASON_NAME)
    except Exception as db_init_err:
        logging.error(f"Error inicializando torneo/temporada en DB: {db_init_err}")
        # Decide if you want to continue or stop if this fails

    async with async_playwright() as p:
        browser = None
        context = None
        page = None

        async def setup_browser_context(existing_browser=None):
            nonlocal browser, context, page
            if existing_browser:
                 logging.info("    Reiniciando contexto del navegador...")
                 try:
                     await existing_browser.close()
                 except Exception as close_err:
                     logging.warning(f"    Advertencia: Error al cerrar el navegador existente: {close_err}")

            try:
                new_browser = await p.chromium.launch(headless=True)
                new_context = await new_browser.new_context(
                    user_agent=random.choice(USER_AGENTS),
                    viewport={"width": 1366, "height": 768}
                )
                await new_context.add_init_script("Object.defineProperty(navigator, 'webdriver', { get: () => undefined });")
                new_page = await new_context.new_page()
                logging.info(f"    Visitando página principal ({_BASE_SOFASCORE_URL}) para inicializar contexto...")
                await new_page.goto(_BASE_SOFASCORE_URL, wait_until="domcontentloaded", timeout=30000)
                await asyncio.sleep(random.uniform(1, 3))
                logging.info("    Contexto inicializado.")
                browser = new_browser
                context = new_context
                page = new_page
            except Exception as setup_err:
                 logging.error(f"    Error grave durante la configuración del navegador: {setup_err}")
                 raise # Re-raise to stop the process if browser setup fails

        try:
             await setup_browser_context()
        except Exception as initial_setup_err:
             print(f"Error FATAL: No se pudo inicializar el navegador Playwright: {initial_setup_err}")
             return [] # Return empty list on fatal browser error

        for round_num in range(1, num_rounds + 1):
            print(f"Procesando Ronda {round_num}...")
            api_url = f"https://www.sofascore.com/api/v1/unique-tournament/{_DEFAULT_TOURNAMENT_ID}/season/{_DEFAULT_SEASON_ID}/events/round/{round_num}"
            round_page_url = f"https://www.sofascore.com/tournament/football/spain/laliga/{_DEFAULT_TOURNAMENT_ID}/season/{_DEFAULT_SEASON_ID}/matches/round/{round_num}"
            logging.info(f"    API URL: {api_url}")

            match_ids_in_round = []
            try:
                await asyncio.sleep(random.uniform(3, 7)) # Delay before request

                # Visit round page first (optional, might help with session/cookies)
                try:
                    logging.info(f"    Visitando página de ronda: {round_page_url}")
                    await page.goto(round_page_url, wait_until="domcontentloaded", timeout=40000)
                    await asyncio.sleep(random.uniform(1, 3))
                except Exception as round_page_err:
                    logging.warning(f"    Advertencia: Falló visita a página de ronda {round_num}: {round_page_err}")

                # Fetch data from API endpoint
                logging.info(f"    Realizando fetch a API: {api_url}")
                response = await page.goto(api_url, wait_until="commit", timeout=30000)

                if response is None:
                     logging.error("   -> Error: page.goto a API devolvió None.")
                     raise ConnectionError("API request returned None")

                if response.status == 200:
                    content = await response.text()
                    try:
                        data = json.loads(content)
                        events = data.get("events", [])
                        logging.info(f"    -> API devolvió {len(events)} eventos para Ronda {round_num}.")

                        # Process each event concurrently
                        tasks = [_process_event_data(event, round_num) for event in events]
                        results = await asyncio.gather(*tasks)

                        # Collect successful match IDs
                        round_match_ids = [match_id for match_id in results if match_id is not None]
                        processed_match_ids.extend(round_match_ids)
                        print(f"    -> Ronda {round_num}: Procesados y guardados datos básicos para {len(round_match_ids)} partidos.")

                    except json.JSONDecodeError as json_err:
                        logging.error(f"    -> Error decodificando JSON para Ronda {round_num}: {json_err}. Contenido: {content[:200]}...")
                    except Exception as proc_err:
                        logging.error(f"    -> Error procesando eventos de Ronda {round_num}: {proc_err}", exc_info=True)

                else:
                    body = await response.text()
                    logging.error(f"    -> Error {response.status} obteniendo datos para Ronda {round_num}.")
                    logging.debug(f"       Respuesta: {body[:150]}...")

                    if response.status == 403:
                        logging.warning("    -> Error 403 detectado. Reiniciando contexto...")
                        await asyncio.sleep(random.uniform(10, 20))
                        try:
                            await setup_browser_context(browser) # Pass existing browser to close it first
                        except Exception as reset_err:
                            logging.error(f"Error FATAL: No se pudo reiniciar el navegador después de 403: {reset_err}")
                            break # Stop processing further rounds if reset fails

            except Exception as e:
                logging.error(f"    -> Error general procesando Ronda {round_num}: {type(e).__name__} - {e}", exc_info=False)
                # Decide if you want to continue to the next round or stop

        # --- Cleanup ---
        if browser:
            try:
                 await browser.close()
                 logging.info("Navegador Playwright cerrado.")
            except Exception as final_close_err:
                 logging.warning(f"    Advertencia: Error al cerrar el navegador al final: {final_close_err}")

    print(f"\n--- Scrapeo y guardado de datos básicos Finalizado ---")
    print(f"Total de IDs de partidos procesados exitosamente: {len(processed_match_ids)}")
    return sorted(list(set(processed_match_ids))) # Return unique, sorted list of IDs

# Example of how to call (in main.py):
# async def main():
#     pool = await init_db_pool()
#     if not pool: return
#     match_ids = await scrape_round_match_ids(num_rounds=1)
#     # ... use match_ids ...
#     await close_db_pool()
