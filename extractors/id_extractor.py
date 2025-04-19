import asyncio
import json
import random
import logging
from playwright.async_api import async_playwright
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
from config.driver_setup import (USER_AGENTS, _BASE_SOFASCORE_URL, _DEFAULT_TOURNAMENT_ID, _DEFAULT_TOURNAMENT_NAME,
                                _DEFAULT_TOURNAMENT_COUNTRY, _DEFAULT_SEASON_ID, _DEFAULT_SEASON_NAME, _SCRAPPE_LAST_ROUND)
from database_utils.db_utils import upsert_tournament, upsert_season, upsert_team, upsert_match

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

        round_number = round_info.get("round", round_num) 
        round_name = round_info.get("name") 

        home_team_id = home_team_info["id"]
        home_team_name = home_team_info.get("name")
        home_team_country = home_team_info.get("country", {}).get("name") 

        away_team_id = away_team_info["id"]
        away_team_name = away_team_info.get("name")
        away_team_country = away_team_info.get("country", {}).get("name") 

        match_datetime_utc = datetime.fromtimestamp(timestamp_unix, tz=timezone.utc)

        
        home_score_final = home_score_info.get("current")
        away_score_final = away_score_info.get("current")
        home_score_ht = home_score_info.get("period1") 
        away_score_ht = away_score_info.get("period1") 

        
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


async def scrape_round_match_ids(num_rounds: int = 38) -> List[int]: # Valor por defecto
    """
    Scrapes match data. If _SCRAPPE_LAST_ROUND > 0 from config, the loop runs ONLY for that specific round.
    Otherwise, it scrapes rounds from 1 to num_rounds.
    Upserts tournament, season, team, and match info into the database.
    Returns a list of successfully processed match IDs.
    """
    processed_match_ids = []
    print(f"--- Iniciando scrapeo y guardado de datos básicos (Torneo, Temporada, Equipos, Partidos) ---")

    if _SCRAPPE_LAST_ROUND > 0:
        rounds_to_process = [_SCRAPPE_LAST_ROUND]
        print(f"Modo 'Sólo última jornada' activado. Procesando únicamente Ronda: {_SCRAPPE_LAST_ROUND}")

    else:
        # Si la bandera es 0, procesar todas las rondas desde 1 hasta num_rounds
        rounds_to_process = range(1, num_rounds + 1)
        print(f"Procesando Rondas: 1 a {num_rounds}")

    try:
        await upsert_tournament(_DEFAULT_TOURNAMENT_ID, _DEFAULT_TOURNAMENT_NAME, _DEFAULT_TOURNAMENT_COUNTRY)
        await upsert_season(_DEFAULT_SEASON_ID, _DEFAULT_TOURNAMENT_ID, _DEFAULT_SEASON_NAME)
    except Exception as db_init_err:
        logging.error(f"Error inicializando torneo/temporada en DB: {db_init_err}")

    async with async_playwright() as p:
        browser = None
        context = None
        page = None

        async def setup_browser_context(existing_browser=None):
            nonlocal browser, context, page
            if existing_browser:
                logging.info("      Reiniciando contexto del navegador...")
                try: await existing_browser.close()
                except Exception as close_err: logging.warning(f"Advertencia al cerrar navegador: {close_err}")

            try:
                new_browser = await p.chromium.launch(headless=True)
                new_context = await new_browser.new_context(
                    user_agent=random.choice(USER_AGENTS), viewport={"width": 1366, "height": 768}
                )
                await new_context.add_init_script("Object.defineProperty(navigator, 'webdriver', { get: () => undefined });")
                new_page = await new_context.new_page()
                logging.info(f"Visitando página principal ({_BASE_SOFASCORE_URL}) para inicializar contexto...")
                await new_page.goto(_BASE_SOFASCORE_URL, wait_until="domcontentloaded", timeout=30000)
                await asyncio.sleep(random.uniform(1, 3))
                logging.info("Contexto inicializado.")
                browser = new_browser
                context = new_context
                page = new_page
            except Exception as setup_err:
                logging.error(f"Error grave durante la configuración del navegador: {setup_err}")
                raise

        try:
            await setup_browser_context()
        except Exception as initial_setup_err:
            print(f"Error FATAL: No se pudo inicializar el navegador Playwright: {initial_setup_err}")
            return []

        for round_num in rounds_to_process:
            # El bucle solo se ejecutará para las rondas que realmente queremos procesar, basado en el if de arriba

            print(f"Procesando Ronda {round_num}...")
            api_url = f"https://www.sofascore.com/api/v1/unique-tournament/{_DEFAULT_TOURNAMENT_ID}/season/{_DEFAULT_SEASON_ID}/events/round/{round_num}"
            round_page_url = f"https://www.sofascore.com/tournament/football/{_DEFAULT_TOURNAMENT_COUNTRY}/{_DEFAULT_TOURNAMENT_NAME}/{_DEFAULT_TOURNAMENT_ID}/season/{_DEFAULT_SEASON_ID}/matches/round/{round_num}"
            logging.info(f"      API URL: {api_url}")

            match_ids_in_round = []
            try:
                await asyncio.sleep(random.uniform(3, 7)) # Delay

                try:
                    logging.info(f"      Visitando página de ronda: {round_page_url}")
                    await page.goto(round_page_url, wait_until="domcontentloaded", timeout=40000)
                    await asyncio.sleep(random.uniform(1, 3))
                except Exception as round_page_err:
                    logging.warning(f"      Advertencia: Falló visita a página de ronda {round_num}: {round_page_err}")

                logging.info(f"      Realizando fetch a API: {api_url}")
                response = await page.goto(api_url, wait_until="commit", timeout=30000)

                if response is None:
                    logging.error("      -> Error: page.goto a API devolvió None.")
                    continue # Saltar esta ronda si la respuesta es None

                if response.status == 200:
                    content = await response.text()
                    try:
                        data = json.loads(content)
                        events = data.get("events", [])
                        logging.info(f"      -> API devolvió {len(events)} eventos para Ronda {round_num}.")

                        tasks = [_process_event_data(event, round_num) for event in events]
                        results = await asyncio.gather(*tasks)

                        round_match_ids = [match_id for match_id in results if match_id is not None]
                        processed_match_ids.extend(round_match_ids)
                        print(f"      -> Ronda {round_num}: Procesados y guardados datos básicos para {len(round_match_ids)} partidos.")

                    except json.JSONDecodeError as json_err:
                        logging.error(f"      -> Error decodificando JSON para Ronda {round_num}: {json_err}. Contenido: {content[:200]}...")
                    except Exception as proc_err:
                        logging.error(f"      -> Error procesando eventos de Ronda {round_num}: {proc_err}", exc_info=True)

                else:
                    body = await response.text()
                    logging.error(f"      -> Error {response.status} obteniendo datos para Ronda {round_num}.")
                    logging.debug(f"         Respuesta: {body[:150]}...")
                    if response.status == 403:
                        logging.warning("      -> Error 403 detectado. Reiniciando contexto...")
                        await asyncio.sleep(random.uniform(10, 20))
                        try:
                            await setup_browser_context(browser)
                        except Exception as reset_err:
                            logging.error(f"Error FATAL: No se pudo reiniciar el navegador después de 403: {reset_err}")
                            break # Salir del bucle si el reinicio falla

            except Exception as e:
                logging.error(f"      -> Error general procesando Ronda {round_num}: {type(e).__name__} - {e}", exc_info=False)
                continue
        # Fin del bucle for round_num
        if browser:
            try:
                await browser.close()
                logging.info("Navegador Playwright cerrado.")
            except Exception as final_close_err:
                logging.warning(f"      Advertencia: Error al cerrar el navegador al final: {final_close_err}")

    print(f"\n--- Scrapeo y guardado de datos básicos Finalizado ---")
    unique_ids = sorted(list(set(processed_match_ids)))
    print(f"Total de IDs de partidos únicos procesados exitosamente: {len(unique_ids)}")
    return unique_ids