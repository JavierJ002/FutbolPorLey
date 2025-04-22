import asyncio
import logging
import random
from playwright.async_api import async_playwright, Page, Browser, BrowserContext
from typing import Optional, Tuple
from config.driver_setup import _NUMERO_DE_RONDAS
# Database utilities
from database_utils.db_utils import (
    init_db_pool, close_db_pool, get_basic_match_details,
    update_team_match_aggregates
)
# Extractor functions
from extractors.id_extractor import scrape_round_match_ids, USER_AGENTS, _BASE_SOFASCORE_URL
from extractors.shots_extractor import process_incidents_and_shots_for_match
from extractors.statistics_extractor import process_team_stats_for_match
from extractors.players_statistics_extractor import process_player_stats_for_match

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')


async def setup_browser_context(p, existing_browser: Optional[Browser] = None) -> Tuple[Optional[Browser], Optional[BrowserContext], Optional[Page]]:
    """Sets up or resets the Playwright browser context."""
    browser, context, page = None, None, None
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
        await new_page.goto(_BASE_SOFASCORE_URL, wait_until="domcontentloaded", timeout=40000) # Increased timeout
        await asyncio.sleep(random.uniform(1, 3))
        logging.info("    Contexto del navegador inicializado/reiniciado.")
        browser = new_browser
        context = new_context
        page = new_page
        return browser, context, page
    except Exception as setup_err:
        logging.error(f"    Error grave durante la configuración/reinicio del navegador: {setup_err}", exc_info=True)
        if browser: await browser.close() # Attempt cleanup
        return None, None, None # Indicate failure


async def main():

    #Database
    pool = None
    try:
        pool = await init_db_pool()
        if not pool:
            logging.error("Fallo al inicializar DB pool. Saliendo.")
            return
    except Exception as db_init_error:
        logging.error(f"Excepción al inicializar DB pool: {db_init_error}", exc_info=True)
        return

    # --- Phase 1: Get Match IDs and Basic Data ---
    all_match_ids = []
    try:
        print(f"\n--- Iniciando Fase 1: Obtener IDs y Datos Básicos ({_NUMERO_DE_RONDAS} rondas) ---")
        all_match_ids = await scrape_round_match_ids(_NUMERO_DE_RONDAS)
    except Exception as id_err:
        logging.error(f"Error crítico durante la Fase 1 (Obtención de IDs): {id_err}", exc_info=True)
        # Decide whether to proceed if some IDs were potentially fetched before error

    if not all_match_ids:
        print("\nNo se obtuvieron IDs de partidos válidos. Terminando.")
        await close_db_pool()
        return

    print(f"\nTotal de IDs únicos a procesar para estadísticas detalladas e incidentes/disparos: {len(all_match_ids)}")

    # --- Phase 2, 3 & 4: Process Detailed Stats (Team & Player) and Incidents/Shots per Match ---
    print(f"\n--- Iniciando Fases 2, 3 & 4: Extracción de estadísticas detalladas e incidentes/disparos ---")

    successful_team_stats_count = 0
    successful_player_stats_count = 0
    successful_incidents_shots_count = 0
    failed_match_ids_detailed = set()

    async with async_playwright() as p:
        browser, context, page = await setup_browser_context(p)
        if not page:
            print("Error FATAL: No se pudo inicializar el navegador Playwright para estadísticas/incidentes. Terminando.")
            await close_db_pool()
            return

        for i, match_id in enumerate(all_match_ids):
            print(f"\nProcesando Partido {i+1}/{len(all_match_ids)} (ID: {match_id})")
            match_processing_failed = False
            team_aggregates = None # Reset for each match

            # Get Home/Away Team IDs for this match - needed for both stats and incidents/shots
            match_details = await get_basic_match_details(match_id)
            if not match_details or 'home_team_id' not in match_details or 'away_team_id' not in match_details:
                logging.warning(f"No se pudieron obtener detalles (IDs de equipo) para Match ID {match_id}. Saltando estadísticas detalladas e incidentes/disparos.")
                failed_match_ids_detailed.add(match_id)
                continue

            home_team_id = match_details['home_team_id']
            away_team_id = match_details['away_team_id']

            try:
                # Phase 2: Process Team Stats
                print(f"  Iniciando Fase 2: Estadísticas de equipo para Match ID {match_id}")
                team_stats_success = await process_team_stats_for_match(page, match_id, home_team_id, away_team_id)
                if team_stats_success:
                    successful_team_stats_count += 1
                else:
                    logging.warning(f"  Falló el procesamiento de estadísticas de equipo para Match ID {match_id}.")
                    match_processing_failed = True # Mark as failed, but try other phases

                # Phase 3: Process Player Stats (and get aggregates)
                print(f"  Iniciando Fase 3: Estadísticas de jugador para Match ID {match_id}")
                player_stats_success, team_aggregates = await process_player_stats_for_match(page, match_id, home_team_id, away_team_id)
                if player_stats_success:
                    successful_player_stats_count += 1
                else:
                    logging.warning(f"  Falló el procesamiento de estadísticas de jugador para Match ID {match_id}.")
                    match_processing_failed = True

                # Update Team Aggregates if player stats were processed successfully
                if player_stats_success and team_aggregates:
                    try:
                        # Update Home Team Aggregates
                        await update_team_match_aggregates(
                            match_id=match_id, team_id=home_team_id, is_home=True,
                            formation=team_aggregates['home']['formation'],
                            avg_rating=team_aggregates['home']['avg_rating'],
                            total_value=team_aggregates['home']['total_value']
                        )
                        # Update Away Team Aggregates
                        await update_team_match_aggregates(
                            match_id=match_id, team_id=away_team_id, is_home=False,
                            formation=team_aggregates['away']['formation'],
                            avg_rating=team_aggregates['away']['avg_rating'],
                            total_value=team_aggregates['away']['total_value']
                        )
                        logging.info(f"    -> Actualizados agregados (formación, rating, valor) para Match ID {match_id}.")
                    except Exception as agg_update_err:
                        logging.error(f"    -> Error actualizando agregados de equipo para Match ID {match_id}: {agg_update_err}", exc_info=True)
                        match_processing_failed = True # This is a failure for this match

                # Phase 4: Process Incidents and Shots
                print(f"  Iniciando Fase 4: Incidentes y Disparos para Match ID {match_id}")
                incidents_shots_success = await process_incidents_and_shots_for_match(page, match_id, home_team_id, away_team_id)
                if incidents_shots_success:
                    successful_incidents_shots_count += 1
                else:
                    logging.warning(f"  Falló el procesamiento de incidentes y disparos para Match ID {match_id}.")
                    match_processing_failed = True

            except Exception as processing_err:
                # Catch potential errors from Playwright (like 403 needing reset) or DB during processing
                logging.error(f"Error general procesando Match ID {match_id}: {type(processing_err).__name__} - {processing_err}", exc_info=False)
                match_processing_failed = True

                # Check if it's a potential blocking error (e.g., 403)
                # A simple way is to check if the 'page' object seems unresponsive or if specific errors occur
                # For robustness, you might add checks for common network errors or specific Playwright exceptions
                logging.warning(f"  Error encontrado para Match ID {match_id}. Intentando reiniciar contexto del navegador...")
                try:
                    browser, context, page = await setup_browser_context(p, browser)
                    if not page:
                        print("Error FATAL: No se pudo reiniciar el navegador después de un error. Terminando.")
                        break # Stop processing further matches
                except Exception as reset_err:
                    logging.error(f"Error FATAL: No se pudo reiniciar el navegador después de un error grave: {reset_err}", exc_info=True)
                    break # Stop processing further matches


            finally:
                if match_processing_failed:
                    failed_match_ids_detailed.add(match_id)
                    print(f"-> Partido {match_id} finalizado con errores en alguna fase.")
                else:
                    print(f"-> Partido {match_id} procesado exitosamente en todas las fases.")
                # Add a small delay between matches to be polite and avoid hammering the server
                await asyncio.sleep(random.uniform(5, 10)) # Increased delay between matches

        #Cleanup Playwright
        if browser:
            try:
                await browser.close()
                logging.info("Navegador Playwright final cerrado.")
            except Exception as final_close_err:
                logging.warning(f"Advertencia: Error al cerrar el navegador Playwright final: {final_close_err}")

    #Logs Summary
    print("\n--- Proceso Completo Finalizado ---")
    total_processed = len(all_match_ids)
    total_detailed_failures = len(failed_match_ids_detailed)
    total_detailed_success = total_processed - total_detailed_failures

    print(f"Resumen:")
    print(f"  - Rondas procesadas para IDs/Datos básicos: {_NUMERO_DE_RONDAS}")
    print(f"  - Total de partidos encontrados inicialmente: {total_processed}")
    print(f"  - Partidos procesados exitosamente en Fases 2/3 (Stats): {successful_team_stats_count} equipos / {successful_player_stats_count} jugadores")
    print(f"  - Partidos procesados exitosamente en Fase 4 (Incidentes/Disparos): {successful_incidents_shots_count}")
    # Note: Successful counts are for fetching/processing data, not guaranteeing every single piece of data was inserted without individual incident/shot errors logged earlier.
    print(f"  - Partidos con errores en alguna fase detallada (Stats/Incidents/Shots): {total_detailed_failures}")
    if failed_match_ids_detailed:
        logging.warning(f"IDs de partidos con errores en Fases 2/3/4: {sorted(list(failed_match_ids_detailed))}")

    #Close Database
    await close_db_pool()

if __name__ == "__main__":
    asyncio.run(main())