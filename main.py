import asyncio
import logging
import random
from playwright.async_api import async_playwright, Page, Browser, BrowserContext
from typing import List, Optional, Tuple, Set
# Database utilities
from database_utils.db_utils import (
    init_db_pool, close_db_pool, get_basic_match_details,
    update_team_match_aggregates
)

# Extractor functions
from extractors.id_extractor import scrape_round_match_ids, USER_AGENTS, _BASE_SOFASCORE_URL
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
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            viewport={"width": 1366, "height": 768}
        )
        await context.add_init_script("Object.defineProperty(navigator, 'webdriver', { get: () => undefined });")
        page = await context.new_page()
        logging.info(f"    Visitando página principal ({_BASE_SOFASCORE_URL}) para inicializar contexto...")
        await page.goto(_BASE_SOFASCORE_URL, wait_until="domcontentloaded", timeout=40000) # Increased timeout
        await asyncio.sleep(random.uniform(1, 3))
        logging.info("    Contexto del navegador inicializado/reiniciado.")
        return browser, context, page
    except Exception as setup_err:
        logging.error(f"    Error grave durante la configuración/reinicio del navegador: {setup_err}", exc_info=True)
        if browser: await browser.close() # Attempt cleanup
        return None, None, None # Indicate failure


async def main():
    NUMERO_DE_RONDAS = 1 # Define how many rounds to process

    # --- Initialize Database Pool ---
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
    # This function now also handles inserting tournament, season, team, and match data
    all_match_ids = []
    try:
        print(f"\n--- Iniciando Fase 1: Obtener IDs y Datos Básicos ({NUMERO_DE_RONDAS} rondas) ---")
        all_match_ids = await scrape_round_match_ids(NUMERO_DE_RONDAS)
    except Exception as id_err:
        logging.error(f"Error crítico durante la Fase 1 (Obtención de IDs): {id_err}", exc_info=True)
        # Decide whether to proceed if some IDs were potentially fetched before error

    if not all_match_ids:
        print("\nNo se obtuvieron IDs de partidos válidos. Terminando.")
        await close_db_pool()
        return

    print(f"\nTotal de IDs únicos a procesar para estadísticas detalladas: {len(all_match_ids)}")

    # --- Phase 2 & 3: Process Detailed Stats (Team & Player) per Match ---
    print(f"\n--- Iniciando Fases 2 & 3: Extracción de estadísticas detalladas (Equipo y Jugador) ---")

    successful_team_stats_count = 0
    successful_player_stats_count = 0
    failed_match_ids_detailed = set()

    async with async_playwright() as p:
        browser, context, page = await setup_browser_context(p)
        if not page:
            print("Error FATAL: No se pudo inicializar el navegador Playwright para estadísticas. Terminando.")
            await close_db_pool()
            return

        for i, match_id in enumerate(all_match_ids):
            print(f"\nProcesando Partido {i+1}/{len(all_match_ids)} (ID: {match_id})")
            match_failed = False
            team_aggregates = None # To store formation, avg_rating, total_value

            try:
                # Get Home/Away Team IDs for this match
                match_details = await get_basic_match_details(match_id)
                if not match_details or 'home_team_id' not in match_details or 'away_team_id' not in match_details:
                    logging.warning(f"No se pudieron obtener detalles (IDs de equipo) para Match ID {match_id}. Saltando estadísticas detalladas.")
                    failed_match_ids_detailed.add(match_id)
                    continue

                home_team_id = match_details['home_team_id']
                away_team_id = match_details['away_team_id']

                # Process Team Stats
                team_stats_success = await process_team_stats_for_match(page, match_id, home_team_id, away_team_id)
                if team_stats_success:
                    successful_team_stats_count += 1
                else:
                    logging.warning(f"Falló el procesamiento de estadísticas de equipo para Match ID {match_id}.")
                    match_failed = True # Mark as potentially failed, but try player stats

                # Process Player Stats (and get aggregates)
                player_stats_success, team_aggregates = await process_player_stats_for_match(page, match_id, home_team_id, away_team_id)
                if player_stats_success:
                    successful_player_stats_count += 1
                else:
                    logging.warning(f"Falló el procesamiento de estadísticas de jugador para Match ID {match_id}.")
                    match_failed = True # Mark as failed if player stats fail

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
                        # Decide if this error constitutes a full match failure
                        match_failed = True


            except Exception as processing_err:
                 # Catch potential errors from Playwright (like 403 needing reset) or DB
                 logging.error(f"Error procesando Match ID {match_id}: {type(processing_err).__name__} - {processing_err}", exc_info=False)
                 match_failed = True

                 # Check if it's a potential blocking error (e.g., 403)
                 # This requires inspecting the error or having the functions raise specific exceptions
                 # For simplicity, we'll try resetting the browser on any exception during processing
                 logging.warning(f"Error encontrado para Match ID {match_id}. Intentando reiniciar contexto del navegador...")
                 browser, context, page = await setup_browser_context(p, browser)
                 if not page:
                     print("Error FATAL: No se pudo reiniciar el navegador después de un error. Terminando.")
                     break # Stop processing further matches

            finally:
                if match_failed:
                    failed_match_ids_detailed.add(match_id)
                    print(f"-> Partido {match_id} finalizado con errores.")
                else:
                    print(f"-> Partido {match_id} procesado exitosamente.")

        # --- Cleanup Playwright ---
        if browser:
            try:
                await browser.close()
                logging.info("Navegador Playwright final cerrado.")
            except Exception as final_close_err:
                logging.warning(f"Advertencia: Error al cerrar el navegador Playwright final: {final_close_err}")

    # --- Final Summary ---
    print("\n--- Proceso Completo Finalizado ---")
    total_processed = len(all_match_ids)
    total_detailed_failures = len(failed_match_ids_detailed)
    total_detailed_success = total_processed - total_detailed_failures

    print(f"Resumen:")
    print(f"  - Rondas procesadas para IDs/Datos básicos: {NUMERO_DE_RONDAS}")
    print(f"  - Total de partidos encontrados inicialmente: {total_processed}")
    print(f"  - Partidos procesados exitosamente para Stats Detalladas (Equipo y Jugador): {total_detailed_success}")
    # Note: Counts below might be slightly off if one stat type succeeded but the other failed for a match
    # print(f"  - Éxito en Stats de Equipo (aprox): {successful_team_stats_count}")
    # print(f"  - Éxito en Stats de Jugador (aprox): {successful_player_stats_count}")
    print(f"  - Partidos con errores durante procesamiento detallado: {total_detailed_failures}")
    if failed_match_ids_detailed:
        logging.warning(f"IDs de partidos con errores en Fases 2/3: {sorted(list(failed_match_ids_detailed))}")

    # --- Close Database Pool ---
    await close_db_pool()

if __name__ == "__main__":
    # Ensure the event loop runs the main async function
    asyncio.run(main())
