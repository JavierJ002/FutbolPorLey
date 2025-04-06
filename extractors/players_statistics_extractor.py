# extractors/players_statistics_extractor.py
import asyncio
import json
import random
from playwright.async_api import async_playwright
import traceback
from typing import Any, Dict, List, Optional, Union # Added typing

# USER_AGENTS debe estar definido aquí o importado
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
]

_BASE_SOFASCORE_URL = "https://www.sofascore.com/"

# --- REVISED & COMPLETED MAPPING: Map SofaScore API keys to desired NUMERIC output keys ---
SOFASCORE_API_TO_NUMERIC_STATS_MAP = {
    # Core Info
    'rating': 'rating', # Keep as float/None
    'minutesPlayed': 'minutes_played',
    'touches': 'touches',

    # Passing
    'accuratePass': 'passes_accurate',
    'totalPass': 'passes_total',
    'keyPass': 'passes_key',
    'accurateLongBalls': 'long_balls_accurate',
    'totalLongBalls': 'long_balls_total',
    'accurateCross': 'crosses_accurate',
    'totalCross': 'crosses_total',

    # Attacking / Goal Contribution
    'goals': 'goals',
    'ownGoals': 'own_goals', # Added based on first example
    'goalAssist': 'assists',
    'totalShoot': 'shots_total',
    'onTargetScoringAttempt': 'shots_on_target',
    'shotOffTarget': 'shots_off_target', # Added from second example
    'blockedScoringAttempt': 'shots_blocked_by_opponent', # Player's own shot blocked by def

    # Dribbling & Possession
    'dribbleWon': 'dribbles_successful',
    'dribbleAttempt': 'dribbles_attempts',
    'possessionLostCtrl': 'possession_lost',
    'dispossessed': 'dispossessed', # Times player was tackled and lost ball

    # Duels
    'duelWon': 'duels_won',
    'duelLost': 'duels_lost',
    'aerialWon': 'aerials_won',
    'aerialLost': 'aerials_lost',
    # Estos pueden no estar directamente, calcular si es necesario
    'groundDuelWon': 'ground_duels_won', # Si API lo da
    # 'groundDuelTotal': 'ground_duels_total', # API might provide this directly
    # 'aerialDuelTotal': 'aerials_total', # API might provide this directly
    'totalContest': 'contests_total', # Added from second example (less common?)
    'wonContest': 'contests_won', # Corresponding won contests

    # Defending
    'totalTackle': 'tackles', # A veces solo viene este total
    'interceptionWon': 'interceptions',
    'totalClearance': 'clearances',
    'outfielderBlock': 'shots_blocked_by_player', # Player blocks an opponent's shot
    'challengeLost': 'dribbled_past', # Times the player was dribbled past

    # Fouls
    'fouls': 'fouls_committed',
    'wasFouled': 'fouls_suffered',

    # Goalkeeping
    'saves': 'saves',
    'punches': 'punches_made',
    'goodHighClaim': 'high_claims',
    'savedShotsFromInsideTheBox': 'saves_inside_box',
    'keeperSweeperWon': 'sweeper_keeper_successful',
    'totalKeeperSweeper': 'sweeper_keeper_total',

}


# --- Helper Function for Safe Numeric Conversion ---
def _safe_to_float(value: Any, default: float = 0.0) -> Optional[float]:
    """Safely converts a value to float, returning None for errors or None input."""
    if value is None:
        return None
    try:
        # Handle cases like "7.5" or 7.5
        return float(str(value).replace(',', '.')) # Ensure decimal point is dot
    except (ValueError, TypeError):
        return None # Return None for invalid conversions

def _safe_to_int(value: Any, default: int = 0) -> Optional[int]:
    """Safely converts a value to int, returning None for errors or None input."""
    if value is None:
        return None
    try:
        # Handle potential floats like 7.0 or "7"
        return int(float(str(value).replace(',', '.')))
    except (ValueError, TypeError):
        return None # Return None for invalid conversions

def _process_player_entry(player_entry: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Extracts and processes data for a single player from the lineup entry.
    Returns a dictionary with player info and NUMERIC stats, or None if basic data is missing.
    """
    player_info = player_entry.get("player")
    stats_raw = player_entry.get("statistics")

    if not player_info or not player_info.get("id") or stats_raw is None:
        print(f"Advertencia: Datos básicos faltantes para entrada de jugador: {player_entry.get('player', {}).get('id')}")
        return None

    # Basic Player Info
    player_data = {
        'player_id': player_info.get("id"),
        'name': player_info.get("name"),
        'short_name': player_info.get("shortName"),
        # Usar posición del lineup primero, luego del perfil si falta
        'position': player_entry.get("position", player_info.get("position")),
        'jersey_number': _safe_to_int(player_entry.get("jerseyNumber")),
        'is_substitute': player_entry.get("substitute", False),
        'height': _safe_to_int(player_info.get("height")), # Renombrado en BD, pero aquí mantenemos original
        'country_code': player_info.get("country", {}).get("alpha2"), # Original
        'country_name': player_info.get("country", {}).get("name"),
        'market_value_eur': _safe_to_int(player_info.get("proposedMarketValueRaw", {}).get("value")),
        'stats': {} # Sub-dictionary for statistics
    }

    # Initialize all potential stats with None for consistency
    for target_key in SOFASCORE_API_TO_NUMERIC_STATS_MAP.values():
        player_data['stats'][target_key] = None

    # Populate stats from the API data
    for api_key, target_key in SOFASCORE_API_TO_NUMERIC_STATS_MAP.items():
        if api_key in stats_raw:
            raw_value = stats_raw[api_key]
            # Handle specific types (float for rating, int for others)
            if target_key in ['rating']:
                player_data['stats'][target_key] = _safe_to_float(raw_value)
            else:
                # All other stats are treated as integers
                player_data['stats'][target_key] = _safe_to_int(raw_value)

    # Calcular ground duels won si no viene directamente
    if 'duels_won' in player_data['stats'] and 'aerials_won' in player_data['stats'] and \
       player_data['stats']['duels_won'] is not None and player_data['stats']['aerials_won'] is not None and \
       'ground_duels_won' not in player_data['stats']: # Solo si no existe ya
           player_data['stats']['ground_duels_won'] = player_data['stats']['duels_won'] - player_data['stats']['aerials_won']


    return player_data


# --- REVISED: Main Parsing Function ---
def _parse_player_lineup_data(lineup_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parses the JSON response from the /lineups API using the helper function.
    Returns a structured dictionary with team info and lists of processed player data.
    """
    parsed_data = {
        "home_team_info": {
            "formation": None,
            "sofascore_rating_avg": None, # Clave original
            "total_market_value_eur": 0, # Clave original
            "players": []
        },
        "away_team_info": {
            "formation": None,
            "sofascore_rating_avg": None, # Clave original
            "total_market_value_eur": 0, # Clave original
            "players": []
        }
    }

    if not lineup_data or not isinstance(lineup_data, dict):
        print("    Advertencia: Datos de alineación inválidos o vacíos.")
        return parsed_data

    # Process Home Team
    home_data = lineup_data.get("home", {})
    parsed_data["home_team_info"]["formation"] = home_data.get("formation")
    home_player_ratings = []
    home_total_market_value = 0

    for player_entry in home_data.get("players", []):
        processed_player = _process_player_entry(player_entry)
        if processed_player:
            parsed_data["home_team_info"]["players"].append(processed_player)
            # Usar el valor de mercado del jugador procesado
            market_value = processed_player.get('market_value_eur')
            if market_value is not None:
                 home_total_market_value += market_value
            # Usar el rating del jugador procesado
            player_rating = processed_player.get('stats', {}).get('rating')
            if isinstance(player_rating, (int, float)) and player_rating > 0: # Check type and > 0
                home_player_ratings.append(player_rating)
        # else:
            # print(f"Advertencia: Entrada de jugador HOME ignorada: {player_entry.get('player',{}).get('id')}")


    # Process Away Team
    away_data = lineup_data.get("away", {})
    parsed_data["away_team_info"]["formation"] = away_data.get("formation")
    away_player_ratings = []
    away_total_market_value = 0

    for player_entry in away_data.get("players", []):
        processed_player = _process_player_entry(player_entry)
        if processed_player:
            parsed_data["away_team_info"]["players"].append(processed_player)
            market_value = processed_player.get('market_value_eur')
            if market_value is not None:
                 away_total_market_value += market_value
            player_rating = processed_player.get('stats', {}).get('rating')
            if isinstance(player_rating, (int, float)) and player_rating > 0:
                away_player_ratings.append(player_rating)
        # else:
            # print(f"Advertencia: Entrada de jugador AWAY ignorada: {player_entry.get('player',{}).get('id')}")


    # Calculate final team stats
    parsed_data["home_team_info"]["total_market_value_eur"] = home_total_market_value
    if home_player_ratings:
        parsed_data["home_team_info"]["sofascore_rating_avg"] = round(sum(home_player_ratings) / len(home_player_ratings), 2)

    parsed_data["away_team_info"]["total_market_value_eur"] = away_total_market_value
    if away_player_ratings:
        parsed_data["away_team_info"]["sofascore_rating_avg"] = round(sum(away_player_ratings) / len(away_player_ratings), 2)

    return parsed_data

# --- Fetching logic ---
async def _fetch_lineup_data_pw(page, match_id) -> Optional[Union[Dict, List]]:
    """Obtiene datos de la API /lineups usando Playwright."""
    lineup_api_url = f"https://www.sofascore.com/api/v1/event/{match_id}/lineups"

    print(f"    Intentando fetch de alineaciones/jugadores para Match ID: {match_id} (API: {lineup_api_url})")
    response = None
    try:
        # No es necesario visitar la página del evento aquí, fetch directo es suficiente
        response = await page.goto(lineup_api_url, wait_until="commit", timeout=30000)

        if response is None:
            print("    -> Error: page.goto a API /lineups devolvió None.")
            return {"error": 500, "message": "Playwright goto returned None"} # Devolver error

        status = response.status
        print(f"    Respuesta API /lineups para {match_id}: Status {status}")

        if status == 200:
            content = await response.text()
            # Validar si es un objeto JSON
            if content.strip().startswith("{") and content.strip().endswith("}"):
                 try:
                     lineup_object = json.loads(content)
                     return lineup_object # Devolver objeto JSON
                 except json.JSONDecodeError as json_err:
                     print(f"    -> Error: No se pudo decodificar el JSON de /lineups. Error: {json_err}. Contenido: {content[:300]}...")
                     return {"error": 500, "message": f"JSON Decode Error: {json_err}"}
            else:
                print(f"    -> Error: La respuesta 200 de /lineups no parece ser un objeto JSON válido. Contenido: {content[:200]}...")
                return {"error": 500, "message": "Invalid JSON format in 200 response"}
        # --- Manejo de errores HTTP ---
        else:
            print(f"    -> Error en fetch de API /lineups: {status}")
            if status == 403:
                 return {"error": 403, "message": "Forbidden"}
            elif status == 404:
                 # Lineups pueden no existir para todos los partidos
                 print(f"    -> Alineaciones no encontradas (404) para match {match_id}. Esto puede ser normal.")
                 return {"error": 404, "message": "Lineups not found"}
            else:
                 # Otros errores HTTP
                 return {"error": status, "message": f"HTTP Error {status}"}

    except asyncio.TimeoutError:
        print(f"    -> Error: Timeout durante fetch de /lineups para Match ID {match_id}")
        return {"error": 408, "message": "Request Timeout"}
    except Exception as e:
        # Captura otros errores de Playwright o red
        print(f"    -> Error inesperado durante fetch de /lineups para Match ID {match_id}: {type(e).__name__}")
        # traceback.print_exc() # Descomentar para depuración detallada si es necesario
        return {"error": 500, "message": f"Unexpected error: {type(e).__name__}"}

# --- Main extraction function ---
async def extract_player_stats_for_match_ids(match_ids_list: List[Union[str, int]]) -> List[Dict[str, Any]]:
    """
    Recibe una LISTA de IDs de partidos y extrae formación, rating de equipo
    y estadísticas detalladas de jugadores usando la API /lineups.

    Returns:
        List[Dict[str, Any]]: Lista de diccionarios, cada uno con "match_id" y "lineup_data".
                               Incluye entradas vacías o con errores si el fetch falla.
    """
    all_matches_lineups = []
    failed_match_ids = []
    total_ids_to_process = len(match_ids_list)

    print(f"--- Iniciando extracción de Alineaciones/Jugadores para {total_ids_to_process} partidos ---")

    if not total_ids_to_process: return []

    async with async_playwright() as p:
        browser = None
        context = None
        page = None

        # --- Browser/Context Setup Function (similar a id_extractor) ---
        async def setup_browser_context(existing_browser=None):
             # Usa nonlocal para modificar o devuelve las nuevas instancias
             nonlocal browser, context, page
             if existing_browser:
                 print("    Reiniciando contexto del navegador (jugadores)...")
                 try:
                     await existing_browser.close()
                 except Exception as close_err:
                     print(f"    Advertencia: Error al cerrar el navegador existente: {close_err}")

             new_browser = await p.chromium.launch(headless=True)
             new_context = await new_browser.new_context(
                 user_agent=random.choice(USER_AGENTS),
                 viewport={"width": 1366, "height": 768}
             )
             await new_context.add_init_script("Object.defineProperty(navigator, 'webdriver', { get: () => undefined });")
             new_page = await new_context.new_page()
             try:
                 # Warm-up visit
                 await new_page.goto(_BASE_SOFASCORE_URL, wait_until="domcontentloaded", timeout=30000)
                 await asyncio.sleep(random.uniform(0.5, 1.5))
                 print("    Contexto del navegador (jugadores) inicializado.")
             except Exception as init_err:
                 print(f"    Advertencia: Falló visita inicial a {_BASE_SOFASCORE_URL} (jugadores): {init_err}")

             # Actualiza variables o devuelve
             browser = new_browser
             context = new_context
             page = new_page
             # O: return new_browser, new_context, new_page

        # Initial browser setup
        try:
            await setup_browser_context()
            # O: browser, context, page = await setup_browser_context()
        except Exception as initial_setup_err:
            print(f"Error FATAL: No se pudo inicializar el navegador Playwright (jugadores): {initial_setup_err}")
            traceback.print_exc()
            return [] # Salir si falla la inicialización

        # --- Main Processing Loop ---
        for i, match_id_any in enumerate(match_ids_list):
            match_id_str = str(match_id_any) # Usar string para consistencia
            print(f"  Procesando Alineaciones Partido {i+1}/{total_ids_to_process} (ID: {match_id_str})")
            await asyncio.sleep(random.uniform(2.5, 5.5)) # Pausa respetuosa

            lineup_raw_data = await _fetch_lineup_data_pw(page, match_id_str)

            # --- Error Handling post-fetch ---
            fetch_error = None
            if isinstance(lineup_raw_data, dict) and "error" in lineup_raw_data:
                fetch_error = lineup_raw_data

            if fetch_error:
                error_code = fetch_error.get("error")
                error_msg = fetch_error.get("message", "Unknown fetch error")
                print(f"    -> Falló la obtención de alineaciones para {match_id_str} (Error {error_code}): {error_msg}")
                failed_match_ids.append(match_id_str)
                # Añadir entrada de error a la lista de resultados
                all_matches_lineups.append({
                    "match_id": match_id_str,
                    "lineup_data": {"error": error_code, "message": error_msg} # Indicar error
                })


                if error_code == 403:
                    print(f"    -> Error 403 detectado. Reiniciando contexto...")
                    await asyncio.sleep(random.uniform(15, 25))
                    try:
                         await setup_browser_context(browser) # Reiniciar
                         # O: browser, context, page = await setup_browser_context(browser)
                         print("    -> Contexto del navegador reiniciado.")
                    except Exception as reset_err:
                         print(f"Error FATAL: No se pudo reiniciar el navegador después de 403: {reset_err}")
                         break # Salir del bucle si el reinicio falla críticamente
                else:
                    # Pausa corta para otros errores
                    await asyncio.sleep(random.uniform(2, 4))

                continue # Al siguiente ID

            # --- Proceed with Parsing if fetch was successful ---
            # Asegurar que no es un diccionario de error y que es un diccionario
            if lineup_raw_data and isinstance(lineup_raw_data, dict) and "error" not in lineup_raw_data:
                try:
                    parsed_lineup_info = _parse_player_lineup_data(lineup_raw_data)
                    # Añadir resultado exitoso
                    all_matches_lineups.append({
                        "match_id": match_id_str,
                        "lineup_data": parsed_lineup_info # Los datos parseados
                    })
                except Exception as parse_err:
                    print(f"    -> Error FATAL parseando datos de alineación para Match ID {match_id_str}: {parse_err}")
                    traceback.print_exc()
                    failed_match_ids.append(match_id_str)
                    # Añadir entrada de error de parseo
                    all_matches_lineups.append({
                         "match_id": match_id_str,
                         "lineup_data": {"error": 500, "message": f"Parsing Error: {parse_err}"}
                    })
            else:
                 # Caso donde lineup_raw_data es None o un tipo inesperado (no debería pasar con el fetch revisado)
                 print(f"    -> Advertencia: Fetch para {match_id_str} no devolvió error pero los datos son inválidos/nulos. Saltando.")
                 failed_match_ids.append(match_id_str)
                 all_matches_lineups.append({
                      "match_id": match_id_str,
                      "lineup_data": {"error": 500, "message": "Invalid or null data received after fetch"}
                 })


        # --- End of Loop ---
        if browser:
            try:
                await browser.close()
            except Exception as final_close_err:
                 print(f"    Advertencia: Error al cerrar el navegador al final (jugadores): {final_close_err}")

    print("\n--- Extracción de Alineaciones/Jugadores Finalizada ---")
    successful_count = sum(1 for item in all_matches_lineups if isinstance(item.get("lineup_data"), dict) and "error" not in item.get("lineup_data", {}))
    print(f"Datos de alineación/jugador obtenidos para {successful_count} partidos.")
    unique_failed_ids = sorted(list(set(failed_match_ids)))
    if unique_failed_ids:
        print(f"Partidos con errores durante fetch/parseo de alineaciones: {len(unique_failed_ids)}")
        # Podrías imprimir los IDs si son pocos: print(f"IDs fallidos: {unique_failed_ids}")

    return all_matches_lineups