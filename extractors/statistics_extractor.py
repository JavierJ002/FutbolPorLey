import asyncio
import json
import random
import time
import logging
from playwright.async_api import Page
import traceback
from typing import List, Dict, Any, Optional, Union, Tuple
from database_utils.db_utils import insert_team_stats_batch
from helpers.convert_stats import _safe_to_float, _safe_to_int, _convert_to_numeric

# Mapping from SofaScore API stat names to temporary processing keys
# We'll map these temporary keys to the final DB columns later
STATS_NAME_MAP_API_TO_TEMP = {
    "Ball possession": "possession_percentage",
    "Big chances": "big_chances",
    "Total shots": "total_shots",
    "Goalkeeper saves": "saves", # Note: Might be duplicated by "Total saves"
    "Total saves": "saves",
    "Corner kicks": "corners",
    "Fouls": "fouls",
    "Passes": "passes_complex", # e.g., "455/524 (87%)"
    "Total tackles": "tackles_total_simple",
    "Free kicks": "free_kicks",
    "Yellow cards": "yellow_cards",
    "Red cards": "red_cards",
    "Shots on target": "shots_on_target",
    "Hit woodwork": "hit_woodwork",
    "Shots off target": "shots_off_target",
    "Blocked shots": "blocked_shots",
    "Shots inside box": "shots_inside_box",
    "Shots outside box": "shots_outside_box",
    "Big chances missed": "big_chances_missed",
    "Fouled in final third": "fouled_final_third",
    "Offsides": "offsides",
    "Accurate passes": "accurate_passes_percentage_complex", # e.g., "87%" or "455/524 (87%)"
    "Throw-ins": "throw_ins",
    "Final third entries": "final_third_entries",
    "Long balls": "long_balls_complex", # e.g., "30/56 (54%)"
    "Crosses": "crosses_complex", # e.g., "5/18 (28%)"
    "Duels": "duels_won_complex", # e.g., "58/116 (50%)" - Assuming 'Duels' means 'Duels Won'
    "Dispossessed": "dispossessed",
    "Ground duels": "ground_duels_complex", # e.g., "46/89 (52%)"
    "Aerial duels": "aerial_duels_complex", # e.g., "12/27 (44%)"
    "Dribbles": "dribbles_complex", # e.g., "7/11 (64%)"
    "Tackles won": "tackles_won_details", # Contains successful/total/percentage despite API name "wonTacklePercent"
    "Interceptions": "interceptions",
    "Clearances": "clearances",
    "Goal kicks": "goal_kicks"
}

# Order of columns for the team_match_stats table INSERT statement
# Must match the VALUES clause in insert_team_stats_batch
# Excludes team_match_stat_id (auto-generated)
TEAM_STATS_DB_ORDER = [
    'match_id', 'team_id', 'is_home_team', 'period',
    'formation', 'average_team_rating', 'total_team_market_value_eur', # These will be NULL initially
    'possession_percentage', 'big_chances', 'total_shots', 'saves', 'corners', 'fouls',
    'passes_successful', 'passes_total', 'passes_percentage',
    'tackles_successful', 'tackles_total', 'tackles_won_percentage',
    'free_kicks', 'yellow_cards', 'red_cards', 'shots_on_target', 'hit_woodwork',
    'shots_off_target', 'blocked_shots', 'shots_inside_box', 'shots_outside_box',
    'big_chances_missed', 'fouled_final_third', 'offsides',
    'accurate_passes_percentage', # Direct percentage if available
    'throw_ins', 'final_third_entries',
    'long_balls_successful', 'long_balls_total', 'long_balls_percentage',
    'crosses_successful', 'crosses_total', 'crosses_percentage',
    'duels_won_successful', 'duels_won_total', 'duels_won_percentage',
    'dispossessed',
    'ground_duels_successful', 'ground_duels_total', 'ground_duels_percentage',
    'aerial_duels_successful', 'aerial_duels_total', 'aerial_duels_percentage',
    'dribbles_successful', 'dribbles_total', 'dribbles_percentage',
    'interceptions', 'clearances', 'goal_kicks'
] # 56 columns total


def _parse_statistics_data(statistics_json_list: List[Dict], match_id: int, home_team_id: int, away_team_id: int) -> List[Tuple]:
    """
    Parses the raw statistics list from the API and transforms it into a list of tuples,
    one tuple per team per period, ready for batch insertion.
    """
    parsed_stats_batch = []
    temp_stats_data = {} # Store intermediate parsed data: {period: {team_loc: {temp_key: value}}}

    # Initialize structure
    for period_code in ["ALL", "1ST", "2ND"]:
        temp_stats_data[period_code] = {"home": {}, "away": {}}

    # First pass: Extract raw values using API names and convert types
    for period_stats_obj in statistics_json_list:
        period_code = period_stats_obj.get("period")
        if period_code not in temp_stats_data:
            logging.warning(f"Match {match_id}: Unknown period code '{period_code}' found in stats.")
            continue

        for group in period_stats_obj.get("groups", []):
            for item in group.get("statisticsItems", []):
                stat_name_api = item.get("name")
                temp_key = STATS_NAME_MAP_API_TO_TEMP.get(stat_name_api)

                if temp_key:
                    for team_loc in ["home", "away"]:
                        # Special handling for "Tackles won" which has value/total in different fields
                        if temp_key == 'tackles_won_details':
                            successful = item.get(f"{team_loc}Value")
                            total = item.get(f"{team_loc}Total")
                            percentage = None
                            if total is not None and successful is not None:
                                try:
                                    total_int = int(total)
                                    successful_int = int(successful)
                                    if total_int > 0:
                                        percentage = round(successful_int / total_int, 4)
                                except (ValueError, TypeError, ZeroDivisionError):
                                    pass
                            converted_value = {
                                "successful": _safe_to_int(successful),
                                "total": _safe_to_int(total),
                                "percentage": percentage
                            }
                        else:
                            raw_value = item.get(team_loc)
                            converted_value = _convert_to_numeric(raw_value)

                        # Store the converted value (can be int, float, dict, or None)
                        temp_stats_data[period_code][team_loc][temp_key] = converted_value

    # Second pass: Map temporary keys to final DB columns and create tuples
    for period_code, teams_data in temp_stats_data.items():
        for team_loc, stats in teams_data.items():
            is_home = team_loc == "home"
            team_id = home_team_id if is_home else away_team_id
            final_stats_map = {key: None for key in TEAM_STATS_DB_ORDER} # Initialize with None

            # --- Populate final_stats_map from parsed stats ---
            final_stats_map['match_id'] = match_id
            final_stats_map['team_id'] = team_id
            final_stats_map['is_home_team'] = is_home
            final_stats_map['period'] = period_code

            for temp_key, db_key in [
                ('possession_percentage', 'possession_percentage'), ('big_chances', 'big_chances'),
                ('total_shots', 'total_shots'), ('saves', 'saves'), ('corners', 'corners'),
                ('fouls', 'fouls'), ('free_kicks', 'free_kicks'), ('yellow_cards', 'yellow_cards'),
                ('red_cards', 'red_cards'), ('shots_on_target', 'shots_on_target'),
                ('hit_woodwork', 'hit_woodwork'), ('shots_off_target', 'shots_off_target'),
                ('blocked_shots', 'blocked_shots'), ('shots_inside_box', 'shots_inside_box'),
                ('shots_outside_box', 'shots_outside_box'), ('big_chances_missed', 'big_chances_missed'),
                ('fouled_final_third', 'fouled_final_third'), ('offsides', 'offsides'),
                ('throw_ins', 'throw_ins'), ('final_third_entries', 'final_third_entries'),
                ('dispossessed', 'dispossessed'), ('interceptions', 'interceptions'),
                ('clearances', 'clearances'), ('goal_kicks', 'goal_kicks'),
                ('tackles_total_simple', 'tackles_total') 
            ]:
                if temp_key in stats:
                    value = stats[temp_key]
                    if isinstance(value, dict):
                         logging.warning(f"Match {match_id}, Period {period_code}, Team {team_id}: Expected simple numeric for {temp_key}, got dict. Setting to None.")
                         final_stats_map[db_key] = None
                    else:
                         final_stats_map[db_key] = value

            for complex_key, s_key, t_key, p_key in [
                ('passes_complex', 'passes_successful', 'passes_total', 'passes_percentage'),
                ('long_balls_complex', 'long_balls_successful', 'long_balls_total', 'long_balls_percentage'),
                ('crosses_complex', 'crosses_successful', 'crosses_total', 'crosses_percentage'),
                ('duels_won_complex', 'duels_won_successful', 'duels_won_total', 'duels_won_percentage'),
                ('ground_duels_complex', 'ground_duels_successful', 'ground_duels_total', 'ground_duels_percentage'),
                ('aerial_duels_complex', 'aerial_duels_successful', 'aerial_duels_total', 'aerial_duels_percentage'),
                ('dribbles_complex', 'dribbles_successful', 'dribbles_total', 'dribbles_percentage'),
                ('tackles_complex', 'tackles_successful', 'tackles_total', 'tackles_won_percentage')
            ]:
                complex_value = stats.get(complex_key)
                if isinstance(complex_value, dict):
                    final_stats_map[s_key] = complex_value.get('successful')
                    final_stats_map[t_key] = complex_value.get('total')
                    final_stats_map[p_key] = complex_value.get('percentage')
                elif isinstance(complex_value, (int, float)):
                    
                    if complex_key == 'duels_won_complex' and isinstance(complex_value, float) and complex_value <= 1.0:
                        # If it's 'Duels' and a float <= 1, assume it's the percentage
                        if p_key:
                            final_stats_map[p_key] = complex_value
                    elif t_key: # Ensure a total key exists (e.g., passes_total)
                        final_stats_map[t_key] = complex_value
                     
                elif complex_value is not None:
                    #Log warning only if it's neither dict nor number (truly unexpected format)
                    logging.warning(f"Match {match_id}, Period {period_code}, Team {team_id}: Unexpected type or format for {complex_key}: {type(complex_value)}. Value: {complex_value}")

            # Handle accurate_passes_percentage (can be simple float or complex dict)
            acc_pass_val = stats.get('accurate_passes_percentage_complex')
            if isinstance(acc_pass_val, dict):
                # If complex, prefer percentage from dict, ensure passes_successful/total are also set
                final_stats_map['accurate_passes_percentage'] = acc_pass_val.get('percentage')
                if 'passes_successful' not in final_stats_map or final_stats_map['passes_successful'] is None:
                     final_stats_map['passes_successful'] = acc_pass_val.get('successful')
                if 'passes_total' not in final_stats_map or final_stats_map['passes_total'] is None:
                     final_stats_map['passes_total'] = acc_pass_val.get('total')
            elif isinstance(acc_pass_val, (float, int)):
                 # If simple percentage, store it directly
                 final_stats_map['accurate_passes_percentage'] = acc_pass_val
            elif 'passes_complex' in stats and isinstance(stats['passes_complex'], dict):
                 # Fallback: if complex passes available, use its percentage
                 final_stats_map['accurate_passes_percentage'] = stats['passes_complex'].get('percentage')

            # Handle Tackles using the dedicated fields
            tackles_won_data = stats.get('tackles_won_details')
            if isinstance(tackles_won_data, dict):
                final_stats_map['tackles_successful'] = tackles_won_data.get('successful')
                # Use total from 'tackles_won_details' if available, otherwise fallback to 'tackles_total_simple'
                final_stats_map['tackles_total'] = tackles_won_data.get('total') if tackles_won_data.get('total') is not None else stats.get('tackles_total_simple')
                final_stats_map['tackles_won_percentage'] = tackles_won_data.get('percentage')
            else:
                
                final_stats_map['tackles_total'] = stats.get('tackles_total_simple')
                


            # Create the final tuple in the correct DB order
            # Default missing values to 0 for integer stats, None for floats/percentages
            stat_tuple = []
            for key in TEAM_STATS_DB_ORDER:
                value = final_stats_map.get(key)
                # Default integer columns to 0 if None, check column type implicitly by name
                if value is None and any(k in key for k in ['_successful', '_total', '_cards', '_kicks', 'shots_', 'chances', 'fouls', 'corners', 'saves', 'offsides', 'throw_ins', 'entries', 'dispossessed', 'interceptions', 'clearances']):
                     stat_tuple.append(0)
                else:
                     stat_tuple.append(value)

            # Validate tuple length
            if len(stat_tuple) == 56:
                parsed_stats_batch.append(tuple(stat_tuple))
            else:
                logging.error(f"Match {match_id}, Period {period_code}, Team {team_id}: Incorrect number of stats in tuple. Expected 56, got {len(stat_tuple)}")

    return parsed_stats_batch


async def _fetch_stats_data_pw(page: Page, match_id: str) -> Optional[Union[List[Dict], Dict[str, Any]]]:
    """Fetches statistics data for a given match_id using Playwright."""
    stats_api_url = f"https://www.sofascore.com/api/v1/event/{match_id}/statistics"
    event_page_url = f"https://www.sofascore.com/event/{match_id}" # Visiting page might help
    logging.info(f"    Intentando fetch de estadísticas para Match ID: {match_id} (API: {stats_api_url})")

    response = None
    try:

        logging.debug(f"    Realizando fetch directo a API: {stats_api_url}")
        response = await page.goto(stats_api_url, wait_until="commit", timeout=30000)

        if response is None:
            logging.error(f"    -> Error: page.goto a API /statistics para {match_id} devolvió None.")
            return {"error": 500, "message": "Playwright goto returned None"}

        status = response.status
        logging.info(f"    Respuesta API /statistics para {match_id}: Status {status}")

        if status == 200:
            content = await response.text()
            if content.strip().startswith("{") and content.strip().endswith("}"):
                 try:
                    data_object = json.loads(content)
                    stats_list = data_object.get("statistics")
                    if stats_list is not None and isinstance(stats_list, list):
                          return stats_list # Return list of stats objects
                    else:
                          logging.error(f"    -> Error: JSON de /statistics para {match_id} no contiene 'statistics' como lista.")
                          return {"error": 500, "message": "Invalid JSON structure: 'statistics' key missing or not a list"}
                 except json.JSONDecodeError as json_err:
                    logging.error(f"    -> Error: No se pudo decodificar el JSON de /statistics para {match_id}. Error: {json_err}. Contenido: {content[:300]}...")
                    return {"error": 500, "message": f"JSON Decode Error: {json_err}"}
            else:
                logging.error(f"    -> Error: La respuesta 200 de /statistics para {match_id} no parece ser un objeto JSON válido. Contenido: {content[:200]}...")
                return {"error": 500, "message": "Invalid JSON format in 200 response"}
        else:
            body = await response.text()
            logging.error(f"    -> Error en fetch de API de estadísticas para {match_id}: {status}")
            if status == 403: return {"error": 403, "message": "Forbidden"}
            if status == 404: return {"error": 404, "message": "Statistics not found"}
            return {"error": status, "message": f"HTTP Error {status} - Body: {body[:100]}"}

    except asyncio.TimeoutError:
        logging.error(f"    -> Error: Timeout durante fetch de /statistics para Match ID {match_id}")
        return {"error": 408, "message": "Request Timeout"}
    except Exception as e:
        logging.error(f"    -> Error inesperado durante fetch de /statistics para Match ID {match_id}: {type(e).__name__}", exc_info=False)
        return {"error": 500, "message": f"Unexpected error: {type(e).__name__}"}


async def process_team_stats_for_match(page: Page, match_id: int, home_team_id: int, away_team_id: int) -> bool:
    """
    Fetches team statistics for a single match, processes the data,
    and inserts it into the database.

    Args:
        page: Active Playwright Page object.
        match_id: The ID of the match to process.
        home_team_id: The ID of the home team.
        away_team_id: The ID of the away team.

    Returns:
        True if processing and database insertion were successful, False otherwise.
    """
    logging.info(f"  Procesando Estadísticas de Equipo Partido ID: {match_id}")
    await asyncio.sleep(random.uniform(1.5, 3.5)) # Delay

    stats_result = await _fetch_stats_data_pw(page, str(match_id))

    if stats_result is None or (isinstance(stats_result, dict) and "error" in stats_result):
        error_code = stats_result.get("error", 500) if isinstance(stats_result, dict) else 500
        error_msg = stats_result.get("message", "Fetch failed or returned None") if isinstance(stats_result, dict) else "Fetch failed"
        logging.error(f"    -> Falló la obtención de estadísticas para {match_id} (Error {error_code}): {error_msg}")
        # if error_code == 403: raise PlaywrightError("403 Forbidden") # Signal main loop
        return False # Failed to fetch

    if not isinstance(stats_result, list):
         logging.error(f"    -> Error: Fetch para {match_id} no devolvió una lista de estadísticas.")
         return False

    #Parse and Insert Data
    parsed_batch = []
    parse_error = False
    try:
        parsed_batch = _parse_statistics_data(stats_result, match_id, home_team_id, away_team_id)
    except Exception as parse_err:
        logging.error(f"    -> Error FATAL parseando datos de estadísticas para Match ID {match_id}: {parse_err}")
        traceback.print_exc()
        parse_error = True

    if parse_error or not parsed_batch:
        logging.warning(f"    -> No se encontraron/procesaron datos de estadísticas de equipo válidos para Match ID {match_id}. Saltando inserción.")
        return False

    #Database
    db_success = True
    try:
        await insert_team_stats_batch(parsed_batch)
        logging.info(f"    -> Insertadas/Actualizadas {len(parsed_batch)} filas de estadísticas de equipo para Match ID {match_id}.")
    except Exception as db_err:
        logging.error(f"    -> Error en base de datos durante inserción de stats de equipo para Match ID {match_id}: {db_err}", exc_info=True)
        db_success = False

    return db_success

