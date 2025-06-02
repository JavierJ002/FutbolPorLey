#players_statistics_extractor.py
import asyncio
import json
import random
import logging
from playwright.async_api import Page
import traceback
from typing import Any, Dict, Optional, Tuple
from config.driver_setup import (USER_AGENTS, _BASE_SOFASCORE_URL, _DEFAULT_TOURNAMENT_ID, _DEFAULT_TOURNAMENT_NAME,
                                _DEFAULT_TOURNAMENT_COUNTRY, _DEFAULT_SEASON_ID, _DEFAULT_SEASON_NAME)
from database_utils.db_utils import upsert_player, insert_player_stats_batch
# Convertion functions
from helpers.convert_stats import _safe_to_float, _safe_to_int

# Mapping from SofaScore API keys to database column names for player_match_stats
SOFASCORE_API_TO_DB_STATS_MAP = {
    # Order MUST match the VALUES clause in insert_player_stats_batch (implicitly via DB_STATS_ORDER)
    # IDs and basic info (handled separately in the tuple prefix)
    'minutesPlayed': 'minutes_played',
    'touches': 'touches',
    'goals': 'goals',
    'goalAssist': 'assists',
    'ownGoals': 'own_goals',
    'accuratePass': 'passes_accurate',
    'totalPass': 'passes_total',
    'keyPass': 'passes_key',
    'accurateLongBalls': 'long_balls_accurate',
    'totalLongBalls': 'long_balls_total',
    'accurateCross': 'crosses_accurate',
    'totalCross': 'crosses_total',
    'totalShoot': 'shots_total',
    'onTargetScoringAttempt': 'shots_on_target',
    'shotOffTarget': 'shots_off_target',
    'blockedScoringAttempt': 'shots_blocked_by_opponent',
    'dribbleWon': 'dribbles_successful',
    'dribbleAttempt': 'dribbles_attempts',
    'possessionLostCtrl': 'possession_lost',
    'dispossessed': 'dispossessed',
    'duelWon': 'duels_won', #SUMA VICTORIAS DUELOS AEREOS + SUELO
    'duelLost': 'duels_lost', # DERROTAS EN AEREOS Y SUELO
    'aerialWon': 'aerials_won', #DUELO AEREO GANADO
    'aerialLost': 'aerials_lost', #DERROTAS AEREO 
    'totalContest': 'ground_duels_total', #DRIBBLES
    'totalTackle': 'tackles',
    'interceptionWon': 'interceptions',
    'totalClearance': 'clearances',
    'outfielderBlock': 'shots_blocked_by_player',
    'challengeLost': 'dribbled_past',
    'fouls': 'fouls_committed',
    'wasFouled': 'fouls_suffered',
    'saves': 'saves',
    'punches': 'punches_made',
    'goodHighClaim': 'high_claims',
    'savedShotsFromInsideTheBox': 'saves_inside_box',
    'keeperSweeperWon': 'sweeper_keeper_successful',
    'totalKeeperSweeper': 'sweeper_keeper_total',
    # New player stats from user request (mapping API key guesses to DB keys)
    'goalsPrevented': 'goals_prevented', # API key guess (can be float)
    'runsOutSuccessful': 'runs_out_successful', # API key guess (int)
    'penaltySave': 'penalties_saved', # API key guess (int)
    'penaltyConceded': 'penalty_committed', # API key guess (int)
    'expectedGoals': 'expected_goals', # API key guess (xG, float)
    'expectedAssists': 'expected_assists', # API key guess (xA, float)
    'penaltyWon': 'penalty_won', # API key guess (int)
    'penaltyMiss': 'penalty_miss', # API key guess (int)
    'bigChanceMissed': 'big_chances_missed', # API key guess (int)
    # New player stats added from the *updated* user list (mapping API key guesses to DB keys)
    'errorLeadToAShot': 'errors_leading_to_shot', # API key guess (int)
    'bigChanceCreated': 'big_chances_created',     # API key guess (int)
    'errorLeadToAGoal': 'errors_leading_to_goal' # API key guess (int)
}

# Ordered list of DB stat keys corresponding to the VALUES clause in insert_player_stats_batch
# Excludes the first 8 fields which are handled separately (match_id, player_id, etc.).
DB_STATS_ORDER = [
    'minutes_played', 'touches', 'goals', 'assists', 'own_goals',
    'passes_accurate', 'passes_total', 'passes_key', 'long_balls_accurate', 'long_balls_total',
    'crosses_accurate', 'crosses_total', 'shots_total', 'shots_on_target', 'shots_off_target',
    'shots_blocked_by_opponent', 'dribbles_successful', 'dribbles_attempts', 'possession_lost',
    'dispossessed', 'duels_won', 'duels_lost', 'aerials_won', 'aerials_lost',
    'ground_duels_won', 'ground_duels_total', 'tackles', 'interceptions', 'clearances',
    'shots_blocked_by_player', 'dribbled_past', 'fouls_committed', 'fouls_suffered',
    'saves', 'punches_made', 'high_claims', 'saves_inside_box',
    'sweeper_keeper_successful', 'sweeper_keeper_total',
    # New player stats DB columns added in the previous request
    'goals_prevented', 'runs_out_successful', 'penalties_saved', 'penalty_committed',
    'expected_goals', 'expected_assists', 'penalty_won', 'penalty_miss', 'big_chances_missed',
    # New player stats DB columns from the *updated* user list
    'errors_leading_to_shot', 'big_chances_created', 'errors_leading_to_goal'
] # Total stats columns: 39 (original) + 9 (prev new) + 3 (new) = 51

def _process_player_entry(player_entry: Dict[str, Any], match_id: int, team_id: int) -> Optional[Tuple[Tuple, Tuple]]:
    """
    Processes a single player entry from the lineup API data.

    Returns:
        A tuple containing two tuples:
        1. Player data tuple for upsert_player.
        2. Player stats tuple for insert_player_stats_batch.
        Returns None if essential data is missing.
    """
    player_info = player_entry.get("player")
    stats_raw = player_entry.get("statistics", {})

    if not player_info or not player_info.get("id"):
        logging.warning(f"Match {match_id}: Datos básicos faltantes para entrada de jugador: {player_entry.get('player', {}).get('id')}")
        return None

    player_id = player_info["id"]
    player_name = player_info.get("name")
    height_cm = _safe_to_int(player_info.get("height"))
    primary_position = player_info.get("position")
    played_position = player_entry.get("position", primary_position) # Position played in this match
    country_name = player_info.get("country", {}).get("name")
    jersey_number = _safe_to_int(player_entry.get("jerseyNumber"))
    is_substitute = player_entry.get("substitute", False)
    market_value_eur = _safe_to_int(player_info.get("proposedMarketValueRaw", {}).get("value"))
    sofascore_rating = _safe_to_float(stats_raw.get('rating')) # Rating is in stats

    # Tuple for player data
    player_tuple = (
        player_id,
        player_name,
        height_cm,
        primary_position,
        country_name
    )

    # --- Prepare stats tuple ---
    # Start with the fixed fields in correct order (8 fields)
    stats_tuple_prefix = [
        match_id,
        player_id,
        team_id,
        is_substitute,
        played_position,
        jersey_number,
        market_value_eur, # Not by timestamp... Mostlikely data leakage if keeped
        sofascore_rating
    ]

    # Extract stats based on the map and order
    extracted_stats = []
    calculated_stats = {} # For stats derived from others

    # Calculate derived stats first if needed
    duels_won = _safe_to_int(stats_raw.get('duelWon'))
    aerials_won = _safe_to_int(stats_raw.get('aerialWon'))
    # Ground duels won might need calculation if not provided directly
    if duels_won is not None and aerials_won is not None:
         # Check if groundDuelWon exists first, otherwise calculate
         calculated_stats['ground_duels_won'] = _safe_to_int(stats_raw.get('groundDuelWon')) if stats_raw.get('groundDuelWon') is not None else (duels_won - aerials_won)
    else:
        calculated_stats['ground_duels_won'] = _safe_to_int(stats_raw.get('groundDuelWon'))

    calculated_stats['duels_lost'] = _safe_to_int(stats_raw.get('duelLost'))
    calculated_stats['aerials_lost'] = _safe_to_int(stats_raw.get('aerialLost'))


    for db_key in DB_STATS_ORDER:
        found_value = None
        if db_key in calculated_stats:
            found_value = calculated_stats[db_key]
        else:
            # Find the corresponding API key in the map
            api_key = next((api for api, db in SOFASCORE_API_TO_DB_STATS_MAP.items() if db == db_key), None)
            if api_key and api_key in stats_raw:
                raw_value = stats_raw[api_key]

                # Use appropriate converter based on expected data type for new stats
                if db_key in ['expected_goals', 'expected_assists', 'goals_prevented']:
                     found_value = _safe_to_float(raw_value) # xG, xA, goals_prevented can be floats
                elif db_key == 'sofascore_rating':
                     found_value = _safe_to_float(raw_value) # Rating is a float
                else:
                    found_value = _safe_to_int(raw_value) # Most other stats are integers (counts)


        # Append extracted/calculated value, defaulting to 0 for most counts or None for floats/percentages if not found
        if found_value is None:
             if db_key in ['expected_goals', 'expected_assists', 'sofascore_rating', 'goals_prevented']:
                  extracted_stats.append(None) # Default floats/ratings to None
             else:
                  extracted_stats.append(0) # Default integer counts to 0
        else:
             extracted_stats.append(found_value)

    # Combine prefix and extracted stats
    player_stats_tuple = tuple(stats_tuple_prefix + extracted_stats)

    # Validate length (8 prefix + 51 stats = 59)
    expected_length = 8 + len(DB_STATS_ORDER) # 8 prefix + 51 stats = 59
    if len(player_stats_tuple) != expected_length:
        logging.error(f"Match {match_id}, Player {player_id}: Incorrect number of stats generated. Expected {expected_length}, got {len(player_stats_tuple)}. DB_STATS_ORDER length: {len(DB_STATS_ORDER)}")
        return None

    return player_tuple, player_stats_tuple


async def _fetch_lineup_data_pw(page: Page, match_id: str) -> Optional[Dict]:
    """Fetches lineup data for a given match_id using Playwright."""
    lineup_api_url = f"https://www.sofascore.com/api/v1/event/{match_id}/lineups"
    logging.info(f"    Intentando fetch de alineaciones/jugadores para Match ID: {match_id} (API: {lineup_api_url})")
    response = None
    try:
        response = await page.goto(lineup_api_url, wait_until="commit", timeout=30000)

        if response is None:
            logging.error(f"    -> Error: page.goto a API /lineups para {match_id} devolvió None.")
            return {"error": 500, "message": "Playwright goto returned None"}

        status = response.status
        logging.info(f"    Respuesta API /lineups para {match_id}: Status {status}")

        if status == 200:
            content = await response.text()
            if content.strip().startswith("{") and content.strip().endswith("}"):
                 try:
                     lineup_object = json.loads(content)
                     return lineup_object # Return the raw JSON object on success
                 except json.JSONDecodeError as json_err:
                     logging.error(f"    -> Error: No se pudo decodificar el JSON de /lineups para {match_id}. Error: {json_err}. Contenido: {content[:300]}...")
                     return {"error": 500, "message": f"JSON Decode Error: {json_err}"}
            else:
                logging.error(f"    -> Error: La respuesta 200 de /lineups para {match_id} no parece ser un objeto JSON válido. Contenido: {content[:200]}...")
                return {"error": 500, "message": "Invalid JSON format in 200 response"}
        else:
            logging.error(f"    -> Error en fetch de API /lineups para {match_id}: {status}")
            if status == 403: return {"error": 403, "message": "Forbidden"}
            if status == 404: return {"error": 404, "message": "Lineups not found"}
            return {"error": status, "message": f"HTTP Error {status}"}

    except asyncio.TimeoutError:
        logging.error(f"    -> Error: Timeout durante fetch de /lineups para Match ID {match_id}")
        return {"error": 408, "message": "Request Timeout"}
    except Exception as e:
        logging.error(f"    -> Error inesperado durante fetch de /lineups para Match ID {match_id}: {type(e).__name__}", exc_info=False)
        return {"error": 500, "message": f"Unexpected error: {type(e).__name__}"}


async def process_player_stats_for_match(page: Page, match_id: int, home_team_id: int, away_team_id: int) -> bool:
    """
    Fetches player lineup and stats for a single match, processes the data,
    and upserts players and their stats into the database.

    Args:
        page: Active Playwright Page object.
        match_id: The ID of the match to process.
        home_team_id: The ID of the home team.
        away_team_id: The ID of the away team.

    Returns:
        True if processing and database insertion were successful, False otherwise.
    """

    logging.info(f"  Procesando Alineaciones/Jugadores Partido ID: {match_id}")
    await asyncio.sleep(random.uniform(1.5, 3.5)) # Shorter delay as browser context is reused

    lineup_raw_data = await _fetch_lineup_data_pw(page, str(match_id))

    if lineup_raw_data is None or (isinstance(lineup_raw_data, dict) and "error" in lineup_raw_data):
        error_code = lineup_raw_data.get("error", 500) if isinstance(lineup_raw_data, dict) else 500
        error_msg = lineup_raw_data.get("message", "Fetch failed or returned None") if isinstance(lineup_raw_data, dict) else "Fetch failed"
        logging.error(f"    -> Falló la obtención de alineaciones para {match_id} (Error {error_code}): {error_msg}")
        return False, None

    players_to_upsert = []
    player_stats_to_insert = []
    parse_error = False

    try:
        home_data = lineup_raw_data.get("home", {})
        away_data = lineup_raw_data.get("away", {})

        for player_entry in home_data.get("players", []):
            processed_data = _process_player_entry(player_entry, match_id, home_team_id)
            if processed_data:
                players_to_upsert.append(processed_data[0])
                player_stats_to_insert.append(processed_data[1])

        # Process Away Team Players
        for player_entry in away_data.get("players", []):
            processed_data = _process_player_entry(player_entry, match_id, away_team_id)
            if processed_data:
                players_to_upsert.append(processed_data[0])
                player_stats_to_insert.append(processed_data[1])

    except Exception as parse_err:
        logging.error(f"    -> Error FATAL parseando datos de alineación para Match ID {match_id}: {parse_err}")
        traceback.print_exc()
        parse_error = True

    if parse_error or not player_stats_to_insert:
        logging.warning(f"    -> No se encontraron/procesaron datos de jugadores válidos para Match ID {match_id}. Saltando inserción.")
        return False, None # Indicate failure if parsing failed or no players found, return tuple

    #Database

    db_success = True
    try:
        player_upsert_tasks = [upsert_player(*player_tuple) for player_tuple in players_to_upsert]
        await asyncio.gather(*player_upsert_tasks)
        logging.info(f"    -> Upserted {len(players_to_upsert)} jugadores para Match ID {match_id}.")

        await insert_player_stats_batch(player_stats_to_insert)
        logging.info(f"    -> Insertadas/Actualizadas {len(player_stats_to_insert)} estadísticas de jugador para Match ID {match_id}.")

    except Exception as db_err:
        logging.error(f"    -> Error en base de datos durante inserción de jugadores/stats para Match ID {match_id}: {db_err}", exc_info=True)
        return False, None # Return tuple indicating failure

    # If DB operations succeeded, return success status and the extracted aggregate data
    # Note: These calculations are based on the tuple structure, which hasn't changed its *prefix*,
    # but the total length of the stats part has increased. Accessing by index 7 (rating) and 6 (value) is fine.
    home_ratings = [p[7] for p in player_stats_to_insert if p[2] == home_team_id and p[7] is not None and p[7] > 0]
    home_values = [p[6] for p in player_stats_to_insert if p[2] == home_team_id and p[6] is not None]
    away_ratings = [p[7] for p in player_stats_to_insert if p[2] == away_team_id and p[7] is not None and p[7] > 0]
    away_values = [p[6] for p in player_stats_to_insert if p[2] == away_team_id and p[6] is not None]

    aggregate_data = {
        "home": {
            "formation": home_data.get("formation"),
            "avg_rating": round(sum(home_ratings) / len(home_ratings), 2) if home_ratings else None,
            "total_value": sum(home_values) if home_values else 0
        },
        "away": {
            "formation": away_data.get("formation"),
            "avg_rating": round(sum(away_ratings) / len(away_ratings), 2) if away_ratings else None,
            "total_value": sum(away_values) if away_values else 0
        }
    }

    return db_success, aggregate_data