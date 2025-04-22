# extractors/incidents_shots_extractor.py
import asyncio
import json
import logging
from playwright.async_api import Page
from typing import List, Dict, Any, Optional, Tuple

# Assuming db_utils contains the necessary upsert and execute_query functions
from database_utils.db_utils import (
    upsert_player, execute_query
)

# Configure logging for this module
logging.getLogger(__name__).setLevel(logging.INFO)

async def _get_team_id(is_home: bool, home_team_id: int, away_team_id: int) -> int:
    """Helper to determine team ID based on isHome flag."""
    return home_team_id if is_home else away_team_id

async def _upsert_player_from_data(player_data: Dict[str, Any]):
    """Upserts player data if available in the incident/shot structure."""
    if not player_data or not player_data.get("id"):
        return None
    player_id = player_data["id"]
    name = player_data.get("name")
    height = player_data.get("height") # Assuming height is in cm
    position = player_data.get("position")
    # Country name is not directly available in the provided JSON snippets for players
    country_name = None # Or try to infer if team country is available and reliable

    if player_id and name:
        await upsert_player(player_id, name, height, position, country_name)
        return player_id
    return None


async def process_incidents_and_shots_for_match(
    page: Page,
    match_id: int,
    home_team_id: int,
    away_team_id: int
) -> bool:
    """
    Fetches incident and shotmap data for a match and inserts it into the database.

    Args:
        page: Playwright Page object for fetching.
        match_id: The ID of the match.
        home_team_id: The database ID of the home team.
        away_team_id: The database ID of the away team.

    Returns:
        True if processing was mostly successful, False otherwise.
        Logs errors for specific failed incidents/shots.
    """
    logging.info(f"  -> Procesando incidentes y disparos para Match ID: {match_id}")
    incidents_url = f"https://www.sofascore.com/api/v1/event/{match_id}/incidents"
    shotmap_url = f"https://www.sofascore.com/api/v1/event/{match_id}/shotmap"

    incidents_data = None
    shotmap_data = None
    success = True

    try:
        # Fetch incidents
        logging.debug(f"    Fetching incidents from: {incidents_url}")
        response = await page.goto(incidents_url, wait_until="commit", timeout=30000)
        if response and response.status == 200:
            incidents_data = await response.json()
            logging.debug(f"    Fetched {len(incidents_data.get('incidents', []))} incidents.")
        else:
            logging.error(f"    Failed to fetch incidents for Match ID {match_id}. Status: {response.status if response else 'N/A'}")
            success = False

        # Fetch shotmap
        logging.debug(f"    Fetching shotmap from: {shotmap_url}")
        response = await page.goto(shotmap_url, wait_until="commit", timeout=30000)
        if response and response.status == 200:
            shotmap_data = await response.json()
            logging.debug(f"    Fetched {len(shotmap_data.get('shotmap', []))} shots.")
        else:
            logging.error(f"    Failed to fetch shotmap for Match ID {match_id}. Status: {response.status if response else 'N/A'}")
            success = False # Consider fetching stats without shots, so not a full failure? Maybe, depends on requirements. Let's allow partial success.

    except Exception as e:
        logging.error(f"    Error fetching API data for Match ID {match_id}: {type(e).__name__} - {e}", exc_info=False)
        return False # Fatal error for this match

    # --- Upsert Players ---
    all_players_data = []
    if incidents_data and 'incidents' in incidents_data:
        for incident in incidents_data['incidents']:
            if 'player' in incident and incident['player']: all_players_data.append(incident['player'])
            if 'playerIn' in incident and incident['playerIn']: all_players_data.append(incident['playerIn'])
            if 'playerOut' in incident and incident['playerOut']: all_players_data.append(incident['playerOut'])
            if 'assist1' in incident and incident['assist1']: all_players_data.append(incident['assist1'])
            # Check nested actions for goalscorers/assistants/keepers in passing networks if they exist
            if 'footballPassingNetworkAction' in incident:
                 for action in incident['footballPassingNetworkAction']:
                     if 'player' in action and action['player']: all_players_data.append(action['player'])
                     if 'goalkeeper' in action and action['goalkeeper']: all_players_data.append(action['goalkeeper'])

    if shotmap_data and 'shotmap' in shotmap_data:
         for shot in shotmap_data['shotmap']:
             if 'player' in shot and shot['player']: all_players_data.append(shot['player'])
             if 'goalkeeper' in shot and shot['goalkeeper']: all_players_data.append(shot['goalkeeper'])

    # Upsert unique players
    unique_players = {p['id']: p for p in all_players_data if p and p.get('id')}
    player_upsert_tasks = [_upsert_player_from_data(p_data) for p_data in unique_players.values()]
    await asyncio.gather(*player_upsert_tasks)
    logging.debug(f"    Upserted {len(unique_players)} unique players for Match ID {match_id}")

    # --- Process Incidents ---
    if incidents_data and 'incidents' in incidents_data:
        for incident in incidents_data['incidents']:
            try:
                incident_type = incident.get("incidentType")
                minute = incident.get("time")
                is_home = incident.get("isHome")
                team_id = await _get_team_id(is_home, home_team_id, away_team_id) if is_home is not None else None
                # Player ID for the base event is often the main participant, but depends on type
                player_id_base = None
                if 'player' in incident and incident['player']: player_id_base = incident['player'].get('id')
                elif 'playerIn' in incident and incident['playerIn']: player_id_base = incident['playerIn'].get('id') # Subs link base to PlayerIn? Check schema... No, base has player_id, which is nullable. Let's link subs to player_out as the event HAPPENS to them. Or leave null. Schema says player_id NULLABLE. Let's leave null if ambiguous. PlayerID is required by schema. Okay, the schema for match_event_base requires player_id NOT NULL? Re-reading tables.sql: `player_id BIGINT NULL REFERENCES players(player_id)`. OK, it IS nullable. Good. Let's use player.id where clear, else NULL.

                # Skip period/injury time incidents - not needed in event tables
                if incident_type in ["period", "injuryTime"]:
                    continue
                # Skip Manager cards - schema is for players
                if incident_type == "card" and incident.get("manager"):
                    continue

                # Determine player_id for match_event_base where applicable
                if incident_type in ["goal", "card", "missedPenalty"]: # MissedPenalty incidentType not in sample, but if it exists
                     if 'player' in incident and incident['player']:
                          player_id_base = incident['player'].get('id')
                elif incident_type == "substitution":
                     # Link substitution event to the player being substituted out?
                     if 'playerOut' in incident and incident['playerOut']:
                          player_id_base = incident['playerOut'].get('id')
                elif incident_type == "varDecision":
                    # VAR decision might not be tied to a single player, or the player reviewed
                    # The schema allows player_id to be NULL. Let's keep it null for VAR unless a player is explicitly involved.
                    if 'player' in incident and incident['player']: # Sometimes VAR involves a specific player (e.g. penalty awarded to X)
                         player_id_base = incident['player'].get('id')
                    else:
                         player_id_base = None # Most VAR events aren't player-specific in the timeline

                # Ensure required fields for match_event_base are present
                if minute is None or team_id is None:
                     logging.warning(f"      Skipping incident (type: {incident_type}) due to missing minute or team ID: {incident}")
                     continue


                # Insert into match_event_base first to get event_id
                sql_base = """
                    INSERT INTO match_event_base (match_id, minute, event_type, team_id, player_id)
                    VALUES ($1, $2, $3, $4, $5)
                    RETURNING event_id;
                """
                base_event_params = (match_id, minute, incident_type, team_id, player_id_base)
                base_event_result = await execute_query(sql_base, base_event_params, fetch=True, many=False)

                if not base_event_result:
                    logging.error(f"      Failed to insert match_event_base for incident (type: {incident_type}) in Match ID {match_id}.")
                    success = False
                    continue

                event_id = base_event_result['event_id']

                # Insert into specific event tables
                if incident_type == "goal":
                    scoring_player_id = incident.get('player', {}).get('id')
                    assist_player_id = incident.get('assist1', {}).get('id')
                    goal_type = incident.get('goalType') # 'regular', 'penalty', etc.

                    # Extract body_part from nested footballPassingNetworkAction if available
                    body_part = None
                    if 'footballPassingNetworkAction' in incident:
                         for action in incident['footballPassingNetworkAction']:
                             if action.get('eventType') == 'goal' and action.get('bodyPart'):
                                 body_part = action['bodyPart']
                                 break # Found the goal action

                    if scoring_player_id: # Goal must have a scorer
                        sql_goal = """
                            INSERT INTO goal_events (event_id, scoring_player_id, assist_player_id, goal_type, body_part)
                            VALUES ($1, $2, $3, $4, $5);
                        """
                        goal_params = (event_id, scoring_player_id, assist_player_id, goal_type, body_part)
                        if not await execute_query(sql_goal, goal_params):
                             logging.error(f"      Failed to insert goal_events for event_id {event_id} (Match ID {match_id}).")
                             success = False
                    else:
                         logging.warning(f"      Goal incident missing scoring player for event_id {event_id} (Match ID {match_id}).")
                         success = False


                elif incident_type == "card":
                    card_type = incident.get('incidentClass') # 'yellow', 'red'
                    reason = incident.get('reason')
                    is_rescinded = incident.get('rescinded', False) # Default to False if not present
                    player_id_card = incident.get('player', {}).get('id') # Player ID for card is required by schema

                    if player_id_card and card_type:
                         sql_card = """
                              INSERT INTO card_events (event_id, card_type, reason, is_rescinded)
                              VALUES ($1, $2, $3, $4);
                         """
                         card_params = (event_id, card_type, reason, is_rescinded)
                         if not await execute_query(sql_card, card_params):
                              logging.error(f"      Failed to insert card_events for event_id {event_id} (Match ID {match_id}).")
                              success = False
                    else:
                         logging.warning(f"      Card incident missing player or type for event_id {event_id} (Match ID {match_id}). Incident: {incident}")
                         success = False


                elif incident_type == "substitution":
                    player_in_id = incident.get('playerIn', {}).get('id')
                    player_out_id = incident.get('playerOut', {}).get('id')

                    if player_in_id and player_out_id:
                        sql_sub = """
                            INSERT INTO substitution_events (event_id, player_in_id, player_out_id)
                            VALUES ($1, $2, $3);
                        """
                        sub_params = (event_id, player_in_id, player_out_id)
                        if not await execute_query(sql_sub, sub_params):
                             logging.error(f"      Failed to insert substitution_events for event_id {event_id} (Match ID {match_id}).")
                             success = False
                    else:
                         logging.warning(f"      Substitution incident missing playerIn or playerOut for event_id {event_id} (Match ID {match_id}). Incident: {incident}")
                         success = False

                elif incident_type == "varDecision":
                    decision_outcome = incident.get('incidentClass') # e.g., 'goalAwarded', 'penaltyNotAwarded'
                    decision_type = 'VAR decision' # Static for now, as no specific type given
                    # incident_class_reviewed = None # Not available in sample JSON

                    sql_var = """
                        INSERT INTO var_decision_events (event_id, decision_type, decision_outcome, incident_class_reviewed)
                        VALUES ($1, $2, $3, $4);
                    """
                    var_params = (event_id, decision_type, decision_outcome, None)
                    if not await execute_query(sql_var, var_params):
                         logging.error(f"      Failed to insert var_decision_events for event_id {event_id} (Match ID {match_id}).")
                         success = False

                # Note: Disallowed goals are not explicitly structured as incidentType='disallowedGoal' in the sample.
                # They might appear as goal incidentType with confirmed=false, possibly linked to a VAR decision.
                # Based on the schema, if there was a clear "disallowed" incidentClass, we would handle it here:
                # elif incident.get('incidentClass') == 'disallowed': # Or other indicator
                #    reason = incident.get('reason') # Or infer reason
                #    sql_disallowed = """
                #        INSERT INTO disallowed_goal_events (event_id, reason)
                #        VALUES ($1, $2);
                #    """
                #    if not await execute_query(sql_disallowed, (event_id, reason)):
                #         logging.error(f"      Failed to insert disallowed_goal_events for event_id {event_id} (Match ID {match_id}).")
                #         success = False


            except Exception as e:
                logging.error(f"    Error processing incident {incident.get('id', 'N/A')} (type: {incident.get('incidentType')}) for Match ID {match_id}: {type(e).__name__} - {e}", exc_info=False)
                success = False # Mark match processing as failed if any incident fails

    # --- Process Shotmap ---
    if shotmap_data and 'shotmap' in shotmap_data:
        for shot in shotmap_data['shotmap']:
            try:
                # Only process actual 'shot' incident types from shotmap
                if shot.get('incidentType') != 'shot':
                    continue

                shooter_player_data = shot.get('player')
                if not shooter_player_data or not shooter_player_data.get('id'):
                     logging.warning(f"      Skipping shot due to missing player data in Match ID {match_id}. Shot: {shot}")
                     success = False
                     continue # Cannot process a shot without a player

                shooter_player_id = shooter_player_data['id']
                minute = shot.get('time')
                added_time = shot.get('addedTime')
                is_home = shot.get('isHome')

                if minute is None or is_home is None:
                     logging.warning(f"      Skipping shot due to missing minute or isHome flag in Match ID {match_id}. Shot: {shot}")
                     success = False
                     continue

                team_id = await _get_team_id(is_home, home_team_id, away_team_id)


                # Insert into match_event_base for the shot event
                sql_base_shot = """
                    INSERT INTO match_event_base (match_id, minute, event_type, team_id, player_id)
                    VALUES ($1, $2, $3, $4, $5)
                    RETURNING event_id;
                """
                base_event_params_shot = (match_id, minute, 'shot', team_id, shooter_player_id)
                base_event_result_shot = await execute_query(sql_base_shot, base_event_params_shot, fetch=True, many=False)

                if not base_event_result_shot:
                    logging.error(f"      Failed to insert match_event_base for shot incident in Match ID {match_id}.")
                    success = False
                    continue

                event_id_shot = base_event_result_shot['event_id']

                # Insert into shot_events
                shot_outcome = shot.get('shotType') # 'goal', 'miss', 'save', 'block', 'post'
                situation = shot.get('situation')
                body_part = shot.get('bodyPart')
                xg = shot.get('xg')
                xgot = shot.get('xgot')

                player_coords = shot.get('playerCoordinates', {})
                player_coord_x = player_coords.get('x')
                player_coord_y = player_coords.get('y')

                goal_mouth_location = shot.get('goalMouthLocation')
                goal_mouth_coords = shot.get('goalMouthCoordinates', {})
                goal_mouth_coord_x = goal_mouth_coords.get('x')
                goal_mouth_coord_y = goal_mouth_coords.get('y')
                goal_mouth_coord_z = goal_mouth_coords.get('z')

                block_coords = shot.get('blockCoordinates', {})
                block_coord_x = block_coords.get('x')
                block_coord_y = block_coords.get('y')

                goalkeeper_id = shot.get('goalkeeper', {}).get('id') if shot.get('goalkeeper') else None


                sql_shot_event = """
                    INSERT INTO shot_events (
                        event_id, shooter_player_id, shot_outcome, situation, body_part, xg, xgot,
                        player_coord_x, player_coord_y, goal_mouth_location, goal_mouth_coord_x,
                        goal_mouth_coord_y, goal_mouth_coord_z, block_coord_x, block_coord_y,
                        goalkeeper_id, added_time
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17);
                """
                shot_params = (
                    event_id_shot, shooter_player_id, shot_outcome, situation, body_part, xg, xgot,
                    player_coord_x, player_coord_y, goal_mouth_location, goal_mouth_coord_x,
                    goal_mouth_coord_y, goal_mouth_coord_z, block_coord_x, block_coord_y,
                    goalkeeper_id, added_time
                )
                if not await execute_query(sql_shot_event, shot_params):
                     logging.error(f"      Failed to insert shot_events for event_id {event_id_shot} (Match ID {match_id}).")
                     success = False

                # Handle missed penalties explicitly from shotmap
                if shot_outcome == 'miss' and situation == 'penalty':
                    sql_missed_penalty = """
                        INSERT INTO missed_penalty_events (event_id, outcome)
                        VALUES ($1, $2);
                    """
                    missed_penalty_params = (event_id_shot, 'missed') # Or use shot_outcome if more detailed
                    if not await execute_query(sql_missed_penalty, missed_penalty_params):
                        logging.error(f"      Failed to insert missed_penalty_events for event_id {event_id_shot} (Match ID {match_id}).")
                        success = False

            except Exception as e:
                logging.error(f"    Error processing shot (ID: {shot.get('id', 'N/A')}, player: {shot.get('player', {}).get('name', 'N/A')}) for Match ID {match_id}: {type(e).__name__} - {e}", exc_info=False)
                success = False # Mark match processing as failed if any shot fails


    logging.info(f"  -> Finalizado procesamiento de incidentes y disparos para Match ID: {match_id}")
    return success