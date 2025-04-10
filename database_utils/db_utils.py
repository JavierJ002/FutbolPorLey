# database_utils/db_utils.py
import asyncpg
import os
import re 
from dotenv import load_dotenv
import logging
from typing import List, Dict, Any, Tuple, Optional, Union

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv(encoding='utf-8')

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

db_pool: Optional[asyncpg.Pool] = None

async def init_db_pool():
    """Inicializa el pool de conexiones asyncpg."""
    global db_pool
    if db_pool:
        logging.info("El pool de conexiones ya está inicializado.")
        return db_pool

    try:
        db_pool = await asyncpg.create_pool(
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            port=DB_PORT,
            min_size=1,
            max_size=5, 
            command_timeout=50 
        )
        logging.info("Pool de conexiones asyncpg inicializado.")
        return db_pool
    except (Exception, asyncpg.PostgresError) as error:
        logging.error(f"Error al inicializar el pool de conexiones asyncpg: {error}")
        db_pool = None
        raise 

async def close_db_pool():
    """Cierra el pool de conexiones asyncpg."""
    global db_pool
    if db_pool:
        try:
            await db_pool.close()
            logging.info("Pool de conexiones asyncpg cerrado.")
            db_pool = None
        except Exception as e:
            logging.error(f"Error cerrando el pool de conexiones asyncpg: {e}")
    else:
        logging.info("El pool de conexiones asyncpg no estaba inicializado o ya fue cerrado.")

async def execute_query(sql: str, params: Optional[Tuple] = None, fetch: bool = False, many: bool = False) -> Optional[Union[List[asyncpg.Record], asyncpg.Record, str]]:
    """
    Ejecuta una consulta SQL de forma asíncrona usando el pool.

    Args:
        sql (str): La consulta SQL parametrizada (usando $1, $2...).
        params (tuple, optional): Tupla de parámetros para la consulta. Defaults to None.
        fetch (bool): Si True, devuelve resultados. Defaults to False.
        many (bool): Si True y fetch=True, devuelve todos los resultados (fetch). Si False, devuelve uno (fetchrow).

    Returns:
        Optional[Union[List[asyncpg.Record], asyncpg.Record, str]]:
            - List[Record] si fetch=True y many=True.
            - Record si fetch=True y many=False.
            - str con estado (e.g., 'INSERT 0 1') si fetch=False y la consulta es INSERT/UPDATE/DELETE.
            - None en caso de error o si no hay pool.
    """
    if not db_pool:
        logging.error("El pool de conexiones no está disponible.")
        return None

    # Convert %s placeholders to $1, $2, ... using re.sub
    count = 0
    def repl(match):
        nonlocal count
        count += 1
        return f"${count}"
    sql = re.sub(r'%s', repl, sql)

    async with db_pool.acquire() as connection:
        try:
            if fetch:
                if many:
                    result = await connection.fetch(sql, *params if params else [])
                    logging.debug(f"Ejecutada SQL (fetch many): {sql[:100]}... con params: {params}")
                    return result
                else:
                    result = await connection.fetchrow(sql, *params if params else [])
                    logging.debug(f"Ejecutada SQL (fetch row): {sql[:100]}... con params: {params}")
                    return result
            else:
                # Para INSERT/UPDATE/DELETE, execute devuelve el estado (e.g., 'INSERT 0 1')
                status = await connection.execute(sql, *params if params else [])
                logging.debug(f"Ejecutada SQL (execute): {sql[:100]}... con params: {params} -> Status: {status}")
                return status 

        except (asyncpg.PostgresError, OSError) as error: # OSError puede ocurrir si la conexión se pierde
            logging.error(f"Error ejecutando SQL: {sql[:100]}... Error: {error}")
            return None
        except Exception as e:
             logging.error(f"Error inesperado ejecutando SQL: {sql[:100]}... Error: {type(e).__name__} - {e}")
             return None


async def execute_many(sql: str, data_list: List[Tuple]):
    """
    Ejecuta una consulta SQL para múltiples filas de datos (INSERT/UPDATE) de forma asíncrona.

    Args:
        sql (str): La consulta SQL parametrizada (usando $1, $2...).
        data_list (List[Tuple]): Lista de tuplas, cada tupla son los parámetros para una fila.

    Returns:
        bool: True si la ejecución fue exitosa (incluso si 0 filas afectadas), False en caso de error.
    """
    if not db_pool:
        logging.error("El pool de conexiones no está disponible para execute_many.")
        return False
    if not data_list:
        logging.warning("execute_many llamado con lista de datos vacía.")
        return True 

    count = 0
    def repl(match):
        nonlocal count
        count += 1
        return f"${count}"
    sql = re.sub(r'%s', repl, sql)


    async with db_pool.acquire() as connection:
        async with connection.transaction():
            try:
                await connection.executemany(sql, data_list)
                logging.info(f"Ejecutado lote SQL ({len(data_list)} filas): {sql[:100]}...")
                return True
            except (asyncpg.PostgresError, OSError) as error:
                logging.error(f"Error ejecutando lote SQL: {sql[:100]}... Error: {error}")
                return False
            except Exception as e:
                 logging.error(f"Error inesperado ejecutando lote SQL: {sql[:100]}... Error: {type(e).__name__} - {e}")
                 return False

#Funciones específicas de Inserción/Actualización 


async def upsert_tournament(tournament_id: int, name: str, country: Optional[str]):
    sql = """
        INSERT INTO tournaments (tournament_id, name, country_name)
        VALUES ($1, $2, $3)
        ON CONFLICT (tournament_id) DO NOTHING;
    """
    await execute_query(sql, (tournament_id, name, country))

async def upsert_season(season_id: int, tournament_id: int, name: str):
    sql = """
        INSERT INTO seasons (season_id, tournament_id, name)
        VALUES ($1, $2, $3)
        ON CONFLICT (season_id) DO NOTHING;
    """
    await execute_query(sql, (season_id, tournament_id, name))

async def upsert_team(team_id: int, name: str, country: Optional[str]):
    sql = """
        INSERT INTO teams (team_id, name, country)
        VALUES ($1, $2, $3)
        ON CONFLICT (team_id) DO UPDATE SET
            name = EXCLUDED.name,
            country = EXCLUDED.country;
    """
    await execute_query(sql, (team_id, name, country))

async def upsert_player(player_id: int, name: str, height: Optional[int], position: Optional[str], country: Optional[str]):
    sql = """
        INSERT INTO players (player_id, name, height_cm, primary_position, country_name)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (player_id) DO UPDATE SET
            name = EXCLUDED.name,
            height_cm = EXCLUDED.height_cm,
            primary_position = EXCLUDED.primary_position,
            country_name = EXCLUDED.country_name;
    """
    await execute_query(sql, (player_id, name, height, position, country))

async def upsert_match(match_id: int, season_id: int, round_num: Optional[int], round_name: Optional[str], dt_utc: Any,
                 home_id: int, away_id: int, home_score: Optional[int] = None,
                 away_score: Optional[int] = None, ht_home: Optional[int] = None,
                 ht_away: Optional[int] = None):
    sql = """
        INSERT INTO matches (match_id, season_id, round_number, round_name, match_datetime_utc,
                             home_team_id, away_team_id, home_score, away_score,
                             home_score_ht, away_score_ht)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        ON CONFLICT (match_id) DO UPDATE SET
            season_id = EXCLUDED.season_id,
            round_number = EXCLUDED.round_number,
            round_name = EXCLUDED.round_name,
            match_datetime_utc = EXCLUDED.match_datetime_utc,
            home_team_id = EXCLUDED.home_team_id,
            away_team_id = EXCLUDED.away_team_id,
            home_score = EXCLUDED.home_score,
            away_score = EXCLUDED.away_score,
            home_score_ht = EXCLUDED.home_score_ht,
            away_score_ht = EXCLUDED.away_score_ht;
    """
    params = (match_id, season_id, round_num, round_name, dt_utc, home_id, away_id,
              home_score, away_score, ht_home, ht_away)
    await execute_query(sql, params)

async def insert_player_stats_batch(player_stats_list: List[Tuple]):
    """
    Inserta un lote de estadísticas de jugadores de forma asíncrona.
    La tupla debe coincidir con el orden de las columnas en SQL.
    """
    if not player_stats_list: return

    sql = """
        INSERT INTO player_match_stats (
            match_id, player_id, team_id, is_substitute, played_position, jersey_number,
            market_value_eur_at_match, sofascore_rating, minutes_played, touches, goals, assists,
            own_goals, passes_accurate, passes_total, passes_key, long_balls_accurate, long_balls_total,
            crosses_accurate, crosses_total, shots_total, shots_on_target, shots_off_target,
            shots_blocked_by_opponent, dribbles_successful, dribbles_attempts, possession_lost,
            dispossessed, duels_won, duels_lost, aerials_won, aerials_lost, ground_duels_won,
            ground_duels_total, tackles, interceptions, clearances, shots_blocked_by_player,
            dribbled_past, fouls_committed, fouls_suffered, saves, punches_made, high_claims,
            saves_inside_box, sweeper_keeper_successful, sweeper_keeper_total
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
            $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40,
            $41, $42, $43, $44, $45, $46, $47
        )
        ON CONFLICT (match_id, player_id) DO UPDATE SET
            team_id = EXCLUDED.team_id,
            is_substitute = EXCLUDED.is_substitute,
            played_position = EXCLUDED.played_position,
            jersey_number = EXCLUDED.jersey_number,
            market_value_eur_at_match = EXCLUDED.market_value_eur_at_match,
            sofascore_rating = EXCLUDED.sofascore_rating,
            minutes_played = EXCLUDED.minutes_played,
            touches = EXCLUDED.touches,
            goals = EXCLUDED.goals, assists = EXCLUDED.assists, own_goals = EXCLUDED.own_goals,
            passes_accurate = EXCLUDED.passes_accurate, passes_total = EXCLUDED.passes_total,
            passes_key = EXCLUDED.passes_key, long_balls_accurate = EXCLUDED.long_balls_accurate,
            long_balls_total = EXCLUDED.long_balls_total, crosses_accurate = EXCLUDED.crosses_accurate,
            crosses_total = EXCLUDED.crosses_total, shots_total = EXCLUDED.shots_total,
            shots_on_target = EXCLUDED.shots_on_target, shots_off_target = EXCLUDED.shots_off_target,
            shots_blocked_by_opponent = EXCLUDED.shots_blocked_by_opponent,
            dribbles_successful = EXCLUDED.dribbles_successful, dribbles_attempts = EXCLUDED.dribbles_attempts,
            possession_lost = EXCLUDED.possession_lost, dispossessed = EXCLUDED.dispossessed,
            duels_won = EXCLUDED.duels_won, duels_lost = EXCLUDED.duels_lost,
            aerials_won = EXCLUDED.aerials_won, aerials_lost = EXCLUDED.aerials_lost,
            ground_duels_won = EXCLUDED.ground_duels_won, ground_duels_total = EXCLUDED.ground_duels_total,
            tackles = EXCLUDED.tackles, interceptions = EXCLUDED.interceptions, clearances = EXCLUDED.clearances,
            shots_blocked_by_player = EXCLUDED.shots_blocked_by_player, dribbled_past = EXCLUDED.dribbled_past,
            fouls_committed = EXCLUDED.fouls_committed, fouls_suffered = EXCLUDED.fouls_suffered,
            saves = EXCLUDED.saves, punches_made = EXCLUDED.punches_made, high_claims = EXCLUDED.high_claims,
            saves_inside_box = EXCLUDED.saves_inside_box,
            sweeper_keeper_successful = EXCLUDED.sweeper_keeper_successful,
            sweeper_keeper_total = EXCLUDED.sweeper_keeper_total;
    """
    await execute_many(sql, player_stats_list)

async def insert_team_stats_batch(team_stats_list: List[Tuple]):
    """
    Inserta un lote de estadísticas de equipos de forma asíncrona.
    La tupla debe coincidir con el orden de las columnas en SQL.
    """
    if not team_stats_list: return

    sql = """
        INSERT INTO team_match_stats (
            match_id, team_id, is_home_team, period, formation, average_team_rating,
            total_team_market_value_eur, possession_percentage, big_chances, total_shots,
            saves, corners, fouls, passes_successful, passes_total, passes_percentage,
            tackles_successful, tackles_total, tackles_won_percentage, free_kicks, yellow_cards,
            red_cards, shots_on_target, hit_woodwork, shots_off_target, blocked_shots,
            shots_inside_box, shots_outside_box, big_chances_missed, fouled_final_third,
            offsides, accurate_passes_percentage, throw_ins, final_third_entries,
            long_balls_successful, long_balls_total, long_balls_percentage, crosses_successful,
            crosses_total, crosses_percentage, duels_won_successful, duels_won_total,
            duels_won_percentage, dispossessed, ground_duels_successful, ground_duels_total,
            ground_duels_percentage, aerial_duels_successful, aerial_duels_total,
            aerial_duels_percentage, dribbles_successful, dribbles_total, dribbles_percentage,
            interceptions, clearances, goal_kicks
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
            $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40,
            $41, $42, $43, $44, $45, $46, $47, $48, $49, $50, $51, $52, $53, $54, $55, $56
        )
        ON CONFLICT (match_id, team_id, period) DO UPDATE SET
            formation = EXCLUDED.formation,
            average_team_rating = EXCLUDED.average_team_rating,
            total_team_market_value_eur = EXCLUDED.total_team_market_value_eur,
            possession_percentage = EXCLUDED.possession_percentage,
            big_chances=EXCLUDED.big_chances, total_shots=EXCLUDED.total_shots, saves=EXCLUDED.saves,
            corners=EXCLUDED.corners, fouls=EXCLUDED.fouls, passes_successful=EXCLUDED.passes_successful,
            passes_total=EXCLUDED.passes_total, passes_percentage=EXCLUDED.passes_percentage,
            tackles_successful=EXCLUDED.tackles_successful, tackles_total=EXCLUDED.tackles_total,
            tackles_won_percentage=EXCLUDED.tackles_won_percentage, free_kicks=EXCLUDED.free_kicks,
            yellow_cards=EXCLUDED.yellow_cards, red_cards=EXCLUDED.red_cards, shots_on_target=EXCLUDED.shots_on_target,
            hit_woodwork=EXCLUDED.hit_woodwork, shots_off_target=EXCLUDED.shots_off_target,
            blocked_shots=EXCLUDED.blocked_shots, shots_inside_box=EXCLUDED.shots_inside_box,
            shots_outside_box=EXCLUDED.shots_outside_box, big_chances_missed=EXCLUDED.big_chances_missed,
            fouled_final_third=EXCLUDED.fouled_final_third, offsides=EXCLUDED.offsides,
            accurate_passes_percentage=EXCLUDED.accurate_passes_percentage, throw_ins=EXCLUDED.throw_ins,
            final_third_entries=EXCLUDED.final_third_entries, long_balls_successful=EXCLUDED.long_balls_successful,
            long_balls_total=EXCLUDED.long_balls_total, long_balls_percentage=EXCLUDED.long_balls_percentage,
            crosses_successful=EXCLUDED.crosses_successful, crosses_total=EXCLUDED.crosses_total,
            crosses_percentage=EXCLUDED.crosses_percentage, duels_won_successful=EXCLUDED.duels_won_successful,
            duels_won_total=EXCLUDED.duels_won_total, duels_won_percentage=EXCLUDED.duels_won_percentage,
            dispossessed=EXCLUDED.dispossessed, ground_duels_successful=EXCLUDED.ground_duels_successful,
            ground_duels_total=EXCLUDED.ground_duels_total, ground_duels_percentage=EXCLUDED.ground_duels_percentage,
            aerial_duels_successful=EXCLUDED.aerial_duels_successful, aerial_duels_total=EXCLUDED.aerial_duels_total,
            aerial_duels_percentage=EXCLUDED.aerial_duels_percentage, dribbles_successful=EXCLUDED.dribbles_successful,
            dribbles_total=EXCLUDED.dribbles_total, dribbles_percentage=EXCLUDED.dribbles_percentage,
            interceptions=EXCLUDED.interceptions, clearances=EXCLUDED.clearances, goal_kicks=EXCLUDED.goal_kicks;
    """
    await execute_many(sql, team_stats_list)

async def update_team_match_aggregates(match_id: int, team_id: int, is_home: bool,
                                     formation: Optional[str], avg_rating: Optional[float],
                                     total_value: Optional[int]):
    """
    Actualiza la formación, rating promedio y valor total para un equipo específico
    en un partido específico para el periodo 'ALL'.
    """
    sql = """
        UPDATE team_match_stats
        SET formation = $1,
            average_team_rating = $2,
            total_team_market_value_eur = $3
        WHERE match_id = $4 AND team_id = $5 AND period = 'ALL';
    """
    params = (formation, avg_rating, total_value, match_id, team_id)
    status = await execute_query(sql, params)
    logging.debug(f"Updated team aggregates for Match {match_id}, Team {team_id} (Home: {is_home}). Status: {status}")


async def get_basic_match_details(match_id: int) -> Optional[Dict[str, Any]]:
    """Obtiene IDs de equipos y datetime de un partido de forma asíncrona."""
    sql = """
        SELECT season_id, round_number, round_name, match_datetime_utc, home_team_id, away_team_id, home_score, away_score, home_score_ht, away_score_ht
        FROM matches
        WHERE match_id = $1;
    """
    result = await execute_query(sql, (match_id,), fetch=True, many=False)
    if result:
        # asyncpg.Record se puede acceder por índice o por nombre de columna
        return {
            "season_id": result['season_id'],
            "round_number": result['round_number'],
            "round_name": result['round_name'],
            "match_datetime_utc": result['match_datetime_utc'],
            "home_team_id": result['home_team_id'],
            "away_team_id": result['away_team_id'],
            "home_score": result['home_score'],
            "away_score": result['away_score'],
            "home_score_ht": result['home_score_ht'],
            "away_score_ht": result['away_score_ht']
        }
    return None


