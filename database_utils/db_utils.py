# database.py
import psycopg2
import psycopg2.pool
import os
from dotenv import load_dotenv
import logging
from typing import List, Dict, Any, Tuple, Optional

# Configurar logging básico
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Cargar variables de entorno
load_dotenv(encoding='utf-8')

# --- Configuración del Pool de Conexiones ---
# Usar un pool es más eficiente que abrir/cerrar conexiones constantemente
try:
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASS = os.getenv("DB_PASS")
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    db_pool = psycopg2.pool.SimpleConnectionPool(
        minconn=1,
        maxconn=5,  # Ajusta según la concurrencia esperada (para scripts secuenciales, bajo es suficiente)
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT,
        client_encoding='UTF8'
    )
    logging.info("Pool de conexiones a la base de datos inicializado.")
except (Exception, psycopg2.DatabaseError) as error:
    logging.error(f"Error al inicializar el pool de conexiones: {error}")
    db_pool = None # Marcar como no disponible

def get_connection():
    """Obtiene una conexión del pool."""
    if db_pool:
        try:
            return db_pool.getconn()
        except Exception as e:
            logging.error(f"Error al obtener conexión del pool: {e}")
            return None
    else:
        logging.error("El pool de conexiones no está disponible.")
        return None

def release_connection(conn):
    """Devuelve una conexión al pool."""
    if db_pool and conn:
        try:
            db_pool.putconn(conn)
        except Exception as e:
            logging.error(f"Error al devolver conexión al pool: {e}")

def execute_query(sql: str, params: Optional[Tuple] = None, fetch: bool = False, many: bool = False):
    """
    Ejecuta una consulta SQL de forma segura usando el pool.
    Maneja la conexión y el cursor automáticamente.

    Args:
        sql (str): La consulta SQL parametrizada (usando %s).
        params (tuple, optional): Tupla de parámetros para la consulta. Defaults to None.
        fetch (bool): Si True, devuelve resultados (uno o todos según 'many'). Defaults to False.
        many (bool): Si True y fetch=True, devuelve todos los resultados (fetchall). Si False, devuelve uno (fetchone).

    Returns:
        Optional[List[Tuple]] or Optional[Tuple] or None: Resultados si fetch=True, None en otro caso o en error.
    """
    conn = None
    result = None
    try:
        conn = get_connection()
        if conn:
            # Autocommit=True simplifica para inserciones/actualizaciones individuales,
            # pero para transacciones más largas, mejor manejar commit/rollback explícitamente.
            # Para este script, podríamos desactivarlo y hacer commit al final de cada lote/partido.
            # Vamos a mantenerlo simple por ahora y commitear tras cada operación exitosa implícitamente.
            # conn.autocommit = True # Opción 1: Simple pero menos control transaccional
            with conn.cursor() as cursor:
                cursor.execute(sql, params or ())
                if fetch:
                    result = cursor.fetchall() if many else cursor.fetchone()
            conn.commit() # Opción 2: Commit explícito después de la operación
            logging.debug(f"Ejecutada SQL: {sql[:100]}... con params: {params}")
        else:
            logging.error("No se pudo obtener conexión para ejecutar la consulta.")

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Error ejecutando SQL: {sql[:100]}... Error: {error}")
        if conn:
            try:
                conn.rollback() # Revertir en caso de error
            except Exception as rb_error:
                 logging.error(f"Error durante rollback: {rb_error}")
        result = None # Asegurar que no se devuelvan resultados parciales en error
    finally:
        if conn:
            release_connection(conn)
    return result

def execute_batch(sql: str, data_list: List[Tuple]):
    """
    Ejecuta una consulta SQL para múltiples filas de datos (INSERT/UPDATE).

    Args:
        sql (str): La consulta SQL parametrizada (usando %s).
        data_list (List[Tuple]): Lista de tuplas, cada tupla son los parámetros para una fila.
    """
    conn = None
    if not data_list:
        logging.warning("execute_batch llamado con lista de datos vacía.")
        return False

    try:
        conn = get_connection()
        if conn:
            with conn.cursor() as cursor:
                # psycopg2.extras.execute_batch es más eficiente para lotes grandes
                # from psycopg2.extras import execute_batch
                # execute_batch(cursor, sql, data_list) # Usar si es muy grande

                # Para lotes moderados, executemany está bien
                cursor.executemany(sql, data_list)

            conn.commit() # Commit después de ejecutar todo el lote
            logging.info(f"Ejecutado lote SQL ({len(data_list)} filas): {sql[:100]}...")
            return True
        else:
            logging.error("No se pudo obtener conexión para ejecutar lote.")
            return False

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Error ejecutando lote SQL: {sql[:100]}... Error: {error}")
        if conn:
            try:
                conn.rollback()
            except Exception as rb_error:
                 logging.error(f"Error durante rollback de lote: {rb_error}")
        return False
    finally:
        if conn:
            release_connection(conn)

# --- Funciones específicas de Inserción/Actualización ---

# Tablas dimensionales (usar ON CONFLICT)
def upsert_tournament(tournament_id: int, name: str, country: Optional[str]):
    sql = """
        INSERT INTO tournaments (tournament_id, name, country_name)
        VALUES (%s, %s, %s)
        ON CONFLICT (tournament_id) DO NOTHING;
    """
    execute_query(sql, (tournament_id, name, country))

def upsert_season(season_id: int, tournament_id: int, name: str):
    sql = """
        INSERT INTO seasons (season_id, tournament_id, name)
        VALUES (%s, %s, %s)
        ON CONFLICT (season_id) DO NOTHING;
    """
    execute_query(sql, (season_id, tournament_id, name))

def upsert_team(team_id: int, name: str, country: Optional[str]):
    sql = """
        INSERT INTO teams (team_id, name, country)
        VALUES (%s, %s, %s)
        ON CONFLICT (team_id) DO UPDATE SET
            name = EXCLUDED.name,
            country = EXCLUDED.country;
            -- O DO NOTHING si prefieres no actualizar
    """
    execute_query(sql, (team_id, name, country))

def upsert_player(player_id: int, name: str, height: Optional[int], position: Optional[str], country: Optional[str]):
    sql = """
        INSERT INTO players (player_id, name, height_cm, primary_position, country_name)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (player_id) DO UPDATE SET
            name = EXCLUDED.name,
            height_cm = EXCLUDED.height_cm,
            primary_position = EXCLUDED.primary_position,
            country_name = EXCLUDED.country_name;
            -- O DO NOTHING si prefieres no actualizar
    """
    execute_query(sql, (player_id, name, height, position, country))

# Tabla de Partidos (usar ON CONFLICT)
def upsert_match(match_id: int, season_id: int, round_num: Optional[int], dt_utc: Any,
                 home_id: int, away_id: int, home_score: Optional[int] = None,
                 away_score: Optional[int] = None, ht_home: Optional[int] = None,
                 ht_away: Optional[int] = None):
    sql = """
        INSERT INTO matches (match_id, season_id, round_number, match_datetime_utc,
                             home_team_id, away_team_id, home_score, away_score,
                             home_score_ht, away_score_ht)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (match_id) DO UPDATE SET
            season_id = EXCLUDED.season_id,
            round_number = EXCLUDED.round_number,
            match_datetime_utc = EXCLUDED.match_datetime_utc,
            home_team_id = EXCLUDED.home_team_id,
            away_team_id = EXCLUDED.away_team_id,
            home_score = EXCLUDED.home_score,
            away_score = EXCLUDED.away_score,
            home_score_ht = EXCLUDED.home_score_ht,
            away_score_ht = EXCLUDED.away_score_ht;
            -- O DO NOTHING si solo quieres insertar una vez
    """
    params = (match_id, season_id, round_num, dt_utc, home_id, away_id,
              home_score, away_score, ht_home, ht_away)
    execute_query(sql, params)

# Funciones para insertar estadísticas (usar batch)
def insert_player_stats_batch(player_stats_list: List[Tuple]):
    """
    Inserta un lote de estadísticas de jugadores.
    La tupla debe coincidir con el orden de las columnas en SQL.
    """
    if not player_stats_list: return

    # Asegúrate de que el número de %s coincida con los campos de la tupla
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
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s
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
            -- O DO NOTHING si no quieres actualizar si ya existe
    """
    execute_batch(sql, player_stats_list)

def insert_team_stats_batch(team_stats_list: List[Tuple]):
    """
    Inserta un lote de estadísticas de equipos.
    La tupla debe coincidir con el orden de las columnas en SQL.
    """
    if not team_stats_list: return

    # Asegúrate de que el número de %s coincida con los campos de la tupla
    # Es una consulta larga, ¡verifica el orden con cuidado!
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
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (match_id, team_id, period) DO UPDATE SET
            formation = EXCLUDED.formation,
            average_team_rating = EXCLUDED.average_team_rating,
            total_team_market_value_eur = EXCLUDED.total_team_market_value_eur,
            possession_percentage = EXCLUDED.possession_percentage,
            -- ... (actualiza todas las demás columnas igual que en player_stats)...
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
            -- O DO NOTHING si no quieres actualizar
    """
    execute_batch(sql, team_stats_list)

def update_team_match_aggregates(match_id: int, home_rating: Optional[float], away_rating: Optional[float],
                                 home_value: Optional[int], away_value: Optional[int]):
    """Actualiza el rating y valor promedio del equipo en team_match_stats para un partido."""
    # Actualizar equipo local
    sql_home = """
        UPDATE team_match_stats
        SET average_team_rating = %s,
            total_team_market_value_eur = %s
        WHERE match_id = %s AND is_home_team = TRUE AND period = 'ALL';
    """
    execute_query(sql_home, (home_rating, home_value, match_id))

    # Actualizar equipo visitante
    sql_away = """
        UPDATE team_match_stats
        SET average_team_rating = %s,
            total_team_market_value_eur = %s
        WHERE match_id = %s AND is_home_team = FALSE AND period = 'ALL';
    """
    execute_query(sql_away, (away_rating, away_value, match_id))

# --- Funciones Adicionales (Ejemplo: Obtener detalles básicos del partido) ---
def get_basic_match_details(match_id: int) -> Optional[Dict[str, Any]]:
    """Obtiene IDs de equipos y datetime de un partido."""
    sql = """
        SELECT season_id, round_number, match_datetime_utc, home_team_id, away_team_id
        FROM matches
        WHERE match_id = %s;
    """
    result = execute_query(sql, (match_id,), fetch=True, many=False)
    if result:
        return {
            "season_id": result[0],
            "round_number": result[1],
            "match_datetime_utc": result[2],
            "home_team_id": result[3],
            "away_team_id": result[4]
        }
    return None

# --- Cierre del Pool ---
def close_pool():
    """Cierra todas las conexiones en el pool."""
    if db_pool:
        try:
            db_pool.closeall()
            logging.info("Pool de conexiones cerrado.")
        except Exception as e:
            logging.error(f"Error cerrando el pool de conexiones: {e}")