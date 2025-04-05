# extractors/players_statistics_extractor.py
import asyncio
import json
import random
from playwright.async_api import async_playwright
import traceback

# User agents (debe ser consistente con los otros módulos o importado)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
]

_BASE_SOFASCORE_URL = "https://www.sofascore.com/"

# Mapeo de claves JSON de SofaScore a nombres de estadísticas deseadas
# Esto ayuda a manejar nombres inconsistentes o complejos en la API
SOFASCORE_PLAYER_STATS_MAP = {
    # Información básica (ya está en player_info)
    'name': 'Name', 'shortName': 'Short Name', 'position': 'Position', 'jerseyNumber': 'Jersey Number', 'height': 'Height',
    'country': 'Country',
    'rating': 'Rating',
    'minutesPlayed': 'Minutes played',
    'touches': 'Touches',
    'accuratePass': 'accuratePass', # Parte de 'Accurate passes'
    'totalPass': 'totalPass',       # Parte de 'Accurate passes'
    'keyPass': 'Key passes',
    'goalAssist': 'Assists',
    'goals': 'Goals',
    'expectedGoals': 'Expected goals', # Verificar si existe esta clave exacta
    'totalShoot': 'Shots', # Verificar si es 'totalShoot' o 'totalShots'
    'onTargetScoringAttempt': 'Shots on target',
    'dribbleWon': 'dribbleWon', # Parte de 'Dribbles completed'
    'dribbleAttempt': 'dribbleAttempt', # Parte de 'Dribbles completed'
    'duelWon': 'duelWon',             # Parte de 'Duels won' / 'Ground duels' / 'Aerial duels'
    'duelLost': 'duelLost',           # Parte de 'Duels won' / 'Ground duels' / 'Aerial duels'
    'aerialWon': 'Aerial duels won',
    'saves': 'Saves',
    'punches': 'Punches',
    'keeperSweeperWon': 'keeperSweeperWon',     # Parte de 'Runs out (succ.)'
    'totalKeeperSweeper': 'totalKeeperSweeper', # Parte de 'Runs out (succ.)'
    'goodHighClaim': 'High claims',
    'savedShotsFromInsideTheBox': 'Saves from inside box',
    'accurateCross': 'accurateCross',     # Parte de 'Crosses (acc.)'
    'totalCross': 'totalCross',         # Parte de 'Crosses (acc.)'
    'accurateLongBalls': 'accurateLongBalls', # Parte de 'Long balls (acc.)'
    'totalLongBalls': 'totalLongBalls',     # Parte de 'Long balls (acc.)'
    'totalClearance': 'Clearances',
    'outfielderBlock': 'Blocked shots', # Verificar si es este o 'blockedScoringAttempt'
    'interceptionWon': 'Interceptions',
    'totalTackle': 'Total tackles',
    'challengeLost': 'Dribbled past', # A menudo 'challengeLost' significa que fue regateado
    'groundDuelWon': 'groundDuelWon', # Ya cubierto por duelWon/Lost?
    'groundDuelTotal': 'groundDuelTotal',
    'aerialDuelWon': 'aerialDuelWon', # Ya cubierto por aerialWon/Lost?
    'aerialDuelTotal': 'aerialDuelTotal',
    'fouls': 'Fouls',
    'wasFouled': 'Was fouled',
    'blockedScoringAttempt': 'Shots blocked', # Podría ser este para 'Shots blocked' por defensores
    'possessionLostCtrl': 'Possession lost', # No en tu lista, pero útil
}

# Campos deseados y sus valores por defecto
DEFAULT_PLAYER_STATS = {
    'proposedMarketValue': 0, 'Rating': 'N/A', 'Minutes played': '0', 'Touches': '0',
    'Accurate passes': '0/0 (0%)', 'Passes accuracy': '0%', 'Key passes': '0',
    'Assists': '0', 'Goals': '0', 'Expected goals': '0.00', # Usar float?
    'Shots': '0', 'Shots on target': '0',
    'Dribbles completed': '0/0 (0%)', 'Duels won': '0/0 (0%)', 'Aerial duels won': '0/0 (0%)',
    'Saves': '0', 'Punches': '0', 'Runs out (succ.)': '0/0 (0%)', 'High claims': '0',
    'Saves from inside box': '0', 'Crosses (acc.)': '0/0 (0%)', 'Long balls (acc.)': '0/0 (0%)',
    'Clearances': '0', 'Blocked shots': '0', 'Interceptions': '0', 'Total tackles': '0',
    'Dribbled past': '0', 'Ground duels (won)': '0/0 (0%)', # Calculado
    # 'Aerial duels (won)': '0/0 (0%)', # Repetido? Usar el de arriba
    'Fouls': '0', 'Was fouled': '0',
    # 'Shots blocked': '0', # Repetido? Usar el de arriba
    'Dribble attempts (succ.)': '0/0 (0%)' # Repetido? Usar Dribbles completed
}
# Simplificar la lista a las claves finales deseadas
FINAL_STATS_KEYS = list(DEFAULT_PLAYER_STATS.keys())


def _calculate_percentage(numerator, denominator):
    """Calcula porcentaje de forma segura, devuelve 0 si el denominador es 0."""
    if denominator == 0:
        return 0
    try:
        return round((numerator / denominator) * 100)
    except TypeError: # Si alguno no es número
        return 0

def _format_stat_with_total_percentage(accurate_key, total_key, player_stats_raw):
    """Formatea estadísticas como 'Acc/Total (Perc%)'."""
    accurate = player_stats_raw.get(accurate_key, 0)
    total = player_stats_raw.get(total_key, 0)
    # Asegurarse de que sean números
    try: accurate = int(accurate)
    except (ValueError, TypeError): accurate = 0
    try: total = int(total)
    except (ValueError, TypeError): total = 0

    percentage = _calculate_percentage(accurate, total)
    return f"{accurate}/{total} ({percentage}%)"

def _convert_stats_to_numeric(player_data):
    """
    Convierte las estadísticas de texto a formato numérico.
    - Convierte fracciones "X/Y (Z%)" a número flotante X/Y
    - Convierte porcentajes "X%" a número flotante X/100
    - Mantiene los valores ya numéricos
    - Ignora valores no convertibles (como 'N/A')
    
    Args:
        player_data (dict): Diccionario con datos de jugador
    
    Returns:
        dict: Diccionario con valores convertidos a numéricos
    """
    numeric_data = {}
    
    for key, value in player_data.items():
        # Conservar llaves no numéricas sin cambios
        if key in ['player_id', 'name', 'short_name', 'position', 'jersey_number', 'is_substitute']:
            numeric_data[key] = value
            continue
            
        # Si es un string vacío o N/A, usar 0
        if not value or value == 'N/A':
            numeric_data[key] = 0
            continue
        
        # Convertir strings a numéricos
        if isinstance(value, str):
            # Caso 1: "X/Y (Z%)" -> convertir a X/Y como float
            if '/' in value and '(' in value:
                try:
                    numerator = float(value.split('/')[0])
                    denominator = float(value.split('/')[1].split('(')[0])
                    if denominator == 0:  # Evitar división por cero
                        numeric_data[key] = 0
                    else:
                        numeric_data[key] = numerator / denominator
                except (ValueError, IndexError):
                    numeric_data[key] = 0
            
            # Caso 2: "X%" -> convertir a X/100 como float
            elif value.endswith('%'):
                try:
                    numeric_data[key] = float(value.rstrip('%')) / 100
                except ValueError:
                    numeric_data[key] = 0
            
            # Caso 3: Intentar convertir directamente a float
            else:
                try:
                    numeric_data[key] = float(value)
                except ValueError:
                    numeric_data[key] = value  # Mantener el valor original si no se puede convertir
        else:
            # Ya es un tipo no string (int, float, etc.)
            numeric_data[key] = value
            
    return numeric_data



def _parse_player_lineup_data(lineup_data):
    """
    Parsea la respuesta JSON de la API /lineups para extraer formación,
    rating de equipo y estadísticas detalladas de jugadores. CORREGIDO para manejar tipos.
    """
    parsed_data = {
        "home_team_info": {"formation": None, "sofascore_rating": None, "players": []},
        "away_team_info": {"formation": None, "sofascore_rating": None, "players": []}
    }

    if not lineup_data or not isinstance(lineup_data, dict):
        print("    Advertencia: Datos de alineación inválidos o vacíos.")
        return parsed_data # Devuelve estructura vacía

    # Extraer info de equipo local
    home_data = lineup_data.get("home", {})
    parsed_data["home_team_info"]["formation"] = home_data.get("formation")
    home_player_ratings = []

    # Extraer info de equipo visitante
    away_data = lineup_data.get("away", {})
    parsed_data["away_team_info"]["formation"] = away_data.get("formation")
    away_player_ratings = []

    # --- Procesar jugadores LOCALES ---
    for player_entry in home_data.get("players", []):
        player_info = player_entry.get("player", {})
        stats_raw = player_entry.get("statistics", {}) # Datos directos de la API para este jugador

        if not stats_raw:
            continue

        player_stats = DEFAULT_PLAYER_STATS.copy() # Empezar con defaults (que son strings formateados)

        # Extraer datos básicos del jugador
        player_stats['player_id'] = player_info.get("id")
        player_stats['name'] = player_info.get("name")
        player_stats['short_name'] = player_info.get("shortName")
        player_stats['position'] = player_entry.get("position", player_info.get("position")) # Posición en el partido
        player_stats['jersey_number'] = player_entry.get("jerseyNumber")
        player_stats['is_substitute'] = player_entry.get("substitute", False) # Si entró desde el banquillo

        # Extraer market value
        market_value_raw = player_info.get("proposedMarketValueRaw", {})
        player_stats['proposedMarketValue'] = market_value_raw.get("value", 0)

        # Mapear estadísticas directas y guardar crudas para cálculos
        temp_raw_stats = {} # Diccionario temporal para guardar valores numéricos crudos para cálculo
        for sofascore_key, target_key_or_component in SOFASCORE_PLAYER_STATS_MAP.items():
            if sofascore_key in stats_raw:
                raw_value = stats_raw[sofascore_key]
                # Mapeos directos a las claves finales (formato string por defecto)
                if target_key_or_component in player_stats:
                    player_stats[target_key_or_component] = str(raw_value) if raw_value is not None else player_stats[target_key_or_component]
                # Guardamos el valor crudo en el temporal si es un componente para cálculo posterior
                if isinstance(target_key_or_component, str) and (
                    target_key_or_component.endswith('Pass') or
                    target_key_or_component.endswith('Won') or
                    target_key_or_component.endswith('Lost') or
                    target_key_or_component.endswith('Attempt') or
                    target_key_or_component.endswith('Sweeper') or
                    target_key_or_component == 'Aerial duels won' # Caso especial para el cálculo
                ):
                   temp_raw_stats[target_key_or_component] = raw_value


        # Calcular estadísticas compuestas (pases, etc.) - Crean strings formateados
        player_stats['Accurate passes'] = _format_stat_with_total_percentage('accuratePass', 'totalPass', temp_raw_stats)
        # Calcular accuracy % numérico para posible uso futuro (no se usa en formato final)
        acc_pass_num = 0
        tot_pass_num = 0
        try: acc_pass_num = int(temp_raw_stats.get('accuratePass', 0))
        except (ValueError, TypeError): pass
        try: tot_pass_num = int(temp_raw_stats.get('totalPass', 0))
        except (ValueError, TypeError): pass
        player_stats['Passes accuracy'] = f"{_calculate_percentage(acc_pass_num, tot_pass_num)}%"

        player_stats['Dribbles completed'] = _format_stat_with_total_percentage('dribbleWon', 'dribbleAttempt', temp_raw_stats)
        player_stats['Dribble attempts (succ.)'] = player_stats['Dribbles completed'] # Son lo mismo
        player_stats['Long balls (acc.)'] = _format_stat_with_total_percentage('accurateLongBalls', 'totalLongBalls', temp_raw_stats)
        player_stats['Crosses (acc.)'] = _format_stat_with_total_percentage('accurateCross', 'totalCross', temp_raw_stats)
        player_stats['Runs out (succ.)'] = _format_stat_with_total_percentage('keeperSweeperWon', 'totalKeeperSweeper', temp_raw_stats)

        # --- INICIO BLOQUE DE CÁLCULO DE DUELOS CORREGIDO ---
        # 1. Obtener valores NUMÉRICOS crudos de stats_raw o temp_raw_stats para CÁLCULOS
        duel_won_num = 0
        try: duel_won_num = int(stats_raw.get('duelWon', 0))
        except (ValueError, TypeError): pass

        duel_lost_num = 0
        try: duel_lost_num = int(stats_raw.get('duelLost', 0))
        except (ValueError, TypeError): pass

        aerial_won_num = 0
        try: aerial_won_num = int(stats_raw.get('aerialWon', 0)) # Usa la clave real de SofaScore
        except (ValueError, TypeError): pass

        aerial_lost_num = 0
        try: aerial_lost_num = int(stats_raw.get('aerialLost', 0)) # Verifica que 'aerialLost' exista en la API
        except (ValueError, TypeError): pass

        ground_won_direct_num = None
        try:
             val = stats_raw.get('groundDuelWon')
             if val is not None: ground_won_direct_num = int(val)
        except (ValueError, TypeError): pass

        ground_total_direct_num = None
        try:
             val = stats_raw.get('groundDuelTotal')
             if val is not None: ground_total_direct_num = int(val)
        except (ValueError, TypeError): pass

        # 2. Calcular totales NUMÉRICOS
        duel_total_num = duel_won_num + duel_lost_num
        aerial_total_num = aerial_won_num + aerial_lost_num

        # 3. Crear los STRINGS formateados y asignarlos a player_stats
        player_stats['Duels won'] = f"{duel_won_num}/{duel_total_num} ({_calculate_percentage(duel_won_num, duel_total_num)}%)"
        player_stats['Aerial duels won'] = f"{aerial_won_num}/{aerial_total_num} ({_calculate_percentage(aerial_won_num, aerial_total_num)}%)"

        # 4. Calcular duelos terrestres y formatear el string
        if ground_won_direct_num is not None and ground_total_direct_num is not None:
            player_stats['Ground duels (won)'] = f"{ground_won_direct_num}/{ground_total_direct_num} ({_calculate_percentage(ground_won_direct_num, ground_total_direct_num)}%)"
        else:
            # Calcular como fallback usando los números
            ground_won_calc = duel_won_num - aerial_won_num
            ground_total_calc = duel_total_num - aerial_total_num # Usa los totales numéricos

            if ground_total_calc >= ground_won_calc and ground_total_calc >= 0: # Sanity check
                player_stats['Ground duels (won)'] = f"{ground_won_calc}/{ground_total_calc} ({_calculate_percentage(ground_won_calc, ground_total_calc)}%)"
            else:
                player_stats['Ground duels (won)'] = f"?/{ground_total_calc} (?%)" # Placeholder si es inconsistente
        # --- FIN BLOQUE DE CÁLCULO DE DUELOS CORREGIDO ---


        # Limpiar claves finales según FINAL_STATS_KEYS y añadir info básica
        final_player_data = {key: player_stats[key] for key in FINAL_STATS_KEYS if key in player_stats}
        final_player_data['player_id'] = player_stats.get('player_id')
        final_player_data['name'] = player_stats.get('name')
        final_player_data['short_name'] = player_stats.get('short_name')
        final_player_data['position'] = player_stats.get('position')
        final_player_data['jersey_number'] = player_stats.get('jersey_number')
        final_player_data['is_substitute'] = player_stats.get('is_substitute')
        # Asegurarse que el market value esté, aunque DEFAULT_PLAYER_STATS lo incluye
        final_player_data['proposedMarketValue'] = player_stats.get('proposedMarketValue', 0)

        # Convertir estadísticas a formato numérico (según la función proporcionada por el usuario)
        final_player_data = _convert_stats_to_numeric(final_player_data)

        parsed_data["home_team_info"]["players"].append(final_player_data)

        # Añadir rating para el cálculo promedio (intenta convertir desde el formato numérico final)
        rating_value = final_player_data.get('Rating') # Ya debería ser numérico o 0/None tras _convert_stats_to_numeric
        if isinstance(rating_value, (int, float)) and rating_value > 0: # Evitar 0 o None/N/A
             home_player_ratings.append(float(rating_value))


    # --- Procesar jugadores VISITANTES (Lógica idéntica) ---
    for player_entry in away_data.get("players", []):
        player_info = player_entry.get("player", {})
        stats_raw = player_entry.get("statistics", {})
        if not stats_raw: continue

        player_stats = DEFAULT_PLAYER_STATS.copy()
        player_stats['player_id'] = player_info.get("id")
        player_stats['name'] = player_info.get("name")
        player_stats['short_name'] = player_info.get("shortName")
        player_stats['position'] = player_entry.get("position", player_info.get("position"))
        player_stats['jersey_number'] = player_entry.get("jerseyNumber")
        player_stats['is_substitute'] = player_entry.get("substitute", False)
        market_value_raw = player_info.get("proposedMarketValueRaw", {})
        player_stats['proposedMarketValue'] = market_value_raw.get("value", 0)

        temp_raw_stats = {}
        for sofascore_key, target_key_or_component in SOFASCORE_PLAYER_STATS_MAP.items():
            if sofascore_key in stats_raw:
                raw_value = stats_raw[sofascore_key]
                if target_key_or_component in player_stats:
                    player_stats[target_key_or_component] = str(raw_value) if raw_value is not None else player_stats[target_key_or_component]
                if isinstance(target_key_or_component, str) and (
                    target_key_or_component.endswith('Pass') or
                    target_key_or_component.endswith('Won') or
                    target_key_or_component.endswith('Lost') or
                    target_key_or_component.endswith('Attempt') or
                    target_key_or_component.endswith('Sweeper') or
                    target_key_or_component == 'Aerial duels won'
                ):
                   temp_raw_stats[target_key_or_component] = raw_value

        player_stats['Accurate passes'] = _format_stat_with_total_percentage('accuratePass', 'totalPass', temp_raw_stats)
        acc_pass_num = 0
        tot_pass_num = 0
        try: acc_pass_num = int(temp_raw_stats.get('accuratePass', 0))
        except (ValueError, TypeError): pass
        try: tot_pass_num = int(temp_raw_stats.get('totalPass', 0))
        except (ValueError, TypeError): pass
        player_stats['Passes accuracy'] = f"{_calculate_percentage(acc_pass_num, tot_pass_num)}%"

        player_stats['Dribbles completed'] = _format_stat_with_total_percentage('dribbleWon', 'dribbleAttempt', temp_raw_stats)
        player_stats['Dribble attempts (succ.)'] = player_stats['Dribbles completed']
        player_stats['Long balls (acc.)'] = _format_stat_with_total_percentage('accurateLongBalls', 'totalLongBalls', temp_raw_stats)
        player_stats['Crosses (acc.)'] = _format_stat_with_total_percentage('accurateCross', 'totalCross', temp_raw_stats)
        player_stats['Runs out (succ.)'] = _format_stat_with_total_percentage('keeperSweeperWon', 'totalKeeperSweeper', temp_raw_stats)

        # --- INICIO BLOQUE DE CÁLCULO DE DUELOS CORREGIDO (VISITANTE) ---
        duel_won_num = 0
        try: duel_won_num = int(stats_raw.get('duelWon', 0))
        except (ValueError, TypeError): pass
        duel_lost_num = 0
        try: duel_lost_num = int(stats_raw.get('duelLost', 0))
        except (ValueError, TypeError): pass
        aerial_won_num = 0
        try: aerial_won_num = int(stats_raw.get('aerialWon', 0))
        except (ValueError, TypeError): pass
        aerial_lost_num = 0
        try: aerial_lost_num = int(stats_raw.get('aerialLost', 0))
        except (ValueError, TypeError): pass
        ground_won_direct_num = None
        try:
             val = stats_raw.get('groundDuelWon')
             if val is not None: ground_won_direct_num = int(val)
        except (ValueError, TypeError): pass
        ground_total_direct_num = None
        try:
             val = stats_raw.get('groundDuelTotal')
             if val is not None: ground_total_direct_num = int(val)
        except (ValueError, TypeError): pass

        duel_total_num = duel_won_num + duel_lost_num
        aerial_total_num = aerial_won_num + aerial_lost_num

        player_stats['Duels won'] = f"{duel_won_num}/{duel_total_num} ({_calculate_percentage(duel_won_num, duel_total_num)}%)"
        player_stats['Aerial duels won'] = f"{aerial_won_num}/{aerial_total_num} ({_calculate_percentage(aerial_won_num, aerial_total_num)}%)"

        if ground_won_direct_num is not None and ground_total_direct_num is not None:
            player_stats['Ground duels (won)'] = f"{ground_won_direct_num}/{ground_total_direct_num} ({_calculate_percentage(ground_won_direct_num, ground_total_direct_num)}%)"
        else:
            ground_won_calc = duel_won_num - aerial_won_num
            ground_total_calc = duel_total_num - aerial_total_num
            if ground_total_calc >= ground_won_calc and ground_total_calc >= 0:
                player_stats['Ground duels (won)'] = f"{ground_won_calc}/{ground_total_calc} ({_calculate_percentage(ground_won_calc, ground_total_calc)}%)"
            else:
                player_stats['Ground duels (won)'] = f"?/{ground_total_calc} (?%)"
        # --- FIN BLOQUE DE CÁLCULO DE DUELOS CORREGIDO (VISITANTE) ---

        final_player_data = {key: player_stats[key] for key in FINAL_STATS_KEYS if key in player_stats}
        final_player_data['player_id'] = player_stats.get('player_id')
        final_player_data['name'] = player_stats.get('name')
        final_player_data['short_name'] = player_stats.get('short_name')
        final_player_data['position'] = player_stats.get('position')
        final_player_data['jersey_number'] = player_stats.get('jersey_number')
        final_player_data['is_substitute'] = player_stats.get('is_substitute')
        final_player_data['proposedMarketValue'] = player_stats.get('proposedMarketValue', 0)

        final_player_data = _convert_stats_to_numeric(final_player_data)
        parsed_data["away_team_info"]["players"].append(final_player_data)

        rating_value = final_player_data.get('Rating')
        if isinstance(rating_value, (int, float)) and rating_value > 0:
            away_player_ratings.append(float(rating_value))


    # Calcular rating promedio del equipo si hay jugadores con rating
    if home_player_ratings:
        parsed_data["home_team_info"]["sofascore_rating"] = round(sum(home_player_ratings) / len(home_player_ratings), 2)
    else:
        parsed_data["home_team_info"]["sofascore_rating"] = None # O 0.0 para evitar None

    if away_player_ratings:
        parsed_data["away_team_info"]["sofascore_rating"] = round(sum(away_player_ratings) / len(away_player_ratings), 2)
    else:
        parsed_data["away_team_info"]["sofascore_rating"] = None # O 0.0 para evitar None

    return parsed_data

async def _fetch_lineup_data_pw(page, match_id):
    """Obtiene datos de la API /lineups usando Playwright."""
    lineup_api_url = f"https://www.sofascore.com/api/v1/event/{match_id}/lineups"
    event_page_url = f"https://www.sofascore.com/event/{match_id}" # Para contexto

    print(f"    Intentando fetch de alineaciones/jugadores para Match ID: {match_id} (API: {lineup_api_url})")
    try:
        try:
            await page.goto(event_page_url, wait_until="domcontentloaded", timeout=45000)
            await asyncio.sleep(random.uniform(1.0, 2.5))
        except Exception as page_load_err:
             print(f"    Advertencia: No se pudo cargar la página del evento {match_id}: {type(page_load_err).__name__}. Intentando fetch directo...")
        response = await page.goto(lineup_api_url, wait_until="commit", timeout=30000)
        if response is None:
            print("    -> Error: page.goto a API /lineups devolvió None.")
            return None
        if response.status == 200:
            content = await response.text()
            if content.strip().startswith("{") and content.strip().endswith("}"):
                 try:
                     lineup_object = json.loads(content)
                     # Devolvemos siempre, aunque no esté confirmado, para que el parser decida
                     return lineup_object
                 except json.JSONDecodeError as json_err:
                     print(f"    -> Error: No se pudo decodificar el JSON de /lineups. Error: {json_err}. Contenido: {content[:300]}...")
                     return None
            else:
                print(f"    -> Error: La respuesta 200 de /lineups no parece ser un objeto JSON válido. Contenido: {content[:200]}...")
                return None
        else:
            body = await response.text()
            print(f"    -> Error en fetch de API /lineups: {response.status}")
            if response.status == 403: raise PermissionError(f"403 Forbidden para {lineup_api_url}")
            elif response.status == 404: return {"error": 404, "message": "Lineups not found"}
            else: raise ConnectionError(f"Error {response.status} para {lineup_api_url}")
    except PermissionError as pe:
         return {"error": 403, "message": "Forbidden"}
    except Exception as e:
        print(f"    -> Error inesperado durante fetch de /lineups para Match ID {match_id}: {type(e).__name__}")
        # traceback.print_exc()
        return {"error": 500, "message": f"Unexpected error: {type(e).__name__}"}


async def extract_player_stats_for_match_ids(match_ids_list):
    """
    Recibe una LISTA de IDs de partidos y extrae formación, rating de equipo
    y estadísticas detalladas de jugadores usando la API /lineups.

    Args:
        match_ids_list (list): Una lista plana de IDs de partidos.

    Returns:
        list: Una lista de diccionarios, donde cada diccionario contiene
              'match_id' y 'lineup_data' (con formación, rating y jugadores).
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

        async def setup_browser_context(existing_browser=None):
             # (Misma función setup_browser_context)
             nonlocal browser, context, page
             if existing_browser:
                 print("    Reiniciando contexto del navegador (jugadores)...")
                 await existing_browser.close()
             new_browser = await p.chromium.launch(headless=True) # Poner True para producción
             new_context = await new_browser.new_context(user_agent=random.choice(USER_AGENTS), viewport={"width": 1366, "height": 768})
             await new_context.add_init_script("Object.defineProperty(navigator, 'webdriver', { get: () => undefined });")
             new_page = await new_context.new_page()
             try:
                 await new_page.goto(_BASE_SOFASCORE_URL, wait_until="domcontentloaded", timeout=30000)
                 await asyncio.sleep(random.uniform(0.5, 1.5))
             except Exception as init_err: print(f"    Advertencia: Falló visita principal (jugadores): {init_err}")
             return new_browser, new_context, new_page

        browser, context, page = await setup_browser_context()

        for i, match_id in enumerate(match_ids_list):
            print(f"  Procesando Alineaciones Partido {i+1}/{total_ids_to_process} (ID: {match_id})")
            await asyncio.sleep(random.uniform(2.5, 5.5)) # Pausa entre partidos

            lineup_raw_data = await _fetch_lineup_data_pw(page, match_id)

            # --- INICIO: Completar el manejo de errores ---
            if isinstance(lineup_raw_data, dict) and lineup_raw_data.get("error") == 403:
                print(f"    -> Error 403 obteniendo alineaciones para {match_id}. Intentando recuperación...")
                failed_match_ids.append(match_id)
                await asyncio.sleep(random.uniform(15, 25))
                browser, context, page = await setup_browser_context(browser)
                continue # Pasar al siguiente ID
            elif isinstance(lineup_raw_data, dict) and lineup_raw_data.get("error") == 404:
                print(f"    -> Alineaciones para {match_id} no encontradas (404). Saltando.")
                failed_match_ids.append(match_id)
                continue
            elif lineup_raw_data is None or (isinstance(lineup_raw_data, dict) and lineup_raw_data.get("error")):
                 # Captura None o cualquier diccionario con una clave "error" (incluyendo el 500 genérico)
                 error_msg = lineup_raw_data.get('message', 'desconocido') if isinstance(lineup_raw_data, dict) else 'fetch devolvió None'
                 print(f"    -> Falló la obtención de alineaciones para {match_id} (error: {error_msg}). Saltando.")
                 failed_match_ids.append(match_id)
                 # Podríamos implementar recuperación si fallan muchos
                 await asyncio.sleep(random.uniform(2, 4)) # Pequeña pausa adicional
                 continue
            # --- FIN: Completar el manejo de errores ---

            # Si llegamos aquí, lineup_raw_data es un diccionario válido con los datos
            try:
                # Parsear los datos crudos
                parsed_lineup_info = _parse_player_lineup_data(lineup_raw_data)

                # Añadir al resultado final
                all_matches_lineups.append({
                    "match_id": match_id,
                    "lineup_data": parsed_lineup_info # Contiene home_team_info y away_team_info
                })
                # print(f"    -> Alineaciones/Jugadores procesados para Match ID: {match_id}") # Opcional

            except Exception as parse_err:
                print(f"    -> Error FATAL parseando datos de alineación para Match ID {match_id}: {parse_err}")
                traceback.print_exc() # Imprimir stack trace para errores de parseo
                failed_match_ids.append(match_id)

        # --- Fin del bucle ---
        if browser:
            await browser.close()

    print("\n--- Extracción de Alineaciones/Jugadores Finalizada ---")
    print(f"Partidos con datos de alineación extraídos exitosamente: {len(all_matches_lineups)}")
    unique_failed_ids = sorted(list(set(failed_match_ids)))
    if unique_failed_ids:
        print(f"Partidos con errores o no encontrados: {len(unique_failed_ids)}")
        print(f"IDs fallidos: {unique_failed_ids}")

    return all_matches_lineups