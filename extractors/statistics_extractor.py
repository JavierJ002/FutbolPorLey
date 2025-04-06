# extractors/statistics_extractor.py
import asyncio
import json
import random
import time
from playwright.async_api import async_playwright
import traceback
from typing import List, Dict, Any, Optional, Union # Añadido Typing

# USER_AGENTS debe estar definido aquí o importado
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
]

_BASE_SOFASCORE_URL = "https://www.sofascore.com/"

# Lista de nombres de estadísticas EXACTOS como aparecen en la API
STATS_TO_EXTRACT = [
    "Ball possession", "Big chances", "Total shots", "Goalkeeper saves",
    "Corner kicks", "Fouls", "Passes", "Tackles", "Free kicks",
    "Yellow cards", "Red cards", "Shots on target", "Hit woodwork",
    "Shots off target", "Blocked shots", "Shots inside box",
    "Shots outside box", "Big chances missed", "Fouled in final third",
    "Offsides", "Accurate passes", "Throw-ins", "Final third entries",
    "Long balls", "Crosses", "Duels", # Duels puede ser % o X/Y
    "Dispossessed", "Ground duels", "Aerial duels", "Dribbles",
    "Tackles won", # A veces viene como X/Y (%)
    "Total tackles", # A veces solo viene el total
    "Interceptions", "Clearances", "Goal kicks",
    "Total saves" # A veces viene como alternativa a Goalkeeper saves
]

# Mapeo de nombres API a claves deseadas en el JSON final (más legibles/consistentes)
# Combina claves duplicadas si es necesario (e.g., saves)
STATS_NAME_MAP = {
    "Ball possession": "possession_percentage", # Guardar como float 0-1
    "Big chances": "big_chances",
    "Total shots": "total_shots",
    "Goalkeeper saves": "saves",
    "Total saves": "saves", # Mapear al mismo
    "Corner kicks": "corners",
    "Fouls": "fouls",
    "Passes": "passes_complex", # Guardar como dict {successful, total, percentage}
    "Tackles": "tackles_complex", # Puede ser X/Y, guardar como dict
    "Total tackles": "tackles_total_simple", # Si viene solo el total
    "Free kicks": "free_kicks",
    "Yellow cards": "yellow_cards",
    "Red cards": "red_cards",
    "Shots on target": "shots_on_target",
    "Hit woodwork": "hit_woodwork",
    "Shots off target": "shots_off_target",
    "Blocked shots": "blocked_shots", # Tiros propios bloqueados
    "Shots inside box": "shots_inside_box",
    "Shots outside box": "shots_outside_box",
    "Big chances missed": "big_chances_missed",
    "Fouled in final third": "fouled_final_third",
    "Offsides": "offsides",
    "Accurate passes": "accurate_passes_percentage", # Guardar como float 0-1
    "Throw-ins": "throw_ins",
    "Final third entries": "final_third_entries",
    "Long balls": "long_balls_complex", # Guardar como dict
    "Crosses": "crosses_complex", # Guardar como dict
    "Duels": "duels_complex", # Guardar como dict (o float si es solo %)
    "Dispossessed": "dispossessed",
    "Ground duels": "ground_duels_complex", # Guardar como dict
    "Aerial duels": "aerial_duels_complex", # Guardar como dict
    "Dribbles": "dribbles_complex", # Guardar como dict
    "Tackles won": "tackles_won_complex", # Guardar como dict
    "Interceptions": "interceptions",
    "Clearances": "clearances",
    "Goal kicks": "goal_kicks"
}
# Obtener todas las claves objetivo únicas para inicializar
TARGET_STAT_KEYS = set(STATS_NAME_MAP.values())


def _convert_to_numeric(value: Any, stat_key_target: Optional[str] = None) -> Optional[Union[int, float, Dict[str, Any], str]]:
    """
    Convierte un valor de estadística a formato numérico o diccionario estructurado.
    Devuelve el string original si no se puede convertir.
    """
    if value is None:
        return None

    value_str = str(value).strip()

    # Caso para valores como "33/71 (46%)" -> dict
    if '/' in value_str and '(' in value_str and value_str.endswith(')'):
        try:
            parts = value_str.split('(')
            fraction_part = parts[0].strip()
            percentage_part = parts[1].split(')')[0].strip('% ') # Limpiar % y espacios

            successful, total = map(int, fraction_part.split('/'))
            # Usar más precisión para porcentajes
            percentage = round(float(percentage_part) / 100, 4)

            return {
                "successful": successful,
                "total": total,
                "percentage": percentage
            }
        except (ValueError, IndexError, TypeError):
             print(f"Advertencia: Error convirtiendo valor complejo '{value_str}' para stat '{stat_key_target}'.")
             return value_str # Devolver original en error

    # Caso para porcentajes simples como "50%" -> float
    elif value_str.endswith('%'):
        try:
            # Usar más precisión
            return round(float(value_str.strip('% ')) / 100, 4)
        except ValueError:
            print(f"Advertencia: Error convirtiendo porcentaje simple '{value_str}' para stat '{stat_key_target}'.")
            return value_str

    # Caso para valores numéricos simples (enteros) como "3" -> int
    elif value_str.isdigit():
        return int(value_str)

    # Caso para valores flotantes simples como "7.5" (menos común en stats de equipo)
    elif '.' in value_str:
        try:
             # Asegurar punto decimal
             return float(value_str.replace(',', '.'))
        except ValueError:
             pass # Dejar que devuelva el string original abajo

    # Devolver el string original si no coincide con ningún formato numérico
    # print(f"Debug: Valor '{value_str}' para stat '{stat_key_target}' no reconocido como numérico/complejo estándar.")
    return value_str


def _parse_statistics_data(statistics_json_list: List[Dict], stats_to_extract_names: List[str], name_map: Dict[str, str]) -> Dict[str, Dict[str, Dict]]:
    """
    Parsea la lista JSON de la API /statistics según los nombres y el mapa proporcionados.
    Convierte valores a tipos numéricos/dict usando _convert_to_numeric.

    Returns:
        Dict: {'ALL': {'home': {...}, 'away': {...}}, '1ST': {...}, '2ND': {...}}
              con las claves del `name_map` y valores convertidos.
    """
    # Estructura de datos para almacenar resultados por periodo
    stats_data = {"ALL": {"home": {}, "away": {}},
                  "1ST": {"home": {}, "away": {}},
                  "2ND": {"home": {}, "away": {}}}

    # Inicializar todas las estadísticas objetivo posibles con None en todos los periodos/equipos
    target_keys = set(name_map.values())
    for period in stats_data:
        for team_loc in ["home", "away"]:
            stats_data[period][team_loc] = {key: None for key in target_keys}

    # Procesar los datos recibidos de la API
    for period_stats_obj in statistics_json_list:
        period_code = period_stats_obj.get("period") # ALL, 1ST, 2ND
        if period_code not in stats_data:
            print(f"Advertencia: Periodo '{period_code}' inesperado encontrado en la API. Ignorando.")
            continue

        # Iterar sobre los grupos de estadísticas (e.g., Attacking, Defending)
        for group in period_stats_obj.get("groups", []):
            # Iterar sobre los items de estadísticas dentro de cada grupo
            for item in group.get("statisticsItems", []):
                stat_name_api = item.get("name") # Nombre como viene de la API

                # Verificar si esta estadística está en nuestra lista de interés
                if stat_name_api in stats_to_extract_names:
                    # Obtener la clave objetivo mapeada
                    stat_key_target = name_map.get(stat_name_api)
                    if not stat_key_target:
                         print(f"Advertencia: Stat '{stat_name_api}' está en STATS_TO_EXTRACT pero no en STATS_NAME_MAP.")
                         continue

                    # Extraer y convertir valores para home y away
                    for team_loc in ["home", "away"]:
                        raw_value = item.get(team_loc) # Valor como string desde la API
                        converted_value = _convert_to_numeric(raw_value, stat_key_target)

                        # Guardar el valor convertido en nuestra estructura de datos
                        stats_data[period_code][team_loc][stat_key_target] = converted_value

    return stats_data


async def _fetch_stats_data_pw(page, match_id: str) -> Optional[Union[List[Dict], Dict[str, Any]]]:
    """
    Obtiene datos de la API /statistics usando Playwright.
    Devuelve la LISTA bajo la clave "statistics" si tiene éxito, o un dict de error.
    """
    stats_api_url = f"https://www.sofascore.com/api/v1/event/{match_id}/statistics"
    event_page_url = f"https://www.sofascore.com/event/{match_id}"
    print(f"    Intentando fetch de estadísticas para Match ID: {match_id} (API: {stats_api_url})")

    response = None
    try:
        # 1. Visitar página del evento (puede ayudar con cookies/estado)
        try:
            print(f"    Visitando página del evento: {event_page_url}")
            await page.goto(event_page_url, wait_until="domcontentloaded", timeout=45000)
            await asyncio.sleep(random.uniform(1.0, 2.5))
            print(f"    Página del evento {match_id} visitada.")
        except Exception as page_load_err:
             print(f"    Advertencia: No se pudo cargar la página del evento {match_id}: {type(page_load_err).__name__}. Intentando fetch directo...")

        # 2. Fetch directo a la API /statistics
        print(f"    Realizando fetch directo a API: {stats_api_url}")
        response = await page.goto(stats_api_url, wait_until="commit", timeout=30000)

        if response is None:
            print(f"    -> Error: page.goto a API /statistics para {match_id} devolvió None.")
            return {"error": 500, "message": "Playwright goto returned None"}

        status = response.status
        print(f"    Respuesta API /statistics para {match_id}: Status {status}")

        if status == 200:
            content = await response.text()
            # Validar si es un objeto JSON que contiene la clave "statistics" con una lista
            if content.strip().startswith("{") and content.strip().endswith("}"):
                 try:
                    data_object = json.loads(content)
                    stats_list = data_object.get("statistics") # Extraer la lista

                    if stats_list is not None and isinstance(stats_list, list):
                          print(f"    -> Estadísticas obtenidas correctamente para {match_id}.")
                          return stats_list # ¡Devolver la LISTA!
                    else:
                          # El JSON es válido, pero no tiene 'statistics' o no es una lista
                          print(f"    -> Error: Clave 'statistics' no encontrada o no es lista en JSON para {match_id}. Respuesta: {content[:300]}...")
                          return {"error": 500, "message": "Invalid JSON structure: 'statistics' key missing or not a list"}

                 except json.JSONDecodeError as json_err:
                    # El contenido parece JSON pero no se puede decodificar
                    print(f"    -> Error: No se pudo decodificar el JSON de /statistics para {match_id}. Error: {json_err}. Contenido: {content[:300]}...")
                    return {"error": 500, "message": f"JSON Decode Error: {json_err}"}
            else:
                # La respuesta 200 no tiene formato de objeto JSON
                print(f"    -> Error: La respuesta 200 de /statistics para {match_id} no parece ser objeto JSON válido. Contenido: {content[:200]}...")
                return {"error": 500, "message": "Invalid JSON format in 200 response"}

        # --- Manejo de errores HTTP ---
        else:
            body = await response.text() # Obtener cuerpo para logs
            print(f"    -> Error en fetch de API de estadísticas: {status}")
            if status == 403:
                # Devolver dict de error para manejo específico
                return {"error": 403, "message": "Forbidden"}
            elif status == 404:
                print(f"    -> Estadísticas no encontradas (404) para match {match_id}.")
                return {"error": 404, "message": "Statistics not found"}
            else:
                # Otros errores HTTP
                 print(f"       Respuesta: {body[:150]}...")
                 return {"error": status, "message": f"HTTP Error {status}"}

    except asyncio.TimeoutError:
        print(f"    -> Error: Timeout durante fetch de /statistics para Match ID {match_id}")
        return {"error": 408, "message": "Request Timeout"}
    except Exception as e:
        # Captura otros errores de Playwright o red
        print(f"    -> Error inesperado durante fetch de estadísticas para Match ID {match_id}: {type(e).__name__}")
        traceback.print_exc() # Ayuda a depurar
        return {"error": 500, "message": f"Unexpected error: {type(e).__name__}"}


async def extract_statistics_for_match_ids(match_ids_list: List[Union[str, int]]) -> List[Dict[str, Any]]:
    """
    Recibe una LISTA de IDs de partidos y extrae sus estadísticas
    usando la API /statistics y Playwright.

    Args:
        match_ids_list (list): Una lista plana de IDs de partidos (int o str).

    Returns:
        list: Una lista de diccionarios, donde cada diccionario contiene
              el 'match_id' y las 'statistics' parseadas (o un error).
    """
    all_matches_stats = []
    failed_match_ids = []
    total_ids_to_process = len(match_ids_list)

    print(f"--- Iniciando extracción de estadísticas para {total_ids_to_process} partidos ---")

    if not total_ids_to_process:
        print("No hay IDs de partidos en la lista de entrada.")
        return []

    async with async_playwright() as p:
        browser = None
        context = None
        page = None

        # --- Función auxiliar para (re)iniciar navegador ---
        async def setup_browser_context(existing_browser=None):
            # Usa nonlocal o devuelve las nuevas instancias
            nonlocal browser, context, page
            if existing_browser:
                print("    Reiniciando contexto del navegador (Team Stats)...")
                try: await existing_browser.close()
                except Exception: pass # Ignorar error al cerrar
            try:
                 new_browser = await p.chromium.launch(headless=True)
                 new_context = await new_browser.new_context(
                     user_agent=random.choice(USER_AGENTS),
                     viewport={"width": 1366, "height": 768}
                 )
                 await new_context.add_init_script("Object.defineProperty(navigator, 'webdriver', { get: () => undefined });")
                 new_page = await new_context.new_page()
                 # Warm-up visit
                 await new_page.goto(_BASE_SOFASCORE_URL, wait_until="domcontentloaded", timeout=40000)
                 await asyncio.sleep(random.uniform(0.5, 1.5))
                 print("    Contexto del navegador (Team Stats) inicializado.")
                 # Actualizar o devolver
                 browser = new_browser
                 context = new_context
                 page = new_page
                 # return new_browser, new_context, new_page
            except Exception as setup_err:
                 print(f"Error fatal configurando navegador (Team Stats): {setup_err}")
                 raise setup_err # Re-lanzar para detener si falla aquí


        # --- Inicialización del Navegador ---
        try:
             await setup_browser_context()
             # O: browser, context, page = await setup_browser_context()
        except Exception:
             print("No se pudo inicializar el navegador. Terminando proceso de stats de equipo.")
             return [] # Salir

        # --- Bucle principal iterando sobre la LISTA de IDs ---
        for i, match_id_any in enumerate(match_ids_list):
            match_id = str(match_id_any) # Usar string consistentemente
            print(f"  Procesando Estadísticas Partido {i+1}/{total_ids_to_process} (ID: {match_id})")

            await asyncio.sleep(random.uniform(3.5, 7.5)) # Pausa

            # Fetch de datos
            stats_result = await _fetch_stats_data_pw(page, match_id)

            # --- Manejo del Resultado del Fetch ---
            fetch_error_data = None
            if isinstance(stats_result, dict) and "error" in stats_result:
                 fetch_error_data = stats_result

            if fetch_error_data:
                error_code = fetch_error_data.get("error")
                error_msg = fetch_error_data.get("message", "Error desconocido")
                print(f"    -> Falló la obtención de estadísticas para {match_id} (Error {error_code}): {error_msg}")
                failed_match_ids.append(match_id)
                # Añadir entrada de error al resultado
                all_matches_stats.append({
                    "match_id": match_id,
                    "statistics": {"error": error_code, "message": error_msg}
                })

                if error_code == 403:
                    print("    -> Error 403 detectado. Intentando reiniciar contexto...")
                    await asyncio.sleep(random.uniform(15, 25))
                    try:
                        await setup_browser_context(browser) # Reiniciar
                        # O: browser, context, page = await setup_browser_context(browser)
                    except Exception as reset_err:
                         print(f"Error FATAL reiniciando navegador después de 403: {reset_err}")
                         break # Salir del bucle
                else:
                    await asyncio.sleep(random.uniform(2, 4)) # Pausa para otros errores
                continue # Al siguiente ID

            # --- Procesar y guardar si Fetch OK (stats_result es una lista) ---
            if isinstance(stats_result, list):
                try:
                    # Parsear los datos usando la lista de nombres y el mapa
                    parsed_stats = _parse_statistics_data(stats_result, STATS_TO_EXTRACT, STATS_NAME_MAP)
                    # Añadir resultado exitoso
                    all_matches_stats.append({
                        "match_id": match_id,
                        "statistics": parsed_stats # El diccionario parseado por periodos
                    })
                except Exception as parse_err:
                    print(f"    -> Error FATAL parseando JSON de estadísticas para Match ID {match_id}: {parse_err}")
                    traceback.print_exc()
                    failed_match_ids.append(match_id)
                    # Añadir entrada de error de parseo
                    all_matches_stats.append({
                         "match_id": match_id,
                         "statistics": {"error": 500, "message": f"Parsing Error: {parse_err}"}
                    })
            else:
                 # Si no es ni error ni lista (no debería ocurrir con fetch revisado)
                 print(f"    -> Advertencia: Resultado inesperado de fetch para {match_id} (no es lista ni error dict). Saltando.")
                 failed_match_ids.append(match_id)
                 all_matches_stats.append({
                      "match_id": match_id,
                      "statistics": {"error": 500, "message": "Unexpected data type after fetch"}
                 })


        # --- Cerrar navegador al final ---
        if browser:
            try:
                await browser.close()
            except Exception as final_close_err:
                 print(f"    Advertencia: Error al cerrar el navegador al final (Team Stats): {final_close_err}")

    # --- Resumen ---
    print("\n--- Extracción de Estadísticas Finalizada ---")
    successful_count = sum(1 for item in all_matches_stats if isinstance(item.get("statistics"), dict) and "error" not in item.get("statistics", {}))
    print(f"Estadísticas de equipo obtenidas para {successful_count} partidos.")
    unique_failed_ids = sorted(list(set(failed_match_ids)))
    if unique_failed_ids:
        print(f"Partidos con errores durante fetch/parseo de estadísticas: {len(unique_failed_ids)}")
        # print(f"IDs fallidos: {unique_failed_ids}")

    return all_matches_stats