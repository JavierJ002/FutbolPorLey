import asyncio
import json
import random
import time
from playwright.async_api import async_playwright
import traceback

# User agents (puede venir de un archivo común o definirse aquí)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
]

_BASE_SOFASCORE_URL = "https://www.sofascore.com/"

# Lista y mapa de estadísticas (igual que antes)
STATS_TO_EXTRACT = [
    "Ball possession", "Big chances", "Total shots", "Goalkeeper saves",
    "Corner kicks", "Fouls", "Passes", "Tackles", "Free kicks",
    "Yellow cards", "Red cards", "Shots on target", "Hit woodwork",
    "Shots off target", "Blocked shots", "Shots inside box",
    "Shots outside box", "Big chances missed", "Fouled in final third",
    "Offsides", "Accurate passes", "Throw-ins", "Final third entries",
    "Long balls", "Crosses", "Duels", "Dispossessed", "Ground duels",
    "Aerial duels", "Dribbles", "Tackles won", "Total tackles",
    "Interceptions", "Clearances", "Goal kicks", "Total saves"
]
STATS_NAME_MAP = {
    "Ball possession": "possession", "Big chances": "big_chances", "Total shots": "total_shots",
    "Goalkeeper saves": "saves", "Total saves": "saves", "Corner kicks": "corners",
    "Fouls": "fouls", "Passes": "passes_total", "Tackles": "tackles_total",
    "Total tackles": "tackles_total", "Free kicks": "free_kicks", "Yellow cards": "yellow_cards",
    "Red cards": "red_cards", "Shots on target": "shots_on_target", "Hit woodwork": "hit_woodwork",
    "Shots off target": "shots_off_target", "Blocked shots": "blocked_shots",
    "Shots inside box": "shots_inside_box", "Shots outside box": "shots_outside_box",
    "Big chances missed": "big_chances_missed", "Fouled in final third": "fouled_final_third",
    "Offsides": "offsides", "Accurate passes": "accurate_passes", "Throw-ins": "throw_ins",
    "Final third entries": "final_third_entries", "Long balls": "long_balls", "Crosses": "crosses",
    "Duels": "duels_won_percent", "Dispossessed": "dispossessed", "Ground duels": "ground_duels",
    "Aerial duels": "aerial_duels", "Dribbles": "dribbles", "Tackles won": "tackles_won_percent",
    "Interceptions": "interceptions", "Clearances": "clearances", "Goal kicks": "goal_kicks"
}

def _parse_statistics_data(statistics_json_list, stats_to_extract_names, name_map):
    """Parsea la lista JSON de la API /statistics."""
    # (Misma función _parse_statistics_data que en la respuesta anterior)
    stats_data = {"ALL": {"home": {}, "away": {}},
                  "1ST": {"home": {}, "away": {}},
                  "2ND": {"home": {}, "away": {}}}
    for period in stats_data:
        for team in ["home", "away"]:
            for stat_name in stats_to_extract_names:
                stat_key = name_map.get(stat_name, stat_name.lower().replace(" ", "_"))
                stats_data[period][team][stat_key] = None
    for period_stats_obj in statistics_json_list:
        period_code = period_stats_obj.get("period")
        if period_code not in stats_data: continue
        for group in period_stats_obj.get("groups", []):
            for item in group.get("statisticsItems", []):
                stat_name = item.get("name")
                if stat_name in stats_to_extract_names:
                    stat_key = name_map.get(stat_name, stat_name.lower().replace(" ", "_"))
                    stats_data[period_code]["home"][stat_key] = item.get("home")
                    stats_data[period_code]["away"][stat_key] = item.get("away")
    return stats_data

async def _fetch_stats_data_pw(page, match_id):
    """
    Obtiene datos de la API /statistics usando el contexto de Playwright.
    CORREGIDO para manejar la respuesta como un objeto JSON con clave "statistics".
    """
    stats_api_url = f"https://www.sofascore.com/api/v1/event/{match_id}/statistics"
    event_page_url = f"https://www.sofascore.com/event/{match_id}"
    print(f"    Intentando fetch de estadísticas para Match ID: {match_id} (API: {stats_api_url})")
    try:
        # 1. (Opcional) Visitar página del evento
        try:
            # print(f"    Navegando a página del evento para contexto: {event_page_url}") # Menos verboso
            await page.goto(event_page_url, wait_until="domcontentloaded", timeout=45000)
            await asyncio.sleep(random.uniform(1.0, 2.5))
            # print(f"    Página del evento cargada.") # Menos verboso
        except Exception as page_load_err:
             print(f"    Advertencia: No se pudo cargar la página del evento {match_id}: {type(page_load_err).__name__}. Intentando fetch directo...")

        # 2. Fetch directo a la API /statistics
        # print(f"    Realizando fetch a API de estadísticas: {stats_api_url}") # Menos verboso
        response = await page.goto(stats_api_url, wait_until="commit", timeout=30000)

        if response is None:
            print("    -> Error: page.goto a API devolvió None.")
            return None

        if response.status == 200:
            content = await response.text()
            # --- INICIO: Corrección ---
            # La respuesta es un OBJETO JSON '{"statistics": [ ... ]}'
            if content.strip().startswith("{") and content.strip().endswith("}"):
                 try:
                     data_object = json.loads(content)
                     # Extraer la lista bajo la clave "statistics"
                     stats_list = data_object.get("statistics") # Usar .get() es más seguro

                     if stats_list is not None and isinstance(stats_list, list):
                          # print("    -> Fetch y extracción de lista de estadísticas exitoso.") # Opcional
                          return stats_list # Devuelve la LISTA correctamente
                     else:
                          # Esto pasaría si la respuesta es un objeto {}, pero no tiene la clave "statistics" o no es una lista
                          print(f"    -> Error: Clave 'statistics' no encontrada o no es una lista en la respuesta JSON. Respuesta: {content[:300]}...")
                          return None

                 except json.JSONDecodeError as json_err:
                     # Esto pasaría si empieza con { pero no es JSON válido
                     print(f"    -> Error: No se pudo decodificar el JSON de /statistics. Error: {json_err}. Contenido: {content[:300]}...")
                     return None
            # --- FIN: Corrección ---
            else:
                # La respuesta 200 no empieza con { o no termina con }
                print(f"    -> Error: La respuesta 200 de /statistics no parece ser un objeto JSON válido. Contenido: {content[:200]}...")
                return None
        else:
            # Manejo de errores HTTP (403, 404, etc. - sin cambios)
            body = await response.text()
            print(f"    -> Error en fetch de API de estadísticas: {response.status}")
            if response.status == 403: raise PermissionError(f"403 Forbidden para {stats_api_url}")
            elif response.status == 404: return {"error": 404, "message": "Statistics not found"}
            else: raise ConnectionError(f"Error {response.status} para {stats_api_url}")

    except PermissionError as pe:
         # print(f"    -> Error de Permiso (403) obteniendo estadísticas para Match ID {match_id}") # Ya se imprime arriba
         return {"error": 403, "message": "Forbidden"}
    except Exception as e:
        print(f"    -> Error inesperado durante fetch de estadísticas para Match ID {match_id}: {type(e).__name__}")
        # traceback.print_exc() # Descomentar para más detalle si es necesario
        return {"error": 500, "message": f"Unexpected error: {type(e).__name__}"}


async def extract_statistics_for_match_ids(match_ids_list):
    """
    Recibe una LISTA de IDs de partidos y extrae sus estadísticas
    usando la API /statistics y Playwright.

    Args:
        match_ids_list (list): Una lista plana de IDs de partidos (int o str).

    Returns:
        list: Una lista de diccionarios, donde cada diccionario contiene
              el 'match_id' y las 'statistics' parseadas.
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

        # Función para inicializar/reiniciar el navegador y contexto
        async def setup_browser_context(existing_browser=None):
             nonlocal browser, context, page
             if existing_browser:
                 print("    Reiniciando contexto del navegador...")
                 await existing_browser.close()
             new_browser = await p.chromium.launch(headless=True) # Poner True para producción
             new_context = await new_browser.new_context(
                 user_agent=random.choice(USER_AGENTS),
                 viewport={"width": 1366, "height": 768}
             )
             await new_context.add_init_script("Object.defineProperty(navigator, 'webdriver', { get: () => undefined });")
             new_page = await new_context.new_page()
             try:
                 # print("    Visitando página principal para inicializar contexto...")
                 await new_page.goto(_BASE_SOFASCORE_URL, wait_until="domcontentloaded", timeout=30000)
                 await asyncio.sleep(random.uniform(0.5, 1.5)) # Pausa corta
                 # print("    Contexto inicializado.")
             except Exception as init_err:
                 print(f"    Advertencia: Falló visita a página principal durante inicialización: {init_err}")
             return new_browser, new_context, new_page

        browser, context, page = await setup_browser_context()

        # --- Bucle principal iterando sobre la LISTA de IDs ---
        for i, match_id in enumerate(match_ids_list):
            print(f"  Procesando Partido {i+1}/{total_ids_to_process} (ID: {match_id})")

            # Pausa antes de procesar el ID
            await asyncio.sleep(random.uniform(3.5, 7.5)) # Pausa entre partidos

            stats_json_list = await _fetch_stats_data_pw(page, match_id)

            # Manejo de errores y recuperación
            if isinstance(stats_json_list, dict) and stats_json_list.get("error") == 403:
                print(f"    -> Error 403 obteniendo estadísticas para {match_id}. Intentando recuperación...")
                failed_match_ids.append(match_id)
                await asyncio.sleep(random.uniform(15, 25))
                browser, context, page = await setup_browser_context(browser)
                continue

            elif isinstance(stats_json_list, dict) and stats_json_list.get("error") == 404:
                print(f"    -> Estadísticas para {match_id} no encontradas (404). Saltando.")
                failed_match_ids.append(match_id)
                continue

            elif stats_json_list is None or isinstance(stats_json_list, dict):
                error_msg = stats_json_list.get('message', 'desconocido') if isinstance(stats_json_list, dict) else 'fetch devolvió None'
                print(f"    -> Falló la obtención de estadísticas para {match_id} (error: {error_msg}). Saltando.")
                failed_match_ids.append(match_id)
                # Podríamos añadir recuperación si fallan muchos seguidos
                await asyncio.sleep(random.uniform(2, 4)) # Pequeña pausa adicional en error
                continue

            # Procesar y guardar si todo OK
            try:
                parsed_stats = _parse_statistics_data(stats_json_list, STATS_TO_EXTRACT, STATS_NAME_MAP)
                all_matches_stats.append({
                    "match_id": match_id,
                    "statistics": parsed_stats
                })
                # print(f"    -> Estadísticas procesadas y añadidas para Match ID: {match_id}") # Opcional: menos verboso
            except Exception as parse_err:
                print(f"    -> Error parseando JSON de estadísticas para Match ID {match_id}: {parse_err}")
                failed_match_ids.append(match_id)

        # --- Fin del bucle ---
        if browser:
            await browser.close()

    print("\n--- Extracción de Estadísticas Finalizada ---")
    print(f"Partidos con estadísticas extraídas exitosamente: {len(all_matches_stats)}")
    unique_failed_ids = sorted(list(set(failed_match_ids)))
    if unique_failed_ids:
        print(f"Partidos con errores o no encontrados: {len(unique_failed_ids)}")
        print(f"IDs fallidos: {unique_failed_ids}")

    return all_matches_stats