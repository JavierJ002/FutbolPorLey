# extractors/id_extractor.py
import asyncio
import json
import random
import time
from playwright.async_api import async_playwright

# Lista de user agents para rotar (puede ser compartida o definida aquí)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
]

_DEFAULT_TOURNAMENT_ID = 8  # LaLiga
_DEFAULT_SEASON_ID = 32501  # Temporada 20/21

async def scrape_round_match_ids(num_rounds=1):
    """
    Scrapea la API de SofaScore usando Playwright para obtener IDs de partidos finalizados
    para LaLiga (ID 8), temporada 20/21 (ID 32501) y un número de rondas dado.

    Args:
        num_rounds (int): El número de rondas (jornadas) a scrapear (desde la 1).

    Returns:
        dict: Un diccionario conteniendo el nombre del torneo, nombre de la temporada,
              y los IDs de los partidos finalizados por cada ronda.
              Devuelve un diccionario parcialmente lleno o con datos vacíos en caso de error.
    """
    result = {
        "tournament_name": f"LaLiga (ID: {_DEFAULT_TOURNAMENT_ID})", # Nombre fijo o intentar extraer
        "season_name": f"2020/2021 (ID: {_DEFAULT_SEASON_ID})", # Nombre fijo o intentar extraer
        "rounds_data": {}
    }
    # Podríamos intentar extraer los nombres reales si una petición funciona,
    # pero para simplificar y asegurar que siempre haya algo, usamos los fijos.
    # fetched_metadata = False

    print(f"--- Iniciando scrapeo de IDs con Playwright ---")
    print(f"Procesando Rondas: 1 a {num_rounds}")

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

            new_browser = await p.chromium.launch(headless=True)
            new_context = await new_browser.new_context(
                user_agent=random.choice(USER_AGENTS),
                viewport={"width": 1366, "height": 768}
            )
            await new_context.add_init_script("Object.defineProperty(navigator, 'webdriver', { get: () => undefined });")
            new_page = await new_context.new_page()

            try:
                 print("    Visitando página principal para inicializar contexto...")
                 await new_page.goto("https://www.sofascore.com/", wait_until="domcontentloaded", timeout=30000)
                 await asyncio.sleep(random.uniform(1, 3))
                 print("    Contexto inicializado.")
            except Exception as init_err:
                 print(f"    Advertencia: Falló visita a página principal durante inicialización: {init_err}")

            return new_browser, new_context, new_page

        browser, context, page = await setup_browser_context()

        # Procesar cada ronda
        for round_num in range(1, num_rounds + 1):
            print(f"Procesando Ronda {round_num} para obtener IDs...")

            # URL de la API para obtener los eventos de la ronda
            api_url = f"https://www.sofascore.com/api/v1/unique-tournament/{_DEFAULT_TOURNAMENT_ID}/season/{_DEFAULT_SEASON_ID}/events/round/{round_num}"
            round_page_url = f"https://www.sofascore.com/tournament/football/spain/laliga/{_DEFAULT_TOURNAMENT_ID}/season/{_DEFAULT_SEASON_ID}/matches/round/{round_num}"

            print(f"    API URL: {api_url}")

            match_ids_for_round = []
            try:
                # Pausa antes de la petición
                await asyncio.sleep(random.uniform(3, 7))

                # Visitar página de la ronda puede ayudar con el contexto
                try:
                    print(f"    Visitando página de ronda: {round_page_url}")
                    await page.goto(round_page_url, wait_until="domcontentloaded", timeout=40000)
                    await asyncio.sleep(random.uniform(1, 3))
                except Exception as round_page_err:
                    print(f"    Advertencia: Falló visita a página de ronda {round_num}: {round_page_err}")

                # Hacer la solicitud a la API
                print(f"    Realizando fetch a API: {api_url}")
                response = await page.goto(api_url, wait_until="commit", timeout=30000)

                if response is None:
                     print("   -> Error: page.goto a API devolvió None.")
                     raise ConnectionError("API request returned None")

                if response.status == 200:
                    content = await response.text()
                    data = json.loads(content)

                    # Extraer IDs de partidos finalizados (status code 100)
                    for event in data.get("events", []):
                        # Aquí podríamos extraer metadata si no la tuviéramos
                        # if not fetched_metadata and event.get(...): ...

                        if event.get("status", {}).get("code", 0) == 100:
                            match_id = event.get("id")
                            if match_id:
                                match_ids_for_round.append(match_id)

                    result["rounds_data"][f"Ronda {round_num}"] = match_ids_for_round
                    print(f"    -> Ronda {round_num}: Encontrados {len(match_ids_for_round)} partidos finalizados.")

                else:
                    body = await response.text()
                    print(f"    -> Error {response.status} obteniendo IDs para Ronda {round_num}.")
                    print(f"       Respuesta: {body[:150]}...")
                    result["rounds_data"][f"Ronda {round_num}"] = [] # Guardar lista vacía en error
                    # Si es 403, reiniciar contexto
                    if response.status == 403:
                        print("    -> Error 403 detectado. Reiniciando contexto...")
                        await asyncio.sleep(random.uniform(10, 20))
                        browser, context, page = await setup_browser_context(browser)

            except Exception as e:
                print(f"    -> Error general procesando Ronda {round_num} para IDs: {type(e).__name__} - {e}")
                result["rounds_data"][f"Ronda {round_num}"] = [] # Guardar lista vacía en error general
                # Podríamos implementar reinicio de contexto aquí también si el error es grave

        if browser:
            await browser.close()

    print("\n--- Scrapeo de IDs Finalizado ---")
    return result

