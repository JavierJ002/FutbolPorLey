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
_BASE_SOFASCORE_URL = "https://www.sofascore.com/" # Para warm-up

_DEFAULT_TOURNAMENT_ID = 8
_DEFAULT_SEASON_ID = 32501

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
        "tournament_name": f"LaLiga (ID: {_DEFAULT_TOURNAMENT_ID})",
        "season_name": f"2020/2021 (ID: {_DEFAULT_SEASON_ID})",
        "rounds_data": {}
    }

    print(f"--- Iniciando scrapeo de IDs con Playwright ---")
    print(f"Procesando Rondas: 1 a {num_rounds}")

    async with async_playwright() as p:
        browser = None
        context = None
        page = None

        # --- Función auxiliar para (re)iniciar navegador ---
        async def setup_browser_context(existing_browser=None):
            # Usa nonlocal para modificar las variables del scope externo si es necesario,
            # pero es más limpio devolverlas
            nonlocal browser, context, page # O define _browser, _context, _page y devuélvelos

            if existing_browser:
                 print("    Reiniciando contexto del navegador...")
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
                 print(f"    Visitando página principal ({_BASE_SOFASCORE_URL}) para inicializar contexto...")
                 await new_page.goto(_BASE_SOFASCORE_URL, wait_until="domcontentloaded", timeout=30000)
                 await asyncio.sleep(random.uniform(1, 3))
                 print("    Contexto inicializado.")
            except Exception as init_err:
                 print(f"    Advertencia: Falló visita a página principal durante inicialización: {init_err}")

            # Actualizar las variables nonlocal o devolver las nuevas instancias
            browser = new_browser
            context = new_context
            page = new_page
            # O return new_browser, new_context, new_page

        # Inicialización inicial
        try:
             await setup_browser_context()
             # O: browser, context, page = await setup_browser_context()
        except Exception as initial_setup_err:
             print(f"Error FATAL: No se pudo inicializar el navegador Playwright: {initial_setup_err}")
             return result # Devuelve resultado parcial o vacío

        # Procesar cada ronda
        for round_num in range(1, num_rounds + 1):
            print(f"Procesando Ronda {round_num} para obtener IDs...")

            # URL de la API para obtener los eventos de la ronda
            api_url = f"https://www.sofascore.com/api/v1/unique-tournament/{_DEFAULT_TOURNAMENT_ID}/season/{_DEFAULT_SEASON_ID}/events/round/{round_num}"
            round_page_url = f"https://www.sofascore.com/tournament/football/spain/laliga/{_DEFAULT_TOURNAMENT_ID}/season/{_DEFAULT_SEASON_ID}/matches/round/{round_num}"

            print(f"    API URL: {api_url}")

            match_ids_for_round = []
            try:
                await asyncio.sleep(random.uniform(3, 7)) # Pausa
                # Visitar página de la ronda puede ayudar con el contexto
                try:
                    print(f"    Visitando página de ronda: {round_page_url}")
                    await page.goto(round_page_url, wait_until="domcontentloaded", timeout=40000)
                    await asyncio.sleep(random.uniform(1, 3))
                except Exception as round_page_err:
                    print(f"    Advertencia: Falló visita a página de ronda {round_num}: {round_page_err}")

                print(f"    Realizando fetch a API: {api_url}")
                response = await page.goto(api_url, wait_until="commit", timeout=30000)

                if response is None:
                     print("   -> Error: page.goto a API devolvió None.")
                     raise ConnectionError("API request returned None") # Lanza error para capturar abajo

                if response.status == 200:
                    content = await response.text()
                    data = json.loads(content)

                    # Extraer IDs de partidos finalizados (status code 100)
                    for event in data.get("events", []):
                        # Añadir comprobación robusta de existencia de claves
                        status_info = event.get("status", {})
                        if isinstance(status_info, dict) and status_info.get("code") == 100:
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
                        await setup_browser_context(browser) # Pasar navegador actual para cerrar
                        # O: browser, context, page = await setup_browser_context(browser)


            except Exception as e:
                print(f"    -> Error general procesando Ronda {round_num} para IDs: {type(e).__name__} - {e}")
                result["rounds_data"][f"Ronda {round_num}"] = [] # Guardar lista vacía en error general

        # Cerrar navegador al final
        if browser:
            try:
                 await browser.close()
            except Exception as final_close_err:
                 print(f"    Advertencia: Error al cerrar el navegador al final: {final_close_err}")


    print("\n--- Scrapeo de IDs Finalizado ---")
    return result