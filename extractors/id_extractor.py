import asyncio
import json
import random
import time
from playwright.async_api import async_playwright

# Lista de user agents para rotar
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36"
]

async def scrape_sofascore_rounds(num_rounds=1):
    result = {
        "tournament_name": "LaLiga",
        "season_name": "2020/2021",
        "rounds_data": {}
    }
    
    async with async_playwright() as p:
        # Usar un navegador persistente con caché para parecer más humano
        browser = await p.chromium.launch(headless=False)  # headless=False para visualizar el proceso
        
        # Creamos un contexto con cookies persistentes
        context = await browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            viewport={"width": 1366, "height": 768}
        )
        
        # Configuraciones para evitar la detección de bot
        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """)
        
        # Primero visitamos la página principal para obtener cookies
        page = await context.new_page()
        
        # Visitar la página principal primero
        try:
            await page.goto("https://www.sofascore.com/", wait_until="networkidle")
            # Esperar un poco para simular comportamiento humano
            await asyncio.sleep(random.uniform(2, 4))
        except Exception as e:
            print(f"Error al cargar la página principal: {e}")
        
        # Procesar cada ronda
        for round_num in range(1, num_rounds + 1):
            print(f"Procesando Ronda {round_num}...")
            
            # URL de la API para obtener los eventos de la ronda
            api_url = f"https://www.sofascore.com/api/v1/unique-tournament/8/season/32501/events/round/{round_num}"
            
            # Imprimir la URL que estamos consultando
            print(f"    Intentando fetch con Playwright: {api_url}")
            
            try:
                # Añadir delay aleatorio entre peticiones
                await asyncio.sleep(random.uniform(3, 7))
                
                # Intentar acceder a la página de la ronda primero
                round_page_url = f"https://www.sofascore.com/tournament/football/spain/laliga/8/season/32501/matches/round/{round_num}"
                await page.goto(round_page_url, wait_until="domcontentloaded")
                await asyncio.sleep(random.uniform(2, 4))
                
                # Luego hacemos la solicitud a la API
                response = await page.goto(api_url)
                
                if response.status == 200:
                    content = await response.text()
                    data = json.loads(content)
                    
                    # Extraer IDs de partidos finalizados
                    match_ids = []
                    for event in data.get("events", []):
                        if event.get("status", {}).get("code", 0) == 100:  # 100 = partido finalizado
                            match_ids.append(event.get("id"))
                    
                    # Guardar los IDs para esta ronda
                    result["rounds_data"][f"Ronda {round_num}"] = match_ids
                    print(f"    -> Se encontraron {len(match_ids)} partidos finalizados en Ronda {round_num}")
                
                else:
                    print(f"    -> Error de Permiso ({response.status}) con Playwright: {await response.text()[:100]}...")
                    print(f"  -> No se pudo obtener datos para la Ronda {round_num} vía Playwright.")
                    
                    # Estrategia de recuperación
                    print("    -> Intentando estrategia de recuperación...")
                    
                    # Cerrar y volver a crear el contexto
                    await context.close()
                    context = await browser.new_context(
                        user_agent=random.choice(USER_AGENTS),
                        viewport={"width": 1366, "height": 768}
                    )
                    page = await context.new_page()
                    
                    # Visitar unas cuantas páginas aleatorias primero
                    random_pages = [
                        "https://www.sofascore.com/football",
                        "https://www.sofascore.com/football/spain/laliga/8",
                        "https://www.sofascore.com/news"
                    ]
                    
                    for rand_page in random.sample(random_pages, 2):
                        await page.goto(rand_page, wait_until="domcontentloaded")
                        await asyncio.sleep(random.uniform(3, 8))
                        
                        # Simular scroll
                        for _ in range(random.randint(2, 5)):
                            await page.evaluate("window.scrollBy(0, window.innerHeight * Math.random());")
                            await asyncio.sleep(random.uniform(1, 3))
                    
                    # Esperar un tiempo más largo antes de reintentar
                    await asyncio.sleep(random.uniform(10, 20))
                    
            except Exception as e:
                print(f"    -> Error al procesar la Ronda {round_num}: {e}")
                # Esperamos un tiempo entre intentos
                await asyncio.sleep(random.uniform(5, 10))
        
        await browser.close()
    
    return result