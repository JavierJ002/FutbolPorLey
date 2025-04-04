import json
import os
import asyncio
import random
import time
from extractors.id_extractor import scrape_sofascore_rounds

# Función async wrapper para poder usar await
async def main():
    # Parámetro principal: cuántas rondas quieres scrapear de LaLiga 20/21
    NUMERO_DE_RONDAS = 3 # Cambia este valor al número N deseado

    # ... (la lógica de comprobación de carpeta puede permanecer igual) ...
    if not os.path.exists("extractors"):
         print("Advertencia: Carpeta 'extractors' no encontrada.")

    print(f"Ejecutando scrapeo para {NUMERO_DE_RONDAS} rondas usando Playwright...")

    # Ejecutamos la función de scraping (ahora es async)
    datos_scrapedos = await scrape_sofascore_rounds(NUMERO_DE_RONDAS) # <--- Usar await

    # Imprimimos los resultados de forma legible
    print("\n--- Resultados del Scrapeo ---")
    if datos_scrapedos:
        print(f"Nombre del Torneo: {datos_scrapedos.get('tournament_name', 'No obtenido')}") # Usar .get() es más seguro
        print(f"Temporada: {datos_scrapedos.get('season_name', 'No obtenida')}")
        print("\nIDs de Partidos Finalizados por Ronda:")

        rounds_data = datos_scrapedos.get("rounds_data", {}) # Usar .get()
        if rounds_data:
            # Ordenamos las rondas numéricamente para la impresión
            try:
                sorted_round_keys = sorted(
                    rounds_data.keys(),
                    key=lambda x: int(x.split()[-1])
                )
                for round_key in sorted_round_keys:
                    match_ids = rounds_data[round_key]
                    print(f"  {round_key}: {match_ids if match_ids else 'Ninguno encontrado o procesado'}")
            except (ValueError, IndexError): # Capturar posibles errores al parsear el número de ronda
                 print("Error al ordenar las rondas, mostrando en orden de diccionario:")
                 for round_key, match_ids in rounds_data.items():
                      print(f"  {round_key}: {match_ids if match_ids else 'Ninguno encontrado o procesado'}")

        else:
            print("  No se procesaron rondas o no se encontraron datos de partidos.")

        # Guardar los resultados en un archivo JSON
        try:
            # Nombre de archivo más descriptivo y específico
            output_filename = f"sofascore_data_LALIGA_20_21_rondas_1_a_{NUMERO_DE_RONDAS}.json"
            with open(output_filename, 'w', encoding='utf-8') as f:
                json.dump(datos_scrapedos, f, indent=4, ensure_ascii=False)
            print(f"\nDatos guardados en el archivo: {output_filename}")
        except Exception as e:
            print(f"\nError al guardar los datos en JSON: {e}")

    else:
        print("El scrapeo no pudo completarse o retornó None (posiblemente debido a errores de red o bloqueos).")

if __name__ == "__main__":
    # Ejecutamos la función main asíncrona
    asyncio.run(main()) # <--- Usar asyncio.run()