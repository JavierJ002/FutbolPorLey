# main.py
import json
import os
import asyncio

# Importar las funciones principales de cada extractor
from extractors.id_extractor import scrape_round_match_ids
from extractors.statistics_extractor import extract_statistics_for_match_ids

async def main():
    NUMERO_DE_RONDAS = 3 # Configura cuántas rondas obtener IDs
    # Nombres de archivo para resultados intermedios y finales
    id_data_filename = f"sofascore_ROUND_DATA_LALIGA_20_21_rondas_1_a_{NUMERO_DE_RONDAS}.json"
    final_stats_filename = f"sofascore_STATS_DATA_LALIGA_20_21_rondas_1_a_{NUMERO_DE_RONDAS}.json"

    # Comprobar si existe la carpeta extractors (opcional)
    if not os.path.exists("extractors"):
         print("Advertencia: Carpeta 'extractors' no encontrada.")

    # --- FASE 1: Obtener Diccionario de IDs por Ronda ---
    print(f"--- Iniciando Fase 1: Obtener IDs de partidos para {NUMERO_DE_RONDAS} rondas ---")
    round_data_dict = await scrape_round_match_ids(NUMERO_DE_RONDAS)

    if not round_data_dict or not round_data_dict.get("rounds_data"):
        print("Error: No se pudieron obtener datos de rondas. Terminando.")
        return

    # Guardar el diccionario completo de rondas (opcional, pero útil para referencia)
    try:
        with open(id_data_filename, 'w', encoding='utf-8') as f:
            json.dump(round_data_dict, f, indent=4, ensure_ascii=False)
        print(f"\nDatos de rondas (IDs, nombres) guardados en: {id_data_filename}")
    except Exception as e:
        print(f"\nAdvertencia: Error al guardar datos de rondas: {e}")

    # --- Preparación para Fase 2: Crear lista plana de IDs ---
    all_match_ids = []
    for round_name, ids_in_round in round_data_dict.get("rounds_data", {}).items():
        if ids_in_round: # Asegurarse de que la lista no esté vacía
            all_match_ids.extend(ids_in_round)

    # Eliminar duplicados si fuera posible (aunque no debería haberlos entre rondas distintas)
    all_match_ids = sorted(list(set(all_match_ids)))

    if not all_match_ids:
        print("\nNo se encontraron IDs de partidos válidos en los datos de las rondas. Terminando.")
        return

    print(f"\nTotal de IDs únicos a procesar para estadísticas: {len(all_match_ids)}")
    # print(f"IDs a procesar: {all_match_ids}") # Descomentar para ver la lista

    # --- FASE 2: Extraer Estadísticas para la Lista de IDs ---
    print(f"\n--- Iniciando Fase 2: Extracción de estadísticas detalladas ---")
    detailed_statistics_list = await extract_statistics_for_match_ids(all_match_ids)

    # --- Guardado Final ---
    if detailed_statistics_list:
        print(f"\nSe extrajeron estadísticas para {len(detailed_statistics_list)} partidos.")
        try:
            with open(final_stats_filename, 'w', encoding='utf-8') as f:
                json.dump(detailed_statistics_list, f, indent=4, ensure_ascii=False)
            print(f"Estadísticas detalladas finales guardadas en: {final_stats_filename}")
        except Exception as e:
            print(f"\nError al guardar el archivo final de estadísticas: {e}")
    else:
        print("\nNo se pudieron extraer estadísticas para ningún partido en la Fase 2.")

    print("\n--- Proceso Completo Finalizado ---")

if __name__ == "__main__":
    # Asegúrate de tener Playwright y sus navegadores instalados
    # pip install playwright
    # playwright install chromium (o playwright install)
    asyncio.run(main())