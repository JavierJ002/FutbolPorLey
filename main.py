# main.py
import json
import os
import asyncio

# Importar las tres funciones principales
from extractors.id_extractor import scrape_round_match_ids
from extractors.statistics_extractor import extract_statistics_for_match_ids 
from extractors.players_statistics_extractor import extract_player_stats_for_match_ids 

async def main():
    NUMERO_DE_RONDAS = 1
    id_data_filename = f"sofascore_ROUND_DATA_LALIGA_20_21_rondas_1_a_{NUMERO_DE_RONDAS}.json"
    team_stats_filename = f"sofascore_TEAM_STATS_LALIGA_20_21_rondas_1_a_{NUMERO_DE_RONDAS}.json"
    player_stats_filename = f"sofascore_PLAYER_STATS_LALIGA_20_21_rondas_1_a_{NUMERO_DE_RONDAS}.json"

    if not os.path.exists("extractors"): print("Advertencia: Carpeta 'extractors' no encontrada.")

    # --- FASE 1: Obtener Diccionario de IDs por Ronda ---
    print(f"\n--- Iniciando Fase 1: Obtener IDs ({NUMERO_DE_RONDAS} rondas) ---")
    round_data_dict = await scrape_round_match_ids(NUMERO_DE_RONDAS)
    if not round_data_dict or not round_data_dict.get("rounds_data"):
        print("Error: No se pudieron obtener datos de rondas. Terminando.")
        return
    try: # Guardado opcional
        with open(id_data_filename, 'w', encoding='utf-8') as f:
            json.dump(round_data_dict, f, indent=4, ensure_ascii=False)
        print(f"-> Datos de rondas guardados en: {id_data_filename}")
    except Exception as e: print(f"Advertencia: Error al guardar datos de rondas: {e}")

    # --- Preparación: Crear lista plana de IDs ---
    all_match_ids = sorted(list(set(
        id_ for ids_in_round in round_data_dict.get("rounds_data", {}).values() for id_ in ids_in_round if id_
    )))
    if not all_match_ids:
        print("\nNo se encontraron IDs de partidos válidos. Terminando.")
        return
    print(f"\nTotal de IDs únicos a procesar: {len(all_match_ids)}")

    # --- FASE 2: Extraer Estadísticas de EQUIPO (/statistics) ---
    print(f"\n--- Iniciando Fase 2: Extracción de estadísticas de Equipo ---")
    team_statistics_list = await extract_statistics_for_match_ids(all_match_ids)
    if team_statistics_list:
        print(f"-> Se extrajeron estadísticas de equipo para {len(team_statistics_list)} partidos.")
        try:
            with open(team_stats_filename, 'w', encoding='utf-8') as f:
                json.dump(team_statistics_list, f, indent=4, ensure_ascii=False)
            print(f"-> Estadísticas de equipo guardadas en: {team_stats_filename}")
        except Exception as e: print(f"Error al guardar estadísticas de equipo: {e}")
    else:
        print("-> No se pudieron extraer estadísticas de equipo.")

    # --- FASE 3: Extraer Estadísticas de JUGADOR (/lineups) ---
    print(f"\n--- Iniciando Fase 3: Extracción de estadísticas de Jugador ---")
    player_statistics_list = await extract_player_stats_for_match_ids(all_match_ids)
    if player_statistics_list:
        print(f"-> Se extrajeron alineaciones/estadísticas de jugador para {len(player_statistics_list)} partidos.")
        try:
            with open(player_stats_filename, 'w', encoding='utf-8') as f:
                json.dump(player_statistics_list, f, indent=4, ensure_ascii=False)
            print(f"-> Estadísticas de jugador guardadas en: {player_stats_filename}")
        except Exception as e: print(f"Error al guardar estadísticas de jugador: {e}")
    else:
        print("-> No se pudieron extraer estadísticas de jugador.")

    print("\n--- Proceso Completo Finalizado ---")

if __name__ == "__main__":
    asyncio.run(main())