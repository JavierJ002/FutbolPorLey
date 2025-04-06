# main.py (Original - Guarda en JSON y fusiona)
import json
import os
import asyncio

# Importar las tres funciones principales con sus nombres originales
from extractors.id_extractor import scrape_round_match_ids
from extractors.statistics_extractor import extract_statistics_for_match_ids
from extractors.players_statistics_extractor import extract_player_stats_for_match_ids

async def main():
    NUMERO_DE_RONDAS = 1 # Ajustar según necesidad
    # Nombres de archivo para los JSON
    id_data_filename = f"sofascore_ROUND_DATA_LALIGA_20_21_rondas_1_a_{NUMERO_DE_RONDAS}.json"
    # Archivo temporal para stats de equipo ANTES de fusionar
    temp_team_stats_filename = f"temp_sofascore_TEAM_STATS_LALIGA_20_21_rondas_1_a_{NUMERO_DE_RONDAS}.json"
    # Archivo para stats de jugador
    player_stats_filename = f"sofascore_PLAYER_STATS_LALIGA_20_21_rondas_1_a_{NUMERO_DE_RONDAS}.json"
    # Archivo FINAL para stats de equipo DESPUÉS de fusionar
    final_team_stats_filename = f"sofascore_TEAM_STATS_MERGED_LALIGA_20_21_rondas_1_a_{NUMERO_DE_RONDAS}.json"


    output_dir = "extracted_data" # Carpeta para guardar los JSON
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Carpeta '{output_dir}' creada.")

    # Rutas completas a los archivos
    id_data_filepath = os.path.join(output_dir, id_data_filename)
    temp_team_stats_filepath = os.path.join(output_dir, temp_team_stats_filename)
    player_stats_filepath = os.path.join(output_dir, player_stats_filename)
    final_team_stats_filepath = os.path.join(output_dir, final_team_stats_filename)


    if not os.path.exists("extractors"): print("Advertencia: Carpeta 'extractors' no encontrada.")

    # --- FASE 1: Obtener Diccionario de IDs por Ronda ---
    print(f"\n--- Iniciando Fase 1: Obtener IDs ({NUMERO_DE_RONDAS} rondas) ---")
    round_data_dict = await scrape_round_match_ids(NUMERO_DE_RONDAS)

    if not round_data_dict or not round_data_dict.get("rounds_data"):
        print("Error: No se pudieron obtener datos de rondas. Terminando.")
        return

    # Guardar datos de IDs
    try:
        with open(id_data_filepath, 'w', encoding='utf-8') as f:
            json.dump(round_data_dict, f, indent=4, ensure_ascii=False)
        print(f"-> Datos de rondas guardados en: {id_data_filepath}")
    except Exception as e:
        print(f"Error crítico al guardar datos de rondas: {e}. Terminando.")
        return

    # --- Preparación: Crear lista plana de IDs ---
    all_match_ids = sorted(list(set(
        match_id # Usar match_id directamente
        for round_name, ids_in_round in round_data_dict.get("rounds_data", {}).items()
        for match_id in ids_in_round if match_id # Asegurarse que el ID no sea None o vacío
    )))

    if not all_match_ids:
        print("\nNo se encontraron IDs de partidos válidos en los datos de rondas. Terminando.")
        return
    print(f"\nTotal de IDs únicos a procesar: {len(all_match_ids)}")
    # print(f"IDs: {all_match_ids}") # Descomentar para ver IDs

    # --- FASE 2: Extraer Estadísticas de EQUIPO (/statistics) ---
    print(f"\n--- Iniciando Fase 2: Extracción de estadísticas de Equipo ---")
    # Llamar a la función original
    team_statistics_list = await extract_statistics_for_match_ids(all_match_ids)

    if not team_statistics_list:
         print("Advertencia: No se pudieron extraer estadísticas de equipo base. La fusión podría fallar.")
         # Guardar archivo vacío o con error si se desea
         try:
             with open(temp_team_stats_filepath, 'w', encoding='utf-8') as f:
                 json.dump([], f, indent=4, ensure_ascii=False)
             print(f"-> Archivo temporal de stats de equipo vacío guardado en: {temp_team_stats_filepath}")
         except Exception as e: print(f"Error al guardar archivo temporal vacío: {e}")
         # Podrías decidir terminar aquí si las stats de equipo son cruciales: return
    else:
        print(f"-> Se extrajeron estadísticas base de equipo para {len(team_statistics_list)} partidos.")
        # Guardar temporalmente ANTES de la fusión
        try:
            with open(temp_team_stats_filepath, 'w', encoding='utf-8') as f:
                json.dump(team_statistics_list, f, indent=4, ensure_ascii=False)
            print(f"-> Estadísticas de equipo TEMPORALES guardadas en: {temp_team_stats_filepath}")
        except Exception as e:
            print(f"Error crítico al guardar stats de equipo temporales: {e}. Terminando.")
            return


    # --- FASE 3: Extraer Estadísticas de JUGADOR (/lineups) ---
    print(f"\n--- Iniciando Fase 3: Extracción de estadísticas de Jugador ---")
    # Llamar a la función original
    player_statistics_list = await extract_player_stats_for_match_ids(all_match_ids)

    if player_statistics_list:
        print(f"-> Se extrajeron alineaciones/estadísticas de jugador para {len(player_statistics_list)} partidos.")
        # Guardar stats de jugador
        try:
            with open(player_stats_filepath, 'w', encoding='utf-8') as f:
                json.dump(player_statistics_list, f, indent=4, ensure_ascii=False)
            print(f"-> Estadísticas de jugador guardadas en: {player_stats_filepath}")
        except Exception as e: print(f"Error al guardar estadísticas de jugador: {e}")
    else:
        print("Advertencia: No se pudieron extraer estadísticas de jugador. No se realizará la fusión.")
        # Si no hay stats de jugador, la fusión no es posible.
        # Puedes renombrar el archivo temporal a final o simplemente no hacer nada más.
        try:
             if os.path.exists(temp_team_stats_filepath):
                  os.rename(temp_team_stats_filepath, final_team_stats_filepath)
                  print(f"-> Stats de equipo (sin fusionar) guardadas como finales en: {final_team_stats_filepath}")
        except Exception as ren_err:
             print(f"Error renombrando archivo temporal: {ren_err}")
        return # Terminar si no hay stats de jugador


    # --- FASE 4: MERGE Player Info (Rating, Market Value) into Team Stats ---
    # Solo proceder si tenemos ambas listas
    if team_statistics_list and player_statistics_list:
        print(f"\n--- Iniciando Fase 4: Fusionando datos de jugador en estadísticas de equipo ---")
        merged_count = 0
        # Crear un lookup para acceso rápido a los datos de jugador por match_id
        player_stats_lookup = {item['match_id']: item.get('lineup_data', {}) # Usar .get para seguridad
                               for item in player_statistics_list
                               if 'match_id' in item and isinstance(item.get('lineup_data'), dict) and 'error' not in item['lineup_data']}

        # Iterar sobre la lista de estadísticas de equipo (la que se guardó temporalmente)
        for team_stat_item in team_statistics_list:
            match_id = team_stat_item.get('match_id')
            # Saltar si falta el ID o si las stats son un error
            if not match_id or (isinstance(team_stat_item.get('statistics'), dict) and 'error' in team_stat_item['statistics']):
                continue

            # Buscar los datos de jugador para este partido
            player_data_for_match = player_stats_lookup.get(match_id)

            if player_data_for_match:
                # Extraer la info agregada del equipo desde los datos del jugador
                home_info = player_data_for_match.get('home_team_info', {})
                away_info = player_data_for_match.get('away_team_info', {})

                # Obtener el diccionario de estadísticas del partido completo ('ALL')
                # Es importante la estructura devuelta por _parse_statistics_data
                stats_all_period = team_stat_item.get('statistics', {}).get('ALL', {})
                if not stats_all_period: # Saltar si no hay periodo 'ALL'
                     print(f"    Advertencia: No se encontró periodo 'ALL' en stats para Match ID {match_id}.")
                     continue

                stats_all_home = stats_all_period.get('home', {})
                stats_all_away = stats_all_period.get('away', {})

                # Añadir los campos fusionados si existen los diccionarios home/away
                if stats_all_home is not None: # Debe ser un dict, no None
                    # Usar las claves EXACTAS calculadas en _parse_player_lineup_data
                    stats_all_home['average_team_rating'] = home_info.get('sofascore_rating_avg')
                    stats_all_home['total_team_market_value'] = home_info.get('total_market_value_eur')
                    # Añadir formación si se desea
                    stats_all_home['formation'] = home_info.get('formation')


                if stats_all_away is not None:
                    stats_all_away['average_team_rating'] = away_info.get('sofascore_rating_avg')
                    stats_all_away['total_team_market_value'] = away_info.get('total_market_value_eur')
                    # Añadir formación si se desea
                    stats_all_away['formation'] = away_info.get('formation')


                # No es estrictamente necesario reasignar si modificaste el dict in-place, pero es más seguro
                # team_stat_item['statistics']['ALL']['home'] = stats_all_home
                # team_stat_item['statistics']['ALL']['away'] = stats_all_away
                merged_count += 1
            else:
                print(f"    Advertencia: No se encontraron datos de jugador válidos para Match ID {match_id} durante la fusión.")

        print(f"-> Datos fusionados para {merged_count} partidos.")

    else:
        print("-> No se realizó la fusión: faltan datos de equipo temporales o de jugador.")


    # --- FASE 5: Save Final Merged Team Stats ---
    print(f"\n--- Iniciando Fase 5: Guardando estadísticas de equipo finales (fusionadas) ---")
    if team_statistics_list: # Guardar la lista modificada (o la original si no hubo fusión)
        try:
            with open(final_team_stats_filepath, 'w', encoding='utf-8') as f:
                # Guardar la lista team_statistics_list que ahora contiene los datos fusionados
                json.dump(team_statistics_list, f, indent=4, ensure_ascii=False)
            print(f"-> Estadísticas de equipo FINALES (fusionadas) guardadas en: {final_team_stats_filepath}")
            # Opcional: borrar el archivo temporal
            try:
                 if os.path.exists(temp_team_stats_filepath):
                      os.remove(temp_team_stats_filepath)
                      print(f"-> Archivo temporal '{temp_team_stats_filepath}' eliminado.")
            except Exception as del_err:
                 print(f"Advertencia: No se pudo eliminar el archivo temporal: {del_err}")
        except Exception as e:
            print(f"Error al guardar estadísticas de equipo finales: {e}")
    else:
        print("-> No hay estadísticas de equipo para guardar como finales.")


    print("\n--- Proceso Completo Finalizado ---")

if __name__ == "__main__":
    # Para ejecutar el script asíncrono principal
    # asyncio.run() es la forma estándar en Python 3.7+
    asyncio.run(main())