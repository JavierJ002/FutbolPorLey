import pandas as pd
from typing import List
def create_position_df(df_player_stats, df_match_stats_full, df_shots):


    def creating_gk_performance(df_shots):

        shots_mapped = map_shot_to_area(df_shots)

        def creating_shots_metrics(shots_mapped):
            shots_converted_goal = shots_mapped[
                shots_mapped['shot_outcome'] == 'goal'
            ]
            shots_saved = shots_mapped[
                shots_mapped['shot_outcome'] == 'save'
            ]

            shots_blocked = shots_mapped[
                shots_mapped['shot_outcome'] == 'block'
            ]

            shots_on_target = shots_mapped[(
                shots_mapped['shot_outcome'] == 'goal') |
                (shots_mapped['shot_outcome'] == 'save')
            ]


            shots_per_zone = shots_mapped.groupby('shot_zone').size().reset_index(name = 'shots_per_zone')
            goals_per_zone = shots_converted_goal.groupby('shot_zone').size().reset_index(name = 'goals_per_zone')
            saves_per_zone = shots_saved.groupby('shot_zone').size().reset_index(name = 'shots_saves_per_zone')
            blocks_per_zone = shots_blocked.groupby('shot_zone').size().reset_index(name = 'shots_blocked_per_zone')
            shot_on_target_per_zone = shots_on_target.groupby('shot_zone').size().reset_index(name = 'shots_on_target_per_zone')


            area_stats = pd.DataFrame()
            area_stats['shot_zone'] = shots_per_zone['shot_zone']
            area_stats['shots_per_zone'] = shots_per_zone['shots_per_zone']
            area_stats['goals_per_zone'] = goals_per_zone['goals_per_zone']
            area_stats['ratio_goals_shot_per_zone'] = area_stats['goals_per_zone'] / area_stats['shots_per_zone']
            total_goals = area_stats['goals_per_zone'].sum()
            area_stats['ratio_goals_zone'] = area_stats['goals_per_zone'] / total_goals
            area_stats['shots_saves_per_zone'] = saves_per_zone['shots_saves_per_zone']
            area_stats['blocks_per_zone'] = blocks_per_zone['shots_blocked_per_zone']

            area_stats['ratio_saves'] = area_stats['shots_saves_per_zone'] / area_stats['shots_per_zone']
            area_stats['ratio_blocks'] = area_stats['blocks_per_zone'] / area_stats['shots_per_zone']
            area_stats['shots_on_target_per_zone'] = shot_on_target_per_zone['shots_on_target_per_zone']

            area_stats['ratio_goal_shot_on_t_per_zone'] = area_stats['goals_per_zone'] / area_stats['shots_on_target_per_zone']
            return area_stats
        
        stats_per_area = creating_shots_metrics(shots_mapped)

        def create_gk_performance_column():
            shots_on_target_only = shots_mapped[
                (shots_mapped['shot_outcome'] == 'goal') | (shots_mapped['shot_outcome'] == 'save')
            ]

            shots_on_target_only['was_goal'] = (shots_on_target_only['shot_outcome'] == 'goal').astype(int)

            shots_on_target_only = shots_on_target_only.merge(
                stats_per_area[['shot_zone', 'ratio_goal_shot_on_t_per_zone']],
                on='shot_zone', how='left'
            )

            shots_on_target_only['performance_gk'] = (
                shots_on_target_only['was_goal']
                - shots_on_target_only['ratio_goal_shot_on_t_per_zone']
            )

            performance_summary = (
                shots_on_target_only
                .groupby(['match_id', 'team_id'])['performance_gk']
                .sum()
                .reset_index()
                .rename(columns={'team_id': 'opp_team_id'})
            )

            local_stats = df_match_stats_full[df_match_stats_full['home_away'] == 'H'].copy()
            visit_stats = df_match_stats_full[df_match_stats_full['home_away'] == 'A'].copy()


            local_stats = local_stats.merge(
                performance_summary,
                on=['match_id', 'opp_team_id'],
                how='left'
            )
            visit_stats = visit_stats.merge(
                performance_summary,
                on=['match_id', 'opp_team_id'],
                how='left'
            )

            full_stats_with_perf = pd.concat([local_stats, visit_stats], ignore_index=True)

        def card_events_per_match_per_player(match_events, card_events, player_stats):
            only_card_events = match_events.loc[match_events['event_type'] == 'card']
            card_events_per_match = pd.merge(only_card_events, card_events, on='event_id', how='left')
            yellow_cards = card_events_per_match[
                card_events_per_match['card_type'] == 'yellow'
            ]

            red_cards = card_events_per_match[
                card_events_per_match['card_type'] == 'red'
            ]
            
            yellow_cards_for_player_each_game = yellow_cards.groupby(['match_id', 'player_id']).size().reset_index(name = 'yellow_cards_count')
            red_cards_for_player_each_game = red_cards.groupby(['match_id', 'player_id']).size().reset_index(name = 'red_cards_count')

            player_stats['yellow_cards'] = yellow_cards_for_player_each_game['yellow_cards_count']
            player_stats['red_cards'] = red_cards_for_player_each_game['red_cards_count']

            player_stats_filled = player_stats.fillna(0)


def filter_per_player(player_stats, position: str)-> pd.DataFrame:
    player_position = player_stats.loc[player_stats['played_position'] == position]
    #Dropping those who didnt played or didnt got a rating
    mask = player_position['sofascore_rating'].isna()
    player_position = player_position.loc[~mask]
    player_position_filled = player_position.fillna(0)
    return player_position