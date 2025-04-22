CREATE TABLE tournaments (
    tournament_id BIGINT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    country_name VARCHAR(100) NULL
);

CREATE TABLE seasons (
    season_id BIGINT PRIMARY KEY,
    tournament_id BIGINT NOT NULL REFERENCES tournaments(tournament_id),
    name VARCHAR(50) NOT NULL 
);

CREATE TABLE teams (
    team_id BIGINT PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    country VARCHAR(100) NULL
);

CREATE TABLE players (
    player_id BIGINT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    height_cm INTEGER NULL, 
    primary_position VARCHAR(50) NULL,
    country_name VARCHAR(100) NULL
);

CREATE TABLE matches (
    match_id BIGINT PRIMARY KEY,
    season_id BIGINT NOT NULL REFERENCES seasons(season_id),
    round_number INTEGER NULL,
    round_name VARCHAR(50),
    match_datetime_utc TIMESTAMPTZ NOT NULL, 
    home_team_id BIGINT NOT NULL REFERENCES teams(team_id),
    away_team_id BIGINT NOT NULL REFERENCES teams(team_id),
    home_score INTEGER NULL, 
    away_score INTEGER NULL, 
    home_score_ht INTEGER NULL, 
    away_score_ht INTEGER NULL  
);

CREATE TABLE player_match_stats (
    player_match_stat_id BIGSERIAL PRIMARY KEY, 
    match_id BIGINT NOT NULL REFERENCES matches(match_id),
    player_id BIGINT NOT NULL REFERENCES players(player_id),
    team_id BIGINT NOT NULL REFERENCES teams(team_id), 
    is_substitute BOOLEAN NOT NULL DEFAULT FALSE,
    played_position VARCHAR(50) NULL,
    jersey_number INTEGER NULL,
    market_value_eur_at_match INTEGER NULL, 
    sofascore_rating FLOAT NULL, 
    minutes_played INTEGER NULL DEFAULT 0, 
    touches INTEGER NULL DEFAULT 0,
    goals INTEGER NOT NULL DEFAULT 0,
    assists INTEGER NOT NULL DEFAULT 0,
    own_goals INTEGER NOT NULL DEFAULT 0,
    passes_accurate INTEGER NULL DEFAULT 0,
    passes_total INTEGER NULL DEFAULT 0,
    passes_key INTEGER NULL DEFAULT 0,
    long_balls_accurate INTEGER NULL DEFAULT 0,
    long_balls_total INTEGER NULL DEFAULT 0,
    crosses_accurate INTEGER NULL DEFAULT 0,
    crosses_total INTEGER NULL DEFAULT 0,
    shots_total INTEGER NULL DEFAULT 0,
    shots_on_target INTEGER NULL DEFAULT 0,
    shots_off_target INTEGER NULL DEFAULT 0,
    shots_blocked_by_opponent INTEGER NULL DEFAULT 0,
    dribbles_successful INTEGER NULL DEFAULT 0,
    dribbles_attempts INTEGER NULL DEFAULT 0,
    possession_lost INTEGER NULL DEFAULT 0,
    dispossessed INTEGER NULL DEFAULT 0,
    duels_won INTEGER NULL DEFAULT 0,
    duels_lost INTEGER NULL DEFAULT 0,
    aerials_won INTEGER NULL DEFAULT 0,
    aerials_lost INTEGER NULL DEFAULT 0,
    ground_duels_won INTEGER NULL DEFAULT 0,
    ground_duels_total INTEGER NULL DEFAULT 0,
    tackles INTEGER NULL DEFAULT 0,
    interceptions INTEGER NULL DEFAULT 0,
    clearances INTEGER NULL DEFAULT 0,
    shots_blocked_by_player INTEGER NULL DEFAULT 0,
    dribbled_past INTEGER NULL DEFAULT 0,
    fouls_committed INTEGER NULL DEFAULT 0,
    fouls_suffered INTEGER NULL DEFAULT 0,
    saves INTEGER NULL DEFAULT 0,
    punches_made INTEGER NULL DEFAULT 0,
    high_claims INTEGER NULL DEFAULT 0,
    saves_inside_box INTEGER NULL DEFAULT 0,
    sweeper_keeper_successful INTEGER NULL DEFAULT 0,
    sweeper_keeper_total INTEGER NULL DEFAULT 0,
    goals_prevented FLOAT NULL,        
    runs_out_successful INTEGER NULL DEFAULT 0, 
    penalties_saved INTEGER NULL DEFAULT 0,  
    penalty_committed INTEGER NULL DEFAULT 0,
    expected_goals FLOAT NULL,         
    expected_assists FLOAT NULL,       
    penalty_won INTEGER NULL DEFAULT 0,      
    penalty_miss INTEGER NULL DEFAULT 0,     
    big_chances_missed INTEGER NULL DEFAULT 0,
    errors_leading_to_shot INTEGER NULL DEFAULT 0,
    big_chances_created INTEGER NULL DEFAULT 0,
    errors_leading_to_goal INTEGER NULL DEFAULT 0,

    CONSTRAINT uq_player_match UNIQUE (match_id, player_id) 
);

CREATE TABLE team_match_stats (
    team_match_stat_id BIGSERIAL PRIMARY KEY, 
    match_id BIGINT NOT NULL REFERENCES matches(match_id),
    team_id BIGINT NOT NULL REFERENCES teams(team_id),
    is_home_team BOOLEAN NOT NULL,
    period VARCHAR(3) NOT NULL CHECK (period IN ('ALL', '1ST', '2ND')),
    formation VARCHAR(20) NULL, 
    average_team_rating FLOAT NULL, 
    total_team_market_value_eur BIGINT NULL, 
    possession_percentage FLOAT NULL, 
    big_chances INTEGER NULL DEFAULT 0,
    total_shots INTEGER NULL DEFAULT 0,
    saves INTEGER NULL DEFAULT 0,
    corners INTEGER NULL DEFAULT 0,
    fouls INTEGER NULL DEFAULT 0,
    passes_successful INTEGER NULL DEFAULT 0,
    passes_total INTEGER NULL DEFAULT 0,
    passes_percentage FLOAT NULL,
    tackles_successful INTEGER NULL DEFAULT 0,
    tackles_total INTEGER NULL DEFAULT 0,
    tackles_won_percentage FLOAT NULL,
    free_kicks INTEGER NULL DEFAULT 0,
    yellow_cards INTEGER NULL DEFAULT 0,
    red_cards INTEGER NULL DEFAULT 0,
    shots_on_target INTEGER NULL DEFAULT 0,
    hit_woodwork INTEGER NULL DEFAULT 0,
    shots_off_target INTEGER NULL DEFAULT 0,
    blocked_shots INTEGER NULL DEFAULT 0,
    shots_inside_box INTEGER NULL DEFAULT 0,
    shots_outside_box INTEGER NULL DEFAULT 0,
    big_chances_missed INTEGER NULL DEFAULT 0,
    fouled_final_third INTEGER NULL DEFAULT 0,
    offsides INTEGER NULL DEFAULT 0,
    accurate_passes_percentage FLOAT NULL, 
    throw_ins INTEGER NULL DEFAULT 0,
    final_third_entries INTEGER NULL DEFAULT 0,
    long_balls_successful INTEGER NULL DEFAULT 0,
    long_balls_total INTEGER NULL DEFAULT 0,
    long_balls_percentage FLOAT NULL,
    crosses_successful INTEGER NULL DEFAULT 0,
    crosses_total INTEGER NULL DEFAULT 0,
    crosses_percentage FLOAT NULL,
    duels_won_successful INTEGER NULL DEFAULT 0,
    duels_won_total INTEGER NULL DEFAULT 0,
    duels_won_percentage FLOAT NULL,
    dispossessed INTEGER NULL DEFAULT 0,
    ground_duels_successful INTEGER NULL DEFAULT 0,
    ground_duels_total INTEGER NULL DEFAULT 0,
    ground_duels_percentage FLOAT NULL,
    aerial_duels_successful INTEGER NULL DEFAULT 0,
    aerial_duels_total INTEGER NULL DEFAULT 0,
    aerial_duels_percentage FLOAT NULL,
    dribbles_successful INTEGER NULL DEFAULT 0,
    dribbles_total INTEGER NULL DEFAULT 0,
    dribbles_percentage FLOAT NULL,
    interceptions INTEGER NULL DEFAULT 0,
    clearances INTEGER NULL DEFAULT 0,
    goal_kicks INTEGER NULL DEFAULT 0,
    expected_goals FLOAT NULL,
    touches_in_penalty_area INTEGER NULL DEFAULT 0,
    passes_in_final_third INTEGER NULL DEFAULT 0,
    recoveries INTEGER NULL DEFAULT 0,
    errors_lead_to_shot INTEGER NULL DEFAULT 0,
    goals_prevented FLOAT NULL,
    big_saves INTEGER NULL DEFAULT 0,
    errors_lead_to_goal INTEGER NULL DEFAULT 0,
    penalty_saves INTEGER NULL DEFAULT 0,
    big_chances_scored INTEGER NULL DEFAULT 0,

    CONSTRAINT uq_team_match_period UNIQUE (match_id, team_id, period) 
);

CREATE INDEX idx_seasons_tournament_id ON seasons(tournament_id);
CREATE INDEX idx_matches_season_id ON matches(season_id);
CREATE INDEX idx_matches_match_datetime_utc ON matches(match_datetime_utc);
CREATE INDEX idx_matches_home_team_id ON matches(home_team_id);
CREATE INDEX idx_matches_away_team_id ON matches(away_team_id);
CREATE INDEX idx_matches_season_round ON matches(season_id, round_number); 
CREATE INDEX idx_player_match_stats_match_id ON player_match_stats(match_id);
CREATE INDEX idx_player_match_stats_player_id ON player_match_stats(player_id);
CREATE INDEX idx_player_match_stats_team_id ON player_match_stats(team_id);
CREATE INDEX idx_player_match_stats_player_match ON player_match_stats(player_id, match_id); 
CREATE INDEX idx_team_match_stats_match_id ON team_match_stats(match_id);
CREATE INDEX idx_team_match_stats_team_id ON team_match_stats(team_id);
CREATE INDEX idx_team_match_stats_period ON team_match_stats(period);
CREATE INDEX idx_team_match_stats_team_match ON team_match_stats(team_id, match_id); 
CREATE INDEX idx_team_match_stats_team_period ON team_match_stats(team_id, period);
CREATE INDEX idx_players_name ON players(name); 




CREATE TABLE match_event_base (
    event_id BIGSERIAL PRIMARY KEY,
    match_id BIGINT NOT NULL REFERENCES matches(match_id),
    minute INTEGER NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    team_id BIGINT NOT NULL REFERENCES teams(team_id),
    player_id BIGINT NULL REFERENCES players(player_id)
);

CREATE INDEX idx_meb_match_minute ON match_event_base(match_id, minute);
CREATE INDEX idx_meb_match_id ON match_event_base(match_id);
CREATE INDEX idx_meb_team_id ON match_event_base(team_id);
CREATE INDEX idx_meb_player_id ON match_event_base(player_id);
CREATE INDEX idx_meb_event_type ON match_event_base(event_type);

CREATE TABLE goal_events (
    event_id BIGINT PRIMARY KEY REFERENCES match_event_base(event_id),
    scoring_player_id BIGINT NOT NULL REFERENCES players(player_id),
    assist_player_id BIGINT NULL REFERENCES players(player_id),
    goal_type VARCHAR(20) NULL,
    body_part VARCHAR(50) NULL
);

CREATE INDEX idx_ge_scoring_player_id ON goal_events(scoring_player_id);
CREATE INDEX idx_ge_assist_player_id ON goal_events(assist_player_id);
CREATE INDEX idx_ge_goal_type ON goal_events(goal_type);
CREATE INDEX idx_ge_body_part ON goal_events(body_part);

CREATE TABLE disallowed_goal_events (
    event_id BIGINT PRIMARY KEY REFERENCES match_event_base(event_id),
    reason VARCHAR(100) NULL
);

CREATE TABLE substitution_events (
    event_id BIGINT PRIMARY KEY REFERENCES match_event_base(event_id),
    player_in_id BIGINT NOT NULL REFERENCES players(player_id),
    player_out_id BIGINT NOT NULL REFERENCES players(player_id)
);

CREATE INDEX idx_se_player_in_id ON substitution_events(player_in_id);
CREATE INDEX idx_se_player_out_id ON substitution_events(player_out_id);

CREATE TABLE card_events (
    event_id BIGINT PRIMARY KEY REFERENCES match_event_base(event_id),
    card_type VARCHAR(20) NOT NULL,
    reason VARCHAR(100) NULL,
    is_rescinded BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE missed_penalty_events (
    event_id BIGINT PRIMARY KEY REFERENCES match_event_base(event_id),
    outcome VARCHAR(50) NULL
);

CREATE TABLE var_decision_events (
    event_id BIGINT PRIMARY KEY REFERENCES match_event_base(event_id),
    decision_type VARCHAR(50) NULL,
    decision_outcome VARCHAR(50) NULL,
    incident_class_reviewed VARCHAR(50) NULL
);

CREATE INDEX idx_vde_decision_type ON var_decision_events(decision_type);
CREATE INDEX idx_vde_decision_outcome ON var_decision_events(decision_outcome);



CREATE TABLE shot_events (
    event_id BIGINT PRIMARY KEY REFERENCES match_event_base(event_id),
    shooter_player_id BIGINT NOT NULL REFERENCES players(player_id),
    shot_outcome VARCHAR(50) NOT NULL,
    situation VARCHAR(50) NULL,
    body_part VARCHAR(50) NULL,
    xg FLOAT NULL,
    xgot FLOAT NULL,
    player_coord_x FLOAT NULL,
    player_coord_y FLOAT NULL,
    goal_mouth_location VARCHAR(50) NULL,
    goal_mouth_coord_x FLOAT NULL,
    goal_mouth_coord_y FLOAT NULL,
    goal_mouth_coord_z FLOAT NULL,
    block_coord_x FLOAT NULL,
    block_coord_y FLOAT NULL,
    goalkeeper_id BIGINT NULL REFERENCES players(player_id),
    added_time INTEGER NULL
);

CREATE INDEX idx_she_shooter_player_id ON shot_events(shooter_player_id);
CREATE INDEX idx_she_shot_outcome ON shot_events(shot_outcome);
CREATE INDEX idx_she_situation ON shot_events(situation);
CREATE INDEX idx_she_body_part ON shot_events(body_part);
CREATE INDEX idx_she_goalkeeper_id ON shot_events(goalkeeper_id);

SET client_encoding = 'UTF8';
