-- Tabla de Torneos
CREATE TABLE tournaments (
    tournament_id BIGINT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    country_name VARCHAR(100) NULL
);

-- Tabla de Temporadas
CREATE TABLE seasons (
    season_id BIGINT PRIMARY KEY,
    tournament_id BIGINT NOT NULL REFERENCES tournaments(tournament_id),
    name VARCHAR(50) NOT NULL -- e.g., "2020/2021"
);

-- Tabla de Equipos
CREATE TABLE teams (
    team_id BIGINT PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    country VARCHAR(100) NULL
);

-- Tabla de Jugadores
CREATE TABLE players (
    player_id BIGINT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    height_cm INTEGER NULL, -- Mantenido como NULL, 0 no tiene sentido aquí
    primary_position VARCHAR(50) NULL,
    country_name VARCHAR(100) NULL
);

-- Tabla de Partidos
CREATE TABLE matches (
    match_id BIGINT PRIMARY KEY,
    season_id BIGINT NOT NULL REFERENCES seasons(season_id),
    round_number INTEGER NULL,
    match_datetime_utc TIMESTAMPTZ NOT NULL, -- Fecha/Hora crucial
    home_team_id BIGINT NOT NULL REFERENCES teams(team_id),
    away_team_id BIGINT NOT NULL REFERENCES teams(team_id),
    home_score INTEGER NULL, -- Resultado final, NULL hasta que se conozca
    away_score INTEGER NULL, -- Resultado final, NULL hasta que se conozca
    home_score_ht INTEGER NULL, -- Resultado al descanso, NULL si no disponible
    away_score_ht INTEGER NULL  -- Resultado al descanso, NULL si no disponible
);

-- Tabla de Estadísticas de Jugador por Partido (Tabla de Hechos)
CREATE TABLE player_match_stats (
    player_match_stat_id BIGSERIAL PRIMARY KEY, -- Surrogate key
    match_id BIGINT NOT NULL REFERENCES matches(match_id),
    player_id BIGINT NOT NULL REFERENCES players(player_id),
    team_id BIGINT NOT NULL REFERENCES teams(team_id), -- Equipo en este partido
    is_substitute BOOLEAN NOT NULL DEFAULT FALSE,
    played_position VARCHAR(50) NULL,
    jersey_number INTEGER NULL,
    market_value_eur_at_match INTEGER NULL, -- NULL es mejor que 0 para 'desconocido'
    sofascore_rating FLOAT NULL, -- NULL es mejor que 0 para 'no calificado'
    minutes_played INTEGER NULL DEFAULT 0, -- 0 si no jugó o dato no disponible tras inserción
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
    tackles INTEGER NULL DEFAULT 0, -- Asumiendo tackles totales o intentados
    interceptions INTEGER NULL DEFAULT 0,
    clearances INTEGER NULL DEFAULT 0,
    shots_blocked_by_player INTEGER NULL DEFAULT 0,
    dribbled_past INTEGER NULL DEFAULT 0,
    fouls_committed INTEGER NULL DEFAULT 0,
    fouls_suffered INTEGER NULL DEFAULT 0,
    -- Estadísticas de Portero (NULL si no es portero o no hay datos)
    saves INTEGER NULL DEFAULT 0,
    punches_made INTEGER NULL DEFAULT 0,
    high_claims INTEGER NULL DEFAULT 0,
    saves_inside_box INTEGER NULL DEFAULT 0,
    sweeper_keeper_successful INTEGER NULL DEFAULT 0,
    sweeper_keeper_total INTEGER NULL DEFAULT 0,

    CONSTRAINT uq_player_match UNIQUE (match_id, player_id) -- Un jugador por partido
);

-- Tabla de Estadísticas de Equipo por Partido (Tabla de Hechos)
CREATE TABLE team_match_stats (
    team_match_stat_id BIGSERIAL PRIMARY KEY, -- Surrogate key
    match_id BIGINT NOT NULL REFERENCES matches(match_id),
    team_id BIGINT NOT NULL REFERENCES teams(team_id),
    is_home_team BOOLEAN NOT NULL,
    period VARCHAR(3) NOT NULL CHECK (period IN ('ALL', '1ST', '2ND')),
    formation VARCHAR(20) NULL, -- Solo aplica a 'ALL', NULL en otros periodos
    average_team_rating FLOAT NULL, -- NULL es mejor que 0
    total_team_market_value_eur BIGINT NULL, -- NULL es mejor que 0
    possession_percentage FLOAT NULL, -- NULL si no aplica/no disponible
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
    accurate_passes_percentage FLOAT NULL, -- Si la fuente da el % directamente
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

    CONSTRAINT uq_team_match_period UNIQUE (match_id, team_id, period) -- Una entrada por equipo y periodo
);

-- --- ÍNDICES PARA MEJORAR RENDIMIENTO ---

-- Índices para `seasons`
CREATE INDEX idx_seasons_tournament_id ON seasons(tournament_id);

-- Índices para `matches` (Las FKs usualmente crean índices, pero explicitarlos es buena práctica)
CREATE INDEX idx_matches_season_id ON matches(season_id);
CREATE INDEX idx_matches_match_datetime_utc ON matches(match_datetime_utc);
CREATE INDEX idx_matches_home_team_id ON matches(home_team_id);
CREATE INDEX idx_matches_away_team_id ON matches(away_team_id);
CREATE INDEX idx_matches_season_round ON matches(season_id, round_number); -- Para buscar jornadas específicas

-- Índices para `player_match_stats` (Tabla potencialmente grande)
CREATE INDEX idx_player_match_stats_match_id ON player_match_stats(match_id);
CREATE INDEX idx_player_match_stats_player_id ON player_match_stats(player_id);
CREATE INDEX idx_player_match_stats_team_id ON player_match_stats(team_id);
CREATE INDEX idx_player_match_stats_player_match ON player_match_stats(player_id, match_id); -- Para historial de un jugador

-- Índices para `team_match_stats` (Tabla potencialmente grande)
CREATE INDEX idx_team_match_stats_match_id ON team_match_stats(match_id);
CREATE INDEX idx_team_match_stats_team_id ON team_match_stats(team_id);
CREATE INDEX idx_team_match_stats_period ON team_match_stats(period);
CREATE INDEX idx_team_match_stats_team_match ON team_match_stats(team_id, match_id); -- Para historial de un equipo
CREATE INDEX idx_team_match_stats_team_period ON team_match_stats(team_id, period); -- Para analizar rendimiento por periodo

CREATE INDEX idx_players_name ON players(name); 