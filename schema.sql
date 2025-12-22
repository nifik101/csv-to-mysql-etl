-- Users table
-- Lagrar användarinformation extraherad från Agent-kolumnen
CREATE TABLE users (
    user_id INT PRIMARY KEY,
    namn VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Daily Performance table
-- Lagrar dagliga prestationsdata för användare
-- PRIMARY KEY på (user_id, datum) för att tillåta UPSERT per dag
CREATE TABLE daily_performance (
    user_id INT NOT NULL,
    datum DATE NOT NULL,
    samtal INT,
    acd_seconds INT,
    acw_seconds INT,
    hold_seconds INT,
    koppling_pct DECIMAL(5,2),
    bb_antal INT,
    pp_antal INT,
    tv_antal INT,
    mbb_antal INT,
    other_antal INT,
    erbjud_pct DECIMAL(5,2),
    save_provis_kr DECIMAL(10,2),
    provis_kr DECIMAL(10,2),
    fmc_prov_kr DECIMAL(10,2),
    value_change_kr DECIMAL(10,2),
    PRIMARY KEY (user_id, datum),
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
    INDEX idx_datum (datum)
);

-- Daily Retention table
-- Lagrar dagliga retention-data för användare
CREATE TABLE daily_retention (
    user_id INT NOT NULL,
    datum DATE NOT NULL,
    vand_tv_pct DECIMAL(5,2),
    vand_bb_pct DECIMAL(5,2),
    vand_pp_pct DECIMAL(5,2),
    vand_total_pct DECIMAL(5,2),
    vand_antal INT,
    PRIMARY KEY (user_id, datum),
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
    INDEX idx_datum (datum)
);

-- Daily NPS table
-- Lagrar dagliga NPS-data för användare
-- NPS kan vara negativ, inga CHECK constraints
CREATE TABLE daily_nps (
    user_id INT NOT NULL,
    datum DATE NOT NULL,
    nps_antal_svar INT,
    nps_score DECIMAL(5,2),
    csat_pct DECIMAL(5,2),
    cb_pct DECIMAL(5,2),
    PRIMARY KEY (user_id, datum),
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
    INDEX idx_datum (datum)
);
