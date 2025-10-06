CREATE TABLE IF NOT EXISTS bot_dbday.users (
    id SERIAL PRIMARY KEY,
    telegram_id BIGINT NOT NULL UNIQUE,
    full_name VARCHAR(255),
    birthday DATE,
    wish TEXT,
    is_admin BOOLEAN NOT NULL DEFAULT FALSE,
    ward_id INTEGER,
    giver_id INTEGER,
    registered_at TIMESTAMP NOT NULL DEFAULT NOW()
);
