CREATE TABLE incidents (
    key TEXT PRIMARY KEY NOT NULL,
    source TEXT NOT NULL,
    active INTEGER NOT NULL CHECK (active IN (0, 1)),
    last_notice INTEGER NOT NULL,
    summary TEXT NOT NULL,
    details TEXT NOT NULL
);
CREATE TABLE queue (
    sequence INTEGER PRIMARY KEY,
    id TEXT NOT NULL UNIQUE,
    payload TEXT NOT NULL,
    created INTEGER NOT NULL,
    due INTEGER NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'sent', 'failed')),
    error TEXT
);
CREATE INDEX queue_pending ON queue(state, sequence);
CREATE TABLE samples (
    key TEXT PRIMARY KEY NOT NULL,
    since INTEGER NOT NULL,
    last INTEGER NOT NULL,
    successes INTEGER NOT NULL CHECK (successes >= 0)
);
PRAGMA user_version = 2;
