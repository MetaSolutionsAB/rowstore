-- One-time maintenance tasks that RowStore runs at startup (e.g., the cleanup of column names, ETL-11). A task is
-- recorded once a run of it has completed, so that later startups skip it. Deleting a row makes the task run again.
CREATE TABLE IF NOT EXISTS maintenance_tasks (
    name TEXT PRIMARY KEY,
    completed TIMESTAMP NOT NULL
);
