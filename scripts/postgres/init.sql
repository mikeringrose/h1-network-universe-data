-- Enable PostGIS for spatial types and functions.
-- Runs automatically on first DB init (empty data dir). For an existing DB, run manually:
--   psql -U user -d pipeline -c "CREATE EXTENSION IF NOT EXISTS postgis;"
CREATE EXTENSION IF NOT EXISTS postgis;
