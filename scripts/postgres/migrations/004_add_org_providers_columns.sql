ALTER TABLE org_providers
    ADD COLUMN IF NOT EXISTS specialty                      TEXT,
    ADD COLUMN IF NOT EXISTS accepts_new_patients           BOOLEAN,
    ADD COLUMN IF NOT EXISTS uses_cms_ma_contract_amendment BOOLEAN,
    ADD COLUMN IF NOT EXISTS letter_of_intent               BOOLEAN,
    ADD COLUMN IF NOT EXISTS accuracy_confidence            TEXT;
