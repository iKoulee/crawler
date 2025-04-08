-- Migration script to update the advertisements table structure
-- 1. Add filename column
-- 2. Remove harvest_date column
-- 3. Rename html_status to http_status

-- Start a transaction to ensure all changes are applied together
BEGIN TRANSACTION;

-- 1. Add text column for file name
ALTER TABLE advertisements ADD COLUMN filename TEXT;

-- 2. Create a temporary table without the harvest_date column
CREATE TABLE advertisements_backup (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    title TEXT,
    description TEXT,
    company TEXT,
    location TEXT,
    url TEXT NOT NULL UNIQUE,
    html_body TEXT NOT NULL,
    http_status INTEGER NOT NULL,  -- Using the new name here
    ad_type TEXT NOT NULL,
    filename TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Copy data from original table to the temporary table
-- Note: html_status is copied to http_status during this step
INSERT INTO advertisements_backup (
    id, title, description, company, location, 
    url, html_body, http_status, ad_type, 
    filename, created_at
)
SELECT 
    id, title, description, company, location, 
    url, html_body, html_status, ad_type, 
    filename, created_at
FROM advertisements;

-- 4. Drop the original table
DROP TABLE advertisements;

-- 5. Rename the temporary table to the original table name
ALTER TABLE advertisements_backup RENAME TO advertisements;

-- 6. Recreate any indexes or constraints that were on the original table
CREATE INDEX IF NOT EXISTS idx_advertisements_url ON advertisements(url);
CREATE INDEX IF NOT EXISTS idx_advertisements_ad_type ON advertisements(ad_type);

-- Commit the transaction
COMMIT;

-- Verify the changes
PRAGMA table_info(advertisements);
