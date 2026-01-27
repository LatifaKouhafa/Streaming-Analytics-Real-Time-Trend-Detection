CREATE TABLE IF NOT EXISTS trends_per_minute (
  minute_start TIMESTAMP,
  minute_end   TIMESTAMP,
  keyword      TEXT,
  count        INT,
  PRIMARY KEY (minute_start, keyword)
);
