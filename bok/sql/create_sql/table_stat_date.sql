CREATE TABLE IF NOT EXISTS bok.stat_date (
    _ts TIMESTAMPTZ NOT NULL,
    카테고리 VARCHAR NOT NULL,
    발표월 VARCHAR NOT NULL,
    통계명 VARCHAR NOT NULL,
    공표일 TIMESTAMPTZ NOT NULL,
    정보 VARCHAR,
    PRIMARY KEY (통계명, 발표월)
);
