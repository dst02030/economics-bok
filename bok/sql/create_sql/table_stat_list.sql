CREATE TABLE IF NOT EXISTS bok.stat_list (
    _ts TIMESTAMPTZ NOT NULL,
    상위통계표코드 VARCHAR(10) NOT NULL,
    통계표코드 VARCHAR(10) NOT NULL,
    통계명 VARCHAR(200) NOT NULL,
    주기 VARCHAR(2),
    검색가능여부 VARCHAR(1) NOT NULL,
    출처 VARCHAR(50),
    PRIMARY KEY (통계표코드)
);
