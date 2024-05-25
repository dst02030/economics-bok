CREATE TABLE IF NOT EXISTS bok.stat_search (
    _ts TIMESTAMPTZ NOT NULL,
    통계표코드 VARCHAR(8) NOT NULL,
    통계명 VARCHAR(200) NOT NULL,
    통계항목코드1 VARCHAR(20) NOT NULL,
    통계항목명1 VARCHAR(200),
    통계항목코드2 VARCHAR(20) NOT NULL,
    통계항목명2 VARCHAR(200),
    통계항목코드3 VARCHAR(20) NOT NULL,
    통계항목명3 VARCHAR(200),
    통계항목코드4 VARCHAR(20) NOT NULL,
    통계항목명4 VARCHAR(200),
    주기 VARCHAR(2) NOT NULL,
    단위 VARCHAR(200),
    가중치 VARCHAR(22),
    시점 VARCHAR(8) NOT NULL,
    값 NUMERIC(18, 2),
    PRIMARY KEY (통계표코드, 시점, 통계항목코드1, 통계항목코드2, 통계항목코드3, 통계항목코드4)
);


CREATE INDEX IF NOT EXISTS idx_stat_search1 ON bok.stat_search (통계표코드, 통계항목코드1, 주기);