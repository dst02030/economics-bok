CREATE TABLE IF NOT EXISTS bok.stat_item (
    _ts TIMESTAMPTZ NOT NULL,
    통계표코드 VARCHAR(8) NOT NULL,
    통계명 VARCHAR(200) NOT NULL,
    항목그룹코드 VARCHAR(20) NOT NULL,
    항목그룹명 VARCHAR(60) NOT NULL,
    통계항목코드 VARCHAR(20) NOT NULL,
    통계항목명 VARCHAR(200) NOT NULL,
    상위통계항목코드 VARCHAR(20),
    상위통계항목명 VARCHAR(200),
    주기 VARCHAR(2),
    수록시작일자 VARCHAR(8) NOT NULL,
    수록종료일자 VARCHAR(8) NOT NULL,
    자료수 INTEGER NOT NULL,
    단위 VARCHAR(200),
    가중치 VARCHAR(22),
    PRIMARY KEY (통계표코드, 항목그룹코드, 통계항목코드, 주기)
);
