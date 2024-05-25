CREATE TABLE IF NOT EXISTS bok.stat_meta (
    _ts TIMESTAMPTZ NOT NULL,
    인덱스 CHAR(6),
    레벨 VARCHAR(2) NOT NULL,
    상위통계항목코드 VARCHAR(10),
    데이터명 VARCHAR(200) NOT NULL,
    영문데이터명 VARCHAR(200) NOT NULL,
    통계항목코드 VARCHAR(10) NOT NULL,
    통계항목명 VARCHAR(200) NOT NULL,
    메타데이터 TEXT,
    입력날짜 TIMESTAMP NOT NULL,
    수정날짜 TIMESTAMP NOT NULL,
    메타데이터_key VARCHAR(10) NOT NULL,
    PRIMARY KEY (레벨, 데이터명, 통계항목코드, 메타데이터_key)
);
