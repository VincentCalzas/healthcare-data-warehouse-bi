USE DATABASE TCH;
USE SCHEMA public;

CREATE TABLE IF NOT EXISTS T_SUIV_RUN(
    RUN_ID INTEGER NOT NULL PRIMARY KEY COMMENT 'Identifiant du run',
    RUN_STRT_DTTM TIMESTAMP(0) NOT NULL COMMENT 'Date de début d éxécution du run',
    RUN_END_DTTM TIMESTAMP(0) COMMENT 'Date de fin d éxécution du run',
    RUN_STTS_CD VARCHAR(10) NOT NULL COMMENT 'Code statut du run'
);

CREATE TABLE IF NOT EXISTS T_SUIV_TRMT(
    RUN_ID INTEGER NOT NULL COMMENT 'Identifiant du run',
    EXEC_ID INTEGER NOT NULL PRIMARY KEY COMMENT 'Identifiant d exécution du traitement',
    SCRPT_NAME VARCHAR(250) NOT NULL COMMENT 'Nom du script exécuté',
    EXEC_STRT_DTTM TIMESTAMP(0) NOT NULL COMMENT 'Date de début d éxécution du script',
    EXEC_END_DTTM TIMESTAMP(0) COMMENT 'Date de fin d éxécution du script',
    EXEC_STTS_CD VARCHAR(10) NOT NULL COMMENT 'Code statut de l exécution',
    FOREIGN KEY (RUN_ID) REFERENCES T_SUIV_RUN(RUN_ID)
);