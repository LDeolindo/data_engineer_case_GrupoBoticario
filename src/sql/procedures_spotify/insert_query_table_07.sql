CREATE OR REPLACE PROCEDURE `case-grupo-boticario.procedures_spotify.insert_query_table_07`()
BEGIN

  CREATE TABLE IF NOT EXISTS `spotify.spotify_tb_7`
  (
    id STRING,
    name STRING,
    description STRING,
    release_date DATE,
    duration_ms INT64,
    language STRING,
    explicit BOOL,
    type STRING
  );

  MERGE `spotify.spotify_tb_7` T
  USING (
    SELECT
      *
    FROM
      `spotify.spotify_tb_6`
    WHERE
      NAME LIKE "%Grupo Boticário%"
  ) AS S
  ON
    T.ID = S.ID
  WHEN MATCHED THEN
    UPDATE SET T.name = S.name,
      T.description = S.description,
      T.release_date = S.release_date,
      T.duration_ms = S.duration_ms,
      T.language = S.language,
      T.explicit = S.explicit,
      T.type = S.type
  WHEN NOT MATCHED THEN
    INSERT ROW
  ;
END;