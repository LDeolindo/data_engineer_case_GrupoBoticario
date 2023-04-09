CREATE OR REPLACE PROCEDURE `case-grupo-boticario.procedures_base_vendas.insert_query_table_02`()
BEGIN

  CREATE TABLE IF NOT EXISTS `refined_zone_base_vendas.r_base_vendas_tb_2`
  (
    ID INT64,
    ID_LINHA INT64,
    LINHA STRING,
    ID_MARCA INT64,
    MARCA STRING,
    QTD_VENDA INT64
  );

  MERGE `refined_zone_base_vendas.r_base_vendas_tb_2` T
  USING (
    SELECT
      CAST(CONCAT(ID_LINHA, ID_MARCA) AS INT64) AS ID
      , ID_LINHA
      , LINHA
      , ID_MARCA
      , MARCA
      , SUM(QTD_VENDA) AS QTD_VENDA
    FROM
      `processing_zone_base_vendas.p_base_vendas`
    GROUP BY
      1, 2, 3, 4, 5
  ) AS S
  ON
    T.ID = S.ID
  WHEN MATCHED THEN
    UPDATE SET T.QTD_VENDA = S.QTD_VENDA
  WHEN NOT MATCHED THEN
    INSERT ROW
  ;
END;