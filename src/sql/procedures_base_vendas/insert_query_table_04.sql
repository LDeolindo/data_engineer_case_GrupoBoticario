CREATE OR REPLACE PROCEDURE `case-grupo-boticario.procedures_base_vendas.insert_query_table_04`()
BEGIN

  CREATE TABLE IF NOT EXISTS `refined_zone_base_vendas.r_base_vendas_tb_4`
  (
    ID INT64,
    ID_LINHA INT64,
    LINHA STRING,
    MES_VENDA INT64,
    ANO_VENDA INT64,
    QTD_VENDA INT64
  );

  MERGE `refined_zone_base_vendas.r_base_vendas_tb_4` T
  USING (
    SELECT
      CAST(CONCAT(ID_LINHA, FORMAT_DATE('%m%Y', DATA_VENDA)) AS INT64) AS ID
      , ID_LINHA
      , LINHA
      , CAST(FORMAT_DATE('%m', DATA_VENDA) AS INT64) AS MES_VENDA
      , CAST(FORMAT_DATE('%Y', DATA_VENDA) AS INT64) AS ANO_VENDA
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