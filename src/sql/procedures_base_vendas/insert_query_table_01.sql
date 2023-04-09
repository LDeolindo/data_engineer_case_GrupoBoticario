CREATE OR REPLACE PROCEDURE `case-grupo-boticario.procedures_base_vendas.insert_query_table_01`()
BEGIN

  CREATE TABLE IF NOT EXISTS `refined_zone_base_vendas.r_base_vendas_tb_1`
  (
    ID INT64,
    QTD_VENDA INT64,
    MES_VENDA INT64,
    ANO_VENDA INT64
  );

  MERGE `refined_zone_base_vendas.r_base_vendas_tb_1` T
  USING (
    SELECT
      CAST(FORMAT_DATE('%m%Y', DATA_VENDA) AS INT64) AS ID
      , SUM(QTD_VENDA) AS QTD_VENDA
      , CAST(FORMAT_DATE('%m', DATA_VENDA) AS INT64) AS MES_VENDA
      , CAST(FORMAT_DATE('%Y', DATA_VENDA) AS INT64) AS ANO_VENDA
    FROM
      `processing_zone_base_vendas.p_base_vendas`
    GROUP BY
      1, 3, 4
  ) AS S
  ON
    T.ID = S.ID
  WHEN MATCHED THEN
    UPDATE SET T.QTD_VENDA = S.QTD_VENDA
  WHEN NOT MATCHED THEN
    INSERT ROW
  ;
END;