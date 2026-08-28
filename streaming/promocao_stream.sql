-- Promoção dos eventos de streaming pelo medalhão, via views LIVE (near real-time).
-- Diferente do batch (PySpark agendado), aqui usamos views: refletem cada evento
-- novo na hora, sem job nem custo ocioso.

-- ============================================================
-- SILVER: faz o parse do JSON cru da bronze de streaming em colunas tipadas
-- ============================================================
CREATE OR REPLACE VIEW `tech-challenge-fase-2-505123.silver.medicoes_stream` AS
SELECT
  JSON_VALUE(data, '$.evento_id')                          AS evento_id,
  TIMESTAMP(JSON_VALUE(data, '$.event_timestamp'))         AS event_timestamp,
  JSON_VALUE(data, '$.tipo_evento')                        AS tipo_evento,
  CAST(JSON_VALUE(data, '$.ano') AS INT64)                 AS ano,
  LPAD(JSON_VALUE(data, '$.id_municipio'), 7, '0')         AS id_municipio,
  JSON_VALUE(data, '$.rede')                               AS rede,
  CAST(JSON_VALUE(data, '$.taxa_alfabetizacao') AS FLOAT64) AS taxa_alfabetizacao,
  CAST(JSON_VALUE(data, '$.media_portugues')  AS FLOAT64)  AS media_portugues,
  JSON_VALUE(data, '$.fonte')                              AS fonte,
  publish_time                                             AS _ingestao_stream
FROM `tech-challenge-fase-2-505123.bronze.eventos_stream`;

-- ============================================================
-- GOLD: última medição por município/rede/ano, enriquecida com a dim_municipio
-- (a camada analítica recebendo o dado de streaming, ao vivo)
-- ============================================================
CREATE OR REPLACE VIEW `tech-challenge-fase-2-505123.gold.indicador_stream_tempo_real` AS
WITH ultima AS (
  SELECT *, ROW_NUMBER() OVER (
      PARTITION BY id_municipio, rede, ano
      ORDER BY event_timestamp DESC) AS rn
  FROM `tech-challenge-fase-2-505123.silver.medicoes_stream`
)
SELECT
  m.ano, m.id_municipio, dm.nome_municipio, dm.sigla_uf, dm.nome_regiao,
  m.rede, m.taxa_alfabetizacao, m.media_portugues, m.event_timestamp
FROM ultima m
LEFT JOIN `tech-challenge-fase-2-505123.gold.dim_municipio` dm USING (id_municipio)
WHERE m.rn = 1;
