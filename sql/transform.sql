MERGE `INTEGRATION.integration_prueba_tecnica` AS T
USING (
  SELECT
    id,
    userId,
    title,
    body,
    CURRENT_DATE() AS processing_date
  FROM `SANDBOX_prueba_alten.api_data`
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY id
    ORDER BY id
  ) = 1
) AS S
ON T.id = S.id

WHEN MATCHED
 AND (
   T.title != S.title
   OR T.body  != S.body
 ) THEN
  UPDATE SET
    userId = S.userId,
    title = S.title,
    body = S.body,
    processing_date = S.processing_date

WHEN NOT MATCHED THEN
  INSERT (
    id,
    userId,
    title,
    body,
    processing_date
  )
  VALUES (
    S.id,
    S.userId,
    S.title,
    S.body,
    S.processing_date
  );
