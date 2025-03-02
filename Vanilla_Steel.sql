CREATE OR REPLACE EXTERNAL TABLE `supplier_data1_staging`
OPTIONS (
  format = 'CSV',
  uris = ['gs:/supplier_data1.csv'],
  skip_leading_rows = 1
);

CREATE OR REPLACE EXTERNAL TABLE `supplier_data2_staging`
OPTIONS (
  format = 'CSV',
  uris = ['gs:/supplier_data2.csv'],
  skip_leading_rows = 1
);

CREATE OR REPLACE EXTERNAL TABLE `buyer_preferences_staging`
OPTIONS (
  format = 'CSV',
  uris = ['gs:/buyer_preferences.csv'],
  skip_leading_rows = 1
);


CREATE OR REPLACE TABLE `supplier_data1_cleaned` AS
SELECT
  SAFE_CAST(NULLIF(`Quality/Choice`, '') AS STRING) AS quality_level,
  SAFE_CAST(NULLIF(`Grade`, '') AS STRING) AS material_grade,
  SAFE_CAST(NULLIF(`Finish`, '') AS STRING) AS surface_treatment,
  SAFE_CAST(NULLIF(`Thickness (mm)`, '') AS FLOAT64) AS thickness_mm,
  SAFE_CAST(NULLIF(`Width (mm)`, '') AS FLOAT64) AS width_mm,
  SAFE_CAST(NULLIF(`Description`, '') AS STRING) AS description,
  SAFE_CAST(NULLIF(`Gross weight (kg)`, '') AS FLOAT64) AS weight_kg,
  SAFE_CAST(NULLIF(`Quantity`, '') AS INT64) AS quantity
FROM `supplier_data1_staging`;

CREATE OR REPLACE TABLE `supplier_data2_cleaned` AS
SELECT
  SAFE_CAST(NULLIF(`Material`, '') AS STRING) AS material_grade,
  SAFE_CAST(NULLIF(`Description`, '') AS STRING) AS description,
  SAFE_CAST(NULLIF(`Article ID`, '') AS STRING) AS article_id,
  SAFE_CAST(NULLIF(`Weight (kg)`, '') AS FLOAT64) AS weight_kg,
  SAFE_CAST(NULLIF(`Quantity`, '') AS INT64) AS quantity,
  SAFE_CAST(NULLIF(`Reserved`, '') AS STRING) AS reserved
FROM `supplier_data2_staging`;

CREATE OR REPLACE TABLE `supplier_data` AS
SELECT
  quality_level,
  material_grade,
  surface_treatment,
  thickness_mm,
  width_mm,
  description,
  weight_kg,
  quantity,
  NULL AS article_id,
  NULL AS reserved
FROM `supplier_data1_cleaned`

UNION ALL

SELECT
  NULL AS quality_level,
  material_grade,
  NULL AS surface_treatment,
  NULL AS thickness_mm,
  NULL AS width_mm,
  description,
  weight_kg,
  quantity,
  article_id,
  reserved
FROM `supplier_data2_cleaned`;


CREATE OR REPLACE TABLE `buyer_preferences` AS
SELECT
  SAFE_CAST(NULLIF(`Buyer ID`, '') AS STRING) AS buyer_id,
  SAFE_CAST(NULLIF(`Preferred Grade`, '') AS STRING) AS preferred_grade,
  SAFE_CAST(NULLIF(`Preferred Finish`, '') AS STRING) AS preferred_finish,
  SAFE_CAST(NULLIF(`Preferred Thickness (mm)`, '') AS FLOAT64) AS preferred_thickness_mm,
  SAFE_CAST(NULLIF(`Preferred Width (mm)`, '') AS FLOAT64) AS preferred_width_mm,
  SAFE_CAST(NULLIF(`Max Weight (kg)`, '') AS FLOAT64) AS max_weight_kg,
  SAFE_CAST(NULLIF(`Min Quantity`, '') AS INT64) AS min_quantity
FROM `buyer_preferences_staging`;


CREATE OR REPLACE TABLE `recommendations` AS
SELECT
  b.buyer_id,
  b.preferred_grade,
  b.preferred_finish,
  b.preferred_thickness_mm,
  b.preferred_width_mm,
  b.max_weight_kg,
  b.min_quantity,
  s.quality_level,
  s.material_grade,
  s.surface_treatment,
  s.thickness_mm,
  s.width_mm,
  s.weight_kg,
  s.quantity,
  s.article_id,
  s.reserved
FROM `supplier_data` s
JOIN `buyer_preferences` b
  ON s.material_grade = b.preferred_grade
     AND s.surface_treatment = b.preferred_finish
     AND s.thickness_mm = b.preferred_thickness_mm
     AND s.width_mm = b.preferred_width_mm
WHERE
  s.weight_kg <= b.max_weight_kg
  AND s.quantity >= b.min_quantity
  AND (s.reserved IS NULL OR s.reserved != 'yes');  
