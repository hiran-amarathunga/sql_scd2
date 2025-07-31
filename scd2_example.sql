CREATE PROC scd2_implementation AS

-- remove duplicates
SELECT *
INTO #tmp_cust
FROM (
	SELECT *
		,ROW_NUMBER() OVER (
			PARTITION BY customer_code ORDER BY tbl_created_date DESC
			) AS row_count
	FROM cust_table_vw
	) t
WHERE row_count = 1;

--- Update historical data when there is changes with default state = 'Y'
SELECT tgt.*
INTO #historical_load
FROM dim_target_table tgt
INNER JOIN #tmp_cust src ON isnull(tgt.customer_code, '') = isnull(src.customer_code, '')
WHERE (
isnull(tgt.[customer_name], '') <> isnull(src.[customer_name],'')
		)
	AND tgt.default_state = 'Y';

--- Set default state = 'N' for data already available in target table
UPDATE tgt
SET tgt.default_state = 'N'	
	,tgt.default_state_updated_date = getdate()
FROM dim_target_table tgt
INNER JOIN #historical_load src ON isnull(tgt.customer_code, '') = isnull(src.customer_code, '')
WHERE tgt.default_state = 'Y';

--- Insert new changes from historical data to target table while setting default state = 'Y'
INSERT INTO dim_target_table (
[customer_code]
,[customer_name]
,[default_state]
,[default_state_created_date]
,[default_state_updated_date]
	)
SELECT src.[customer_code]
,src.[customer_name]
,'Y' AS [default_state]
, getdate() AS [default_state_created_date] 
FROM #tmp_cust src
INNER JOIN #historical_load tgt ON isnull(src.customer_code, '') = isnull(tgt.customer_code, '');

--- Insert new records to target table while setting default state = 'Y'
INSERT INTO dim_target_table (
[customer_code]
,[customer_name]
,[default_state]
,[default_state_created_date]
,[default_state_updated_date]
	)
SELECT src.[customer_code]
,src.[customer_name]
,'Y' AS [default_state]
, getdate() AS [default_state_created_date] 
FROM #tmp_cust src
LEFT JOIN dim_target_table tgt ON isnull(src.customer_code, '') = isnull(tgt.customer_code, '')
WHERE tgt.surrogate_key_in_target IS NULL;

DROP TABLE #tmp_cust;

DROP TABLE #historical_load;

GO


