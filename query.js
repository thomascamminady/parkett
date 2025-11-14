// Default SQL query template
export const DEFAULT_QUERY = `SELECT
  *,
  row_number() OVER (PARTITION BY filename) AS filename_index
FROM 
	read_parquet('*.parquet', union_by_name=true, filename=true)
ORDER BY 
	filename, filename_index`;
