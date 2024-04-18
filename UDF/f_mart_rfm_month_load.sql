CREATE OR REPLACE FUNCTION stg.f_mart_rfm_month_load(p_table text,p_schema_name text,p_month text)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
AS $$
	


DECLARE
	v_table_name text;
	v_sql text;
	v_return int;
BEGIN
	

	v_table_name := p_schema_name||'.'||p_table||'_'||p_month;
	v_sql := 'drop table if exists '||v_table_name||' cascade';
					
    RAISE notice 'SQL_IS: %',v_sql;
 	execute v_sql;
 
 	v_sql = 'create table '||v_table_name||'
				    WITH (
					appendonly=true,
					orientation=column,
					compresstype=zstd,
					compresslevel=1
				    )
			 as 
				
		SELECT SN,DATE_PART(''day'',to_date('''||p_month||''',''YYYYMM'')::timestamp - MAX(sdate)::timestamp) AS recency,COUNT(DISTINCT nombloc) AS frequency,SUM(amount_rub) AS monetary_value,
			CASE
				WHEN DATE_PART(''day'',to_date('''||p_month||''',''YYYYMM'')::timestamp - MAX(sdate)::timestamp) <=180 THEN ''Active''
        		WHEN DATE_PART(''day'',to_date('''||p_month||''',''YYYYMM'')::timestamp - MAX(sdate)::timestamp) <= 360 THEN ''Potential''
        		ELSE ''Inactive''
    		END AS segment
	    from dds.sales s	
		left join  dds.card c on s.id_cft=c.id_cft 
		where sdate<to_date('''||p_month||''',''YYYYMM'')
		GROUP BY sn	 
			distributed randomly';
						
			
	RAISE notice 'SQL_IS: %',v_sql;

	execute v_sql;

	execute 'select count(*) from '|| v_table_name into v_return;

	return v_return;

END;

$$
EXECUTE ON ANY;	

