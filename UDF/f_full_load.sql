CREATE OR REPLACE FUNCTION stg.f_full_load(p_table text,  p_schema_name text,p_tmp_schema_name text, p_pxf_table text, p_pxf_schema text)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
AS $$
		
declare

 v_table_name text;
 v_ext_table text;
 v_sql text;
 v_pxf text;
 v_return int; 
 v_result int;
begin 
	
    v_ext_table = p_table||'_ext';
  
    EXECUTE 'drop external table if exists '||p_tmp_schema_name||'.'||v_ext_table;
   
		v_pxf = 'pxf://'||p_pxf_schema||'/'||p_pxf_table||'.csv?PROFILE=s3:csv';
   	 
   		RAISE notice 'SQL_IS: %',v_pxf;
   	
    	v_sql = 'create external table '||p_tmp_schema_name||'.'||v_ext_table||'(like '||p_schema_name||'.'||p_table||')
				LOCATION ('''||v_pxf||'''
				) ON ALL
				FORMAT ''TEXT'' (DELIMITER ''|'' NULL '''' escape ''$'' )
				ENCODING ''windows-1251''';
			
		RAISE notice 'SQL_IS: %',v_sql;
	
		EXECUTE v_sql;
	
    execute 'truncate table '||p_schema_name||'.'||p_table;
  
	execute 'insert into '||p_schema_name||'.'||p_table||' select * from '||p_tmp_schema_name||'.'||v_ext_table;
	
    execute 'analyze '||p_schema_name||'.'||p_table;

	execute 'select count(1) from '||p_schema_name||'.'||p_table into v_result;
	
	return v_result;
end;


$$
EXECUTE ON ANY;
