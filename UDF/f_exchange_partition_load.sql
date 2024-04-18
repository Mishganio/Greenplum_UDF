CREATE OR REPLACE FUNCTION stg.f_exchange_partition_load(p_table text, p_schema_name text,p_tmp_schema_name text, p_partition_key text, p_start_date timestamp, p_end_date timestamp, p_pxf_table text, p_pxf_schema text)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
	
declare

 v_table_name text;
 v_ext_table text;
 v_temp_table text;
 v_part_temp_table text;
 v_full_table_name text;
 v_sql text;
 v_return int; 
 v_pxf text;
 v_result int;
 v_dist_key text;
 v_params text;
 v_where text;
 v_load_interval interval;
 v_start_date date;
 v_end_date date;
 v_table_oid int4;
begin 
   
	v_full_table_name = p_schema_name||'.'||p_table;
   	v_ext_table = p_table||'_ext';
	v_temp_table = p_table||'_tmp';
	v_part_temp_table = p_table||'_part_tmp' ;

	select c.oid
	into v_table_oid
	from pg_class as c inner join pg_namespace as n on c.relnamespace = n.oid
	where n.nspname||'.'||c.relname = v_full_table_name
	limit 1;
	if v_table_oid = 0 or v_table_oid is null then
	  v_dist_key = 'DISTRIBUTED RANDOMLY';
	else
	  v_dist_key = pg_get_table_distributedby(v_table_oid);
	end if;	

	select coalesce('with (' || array_to_string(reloptions, ', ') || ')','')
	from pg_class  
	into v_params
	where oid = v_full_table_name::regclass;

   	
	    EXECUTE 'drop external table if exists '||p_tmp_schema_name||'.'||v_ext_table;
   
		v_pxf = 'pxf://'||p_pxf_schema||'/'||p_pxf_table||'.csv?PROFILE=s3:csv';
   	 
   		RAISE notice 'SQL_IS: %',v_pxf;
   	
    	v_sql = 'create external table '||p_tmp_schema_name||'.'||v_ext_table||'(like '||p_schema_name||'.'||p_table||')
				LOCATION ('''||v_pxf||'''
				) ON ALL
				FORMAT ''TEXT'' (DELIMITER ''|'' NULL '''')
				ENCODING ''windows-1251''';
			
		RAISE notice 'SQL_IS: %',v_sql;
	
		EXECUTE v_sql;

	 	v_sql := 'DROP TABLE IF EXISTS '||p_tmp_schema_name||'.'|| v_temp_table ||';'
          	|| 'CREATE TABLE '||p_tmp_schema_name||'.'|| v_temp_table ||' (like '||p_schema_name||'.'||p_table||') ' ||v_params||' '||v_dist_key||';';
         
    	RAISE notice 'SQL_IS: %',v_sql;
   
   		EXECUTE v_sql;
	
    	v_sql = 'INSERT INTO '||p_tmp_schema_name||'.'|| v_temp_table ||' SELECT * FROM '||p_tmp_schema_name||'.'||v_ext_table;
   
   		RAISE notice 'SQL_IS: %',v_sql;
   
    	EXECUTE v_sql;
   
   		EXECUTE 'analyze '||p_tmp_schema_name||'.'|| v_temp_table;
		
        v_load_interval = '1 month'::interval;
        v_start_date := DATE_TRUNC('month', p_start_date);
   
    while v_start_date < p_end_date
	  loop  	
 		 
	 	v_sql := 'DROP TABLE IF EXISTS '||p_tmp_schema_name||'.'|| v_part_temp_table ||';'
          	|| 'CREATE TABLE '||p_tmp_schema_name||'.'|| v_part_temp_table ||' (like '||p_schema_name||'.'||p_table||') ' ||v_params||' '||v_dist_key||';';
         
    	RAISE notice 'SQL_IS: %',v_sql;
   
   		EXECUTE v_sql;

        v_end_date := DATE_TRUNC('month', v_start_date) + v_load_interval;
        v_where = p_partition_key ||' >= '''||v_start_date||'''::date and '||p_partition_key||' < '''||v_end_date||'''::date';     
        
    	v_sql = 'INSERT INTO '||p_tmp_schema_name||'.'|| v_part_temp_table  ||' SELECT * FROM '||p_tmp_schema_name||'.'||v_temp_table||' where '||v_where;
   
   		RAISE notice 'SQL_IS: %',v_sql;
   
    	EXECUTE v_sql;  
		  
		v_sql =  'alter table '||p_schema_name||'.'||p_table||' exchange partition for (date '''||v_start_date||''') with table '||p_tmp_schema_name||'.'|| v_part_temp_table ||' with validation';

		RAISE notice 'SQL_IS: %',v_sql;

		EXECUTE v_sql;
		
		v_start_date := v_start_date + v_load_interval;
	end loop;
	
	EXECUTE 'select count(1) from '||p_schema_name||'.'||p_table||' where '||p_partition_key ||' >= '''||p_start_date||'''::date and '||p_partition_key||' < '''||p_end_date||'''::date' into v_result;

	return v_result;

end;


$$
EXECUTE ON ANY;
