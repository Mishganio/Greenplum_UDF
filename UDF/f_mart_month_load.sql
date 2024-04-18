CREATE OR REPLACE FUNCTION stg.f_mart_month_load(p_table text,p_schema_name text,p_month text)
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
				select c.sn,p.pname,coalesce(p.brand,''Прочее'') as brand,sh.sname,nombloc,sum(quantity) as quantity,sum(amount_rub) as amount_rub
 				from dds.sales s
				   left join  dds.card c on s.id_cft=c.id_cft 
				   left join  dds.product p on s.id_code=p.id_code
				   left join  dds.shop sh on s.kodfil=sh.kodfil
				where sdate >= to_date('''||p_month||''',''YYYYMM'')
			    and sdate < to_date('''||p_month||''',''YYYYMM'') + interval ''1 month'' 
                group by c.sn,p.pname,p.brand,sh.sname,nombloc  
				distributed randomly';
						
			
	RAISE notice 'SQL_IS: %',v_sql;

	execute v_sql;

	execute 'select count(*) from '|| v_table_name into v_return;

	return v_return;

END;

$$
EXECUTE ON ANY;	