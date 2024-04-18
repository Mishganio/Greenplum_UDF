
------- магазины

DROP EXTERNAL TABLE IF EXISTS stg.shop_ext;
CREATE EXTERNAL TABLE stg.shop_ext
						(
        		        kodfil BIGINT,
                        sname VARCHAR(50),
                        id_parent BIGINT,
                        address VARCHAR(200)
                        )
LOCATION ('pxf://crm-sales/shop.csv?PROFILE=s3:csv')
FORMAT 'TEXT' 
        (DELIMITER '|' NULL '' )
encoding 'windows-1251';

       
------- клиенты
       
DROP EXTERNAL TABLE IF EXISTS stg.shop_ext;
CREATE EXTERNAL TABLE stg.card_ext
						(
        		        id_cft bigint,
                        cname varchar(50),
                        sn varchar(20),
                        begin_date date,
                        status varchar(20)
                        )
LOCATION ('pxf://crm-sales/card.csv?PROFILE=s3:csv')
FORMAT 'TEXT' 
        (DELIMITER '|' NULL '' escape '"' )
encoding 'windows-1251';

------- Товары

DROP EXTERNAL TABLE IF EXISTS stg.shop_ext;
CREATE EXTERNAL TABLE stg.product_ext
						(
        		        id_code bigint,
                        pname text,
                        model text,
                        brand varchar(50)                        
                        )
LOCATION ('pxf://crm-sales/product.csv?PROFILE=s3:csv')
FORMAT 'TEXT' 
        (DELIMITER '|' NULL '' escape '$' )
encoding 'windows-1251';

---------- Продажи

DROP EXTERNAL TABLE IF EXISTS stg.sales_ext;
CREATE EXTERNAL TABLE stg.sales_ext
						(
        		        sdate date,
                		id_cft bigint,
                		id_code bigint,
                		kodfil bigint,
               			id_user bigint,
               			nombloc bigint,
                		quantity decimal(15,2),
                		amount_rub decimal(19,4)                    
                        )
LOCATION ('pxf://crm-sales/sales.csv?PROFILE=s3:csv&')
FORMAT 'TEXT' 
        (DELIMITER '|' NULL '' )
encoding 'windows-1251';


------------ Продажи 2023

DROP EXTERNAL TABLE IF EXISTS stg.sales_2023_ext;
CREATE EXTERNAL TABLE stg.sales_2023_ext
						(
        		        sdate date,
                		id_cft bigint,
                		id_code bigint,
                		kodfil bigint,
               			id_user bigint,
               			nombloc bigint,
                		quantity decimal(15,2),
                		amount_rub decimal(19,4)                         
                        )
LOCATION ('pxf://crm-sales/sales.csv?PROFILE=s3:csv&')
FORMAT 'TEXT' 
        (DELIMITER '|' NULL '' )
encoding 'windows-1251';



			