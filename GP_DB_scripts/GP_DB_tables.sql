create database slurm;
create schema dds; 
create schema stg;
create schema dm;

------- магазины


DROP TABLE IF EXISTS dds.shop;    
CREATE TABLE dds.shop
						(
        		        kodfil bigint not null,
                        sname varchar(50) null,
                        id_parent bigint null,
                        address varchar(200) null
                        )
DISTRIBUTED REPLICATED;
  
       
------- клиенты
 

DROP TABLE IF EXISTS dds.card;    
CREATE TABLE dds.card
						(
        		        id_cft bigint not null,
                        cname varchar(50) null,
                        sn varchar(20) null,
                        begin_date date null,
                        status varchar(20) null
                        )
DISTRIBUTED REPLICATED;

------- Товары

DROP TABLE IF EXISTS dds.product;    
CREATE TABLE dds.product
						(
        		        id_code bigint not null,
                        pname text null,
                        model text null,
                        brand varchar(50) null
                        )
DISTRIBUTED REPLICATED;

------ Продажи

DROP TABLE IF EXISTS dds.sales;    
CREATE TABLE dds.sales (
                sdate date not null,
                id_cft bigint not null,
                id_code bigint not null,
                kodfil bigint not null,
                id_user bigint not null,
                nombloc bigint not null,
                quantity decimal(15,2) null,
                amount_rub decimal(19,4) null
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED BY (nombloc)
partition  by range(sdate)
( start (date '2013-01-01') inclusive 
  end (date '2024-01-01') exclusive 
  every(interval '1 month')
);


			