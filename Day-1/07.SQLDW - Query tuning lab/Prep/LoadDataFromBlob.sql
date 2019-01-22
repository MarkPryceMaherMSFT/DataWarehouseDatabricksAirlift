set nocount on;
-- cleanup
declare @i int;
declare @q varchar(400);
set @i=1;

while @i<7
begin
	set @q='if object_id(''dbo.customer_'+convert(varchar,@i)+''') is not null drop table dbo.customer_'+convert(varchar,@i)+';';
	exec(@q);
	set @q='if object_id(''dbo.orders_'+convert(varchar,@i)+''') is not null drop table dbo.orders_'+convert(varchar,@i)+';';
	exec(@q);
	set @q='if object_id(''dbo.lineitem_'+convert(varchar,@i)+''') is not null drop table dbo.lineitem_'+convert(varchar,@i)+';';
	exec(@q);
	set @q='if object_id(''dbo.part_'+convert(varchar,@i)+''') is not null drop table dbo.part_'+convert(varchar,@i)+';';
	exec(@q);
	set @q='if object_id(''dbo.partsupp_'+convert(varchar,@i)+''') is not null drop table dbo.partsupp_'+convert(varchar,@i)+';';
	exec(@q);
	set @q='if object_id(''dbo.supplier_'+convert(varchar,@i)+''') is not null drop table dbo.supplier_'+convert(varchar,@i)+';';
	exec(@q);
	set @q='if object_id(''dbo.nation_'+convert(varchar,@i)+''') is not null drop table dbo.nation_'+convert(varchar,@i)+';';
	exec(@q);
	set @q='if object_id(''dbo.region_'+convert(varchar,@i)+''') is not null drop table dbo.region_'+convert(varchar,@i)+';';
	exec(@q);
	set @i=@i+1;
end
print 'existing tables droped';
go

if object_id('dbo.customer_0') is not null drop external table dbo.customer_0;
if object_id('dbo.orders_0') is not null drop external table dbo.orders_0;
if object_id('dbo.lineitem_0') is not null drop external table dbo.lineitem_0;
if object_id('dbo.part_0') is not null drop external table dbo.part_0;
if object_id('dbo.partsupp_0') is not null drop external table dbo.partsupp_0;
if object_id('dbo.supplier_0') is not null drop external table dbo.supplier_0;
if object_id('dbo.nation_0') is not null drop external table dbo.nation_0;
if object_id('dbo.region_0') is not null drop external table dbo.region_0;
print 'existing external tables droped';
go

drop external file format [tpch_txtfmt];
go
drop external data source [tpch_ds];
go
drop database scoped credential [tpch_cred];
go
print 'existing source info droped';
go

-- create source info
--create database scoped credential [tpch_cred] with identity='pdw_user', secret='eiycJzJOm616hx1mopRT9QVkcblJr4dm4LLJZXGW9EqYv1EnQftS4FmCzzkfXND6fDxxgc9/bNVwNxP2uNi2LA==';
create database scoped credential [tpch_cred] with identity='pdw_user', secret='9fSL7l2W00VvQyJlKO0LVef7DYCHnXkuTwLfELgRchidKuwRbbY87jsT7rRn0j66r+uf0n5lFTcL0rnv8JfhKg==';
go
--create external data source [tpch_ds] with (type=hadoop, location='wasb://container1@qilistor.blob.core.windows.net', credential=[tpch_cred]);
create external data source [tpch_ds] with (type=hadoop, location='wasb://tpch10gb@tpcraw.blob.core.windows.net', credential=[tpch_cred]);
go
create external file format [tpch_txtfmt] with (format_type=delimitedtext);
go
print 'source info created';
go

-- create external tables
create external table dbo.customer_0 (c_custkey bigint, c_name varchar(25), c_address varchar(40), c_nationkey integer, c_phone char(15), c_acctbal decimal(15,2), c_mktsegment char(10), c_comment varchar(117))
with (location='customer.tbl', data_source=tpch_ds, file_format=tpch_txtfmt);
create external table dbo.orders_0 (o_orderkey bigint, o_custkey bigint, o_orderstatus char(1), o_totalprice decimal(15,2), o_orderdate date, o_orderpriority char(15), o_clerk char(15), o_shippriority integer, o_comment varchar(79))
with (location='orders.tbl', data_source=tpch_ds, file_format=tpch_txtfmt);
 create external table dbo.lineitem_0 (l_orderkey bigint, l_partkey bigint, l_suppkey bigint, l_linenumber bigint, l_quantity decimal(15,2), l_extendedprice decimal(15,2), l_discount decimal(15,2), l_tax decimal(15,2), l_returnflag char(1), l_linestatus char(1), l_shipdate date, l_commitdate date, l_receiptdate date, l_shipinstruct char(25), l_shipmode char(10), l_comment varchar(44))
with (location='lineitem.tbl', data_source=tpch_ds, file_format=tpch_txtfmt);
create external table dbo.part_0 (p_partkey bigint, p_name varchar(55), p_mfgr char(25), p_brand char(10), p_type varchar(25), p_size integer, p_container char(10), p_retailprice decimal(15,2), p_comment varchar(23))
with (location='part.tbl', data_source=tpch_ds, file_format=tpch_txtfmt);
create external table dbo.partsupp_0 (ps_partkey bigint, ps_suppkey bigint, ps_availqty integer, ps_supplycost decimal(15,2), ps_comment varchar(199))
with (location='partsupp.tbl', data_source=tpch_ds, file_format=tpch_txtfmt);
create external table dbo.supplier_0 (s_suppkey bigint, s_name char(25), s_address varchar(40), s_nationkey integer, s_phone char(15), s_acctbal decimal(15,2), s_comment varchar(101))
with (location='supplier.tbl', data_source=tpch_ds, file_format=tpch_txtfmt);
create external table dbo.nation_0 (n_nationkey integer, n_name char(25), n_regionkey integer, n_comment varchar(152))
with (location='nation.tbl', data_source=tpch_ds, file_format=tpch_txtfmt);
create external table dbo.region_0 (r_regionkey integer, r_name char(25), r_comment varchar(152))
with (location='region.tbl', data_source=tpch_ds, file_format=tpch_txtfmt);
print 'external tables created';
go

-- create empty tables
create table dbo.customer_1 (c_custkey bigint, c_name varchar(25), c_address varchar(40), c_nationkey integer, c_phone char(15), c_acctbal decimal(15,2), c_mktsegment char(10), c_comment varchar(117))
with (distribution=hash(c_custkey));
create table dbo.orders_1 (o_orderkey bigint, o_custkey bigint, o_orderstatus char(1), o_totalprice decimal(15,2), o_orderdate date, o_orderpriority char(15), o_clerk char(15), o_shippriority integer, o_comment varchar(79))
with (distribution=hash(o_orderkey));
create table dbo.lineitem_1 (l_orderkey bigint, l_partkey bigint, l_suppkey bigint, l_linenumber bigint, l_quantity decimal(15,2), l_extendedprice decimal(15,2), l_discount decimal(15,2), l_tax decimal(15,2), l_returnflag char(1), l_linestatus char(1), l_shipdate date, l_commitdate date, l_receiptdate date, l_shipinstruct char(25), l_shipmode char(10), l_comment varchar(44))
with (distribution=hash(l_orderkey));
create table dbo.part_1 (p_partkey bigint, p_name varchar(55), p_mfgr char(25), p_brand char(10), p_type varchar(25), p_size integer, p_container char(10), p_retailprice decimal(15,2), p_comment varchar(23))
with (distribution=hash(p_partkey));
create table dbo.partsupp_1 (ps_partkey bigint, ps_suppkey bigint, ps_availqty integer, ps_supplycost decimal(15,2), ps_comment varchar(199))
with (distribution=hash(ps_partkey));
create table dbo.supplier_1 (s_suppkey bigint, s_name char(25), s_address varchar(40), s_nationkey integer, s_phone char(15), s_acctbal decimal(15,2), s_comment varchar(101))
with (distribution=hash(s_suppkey));
create table dbo.nation_1 (n_nationkey integer, n_name char(25), n_regionkey integer, n_comment varchar(152))
with (distribution=hash(n_nationkey));
create table dbo.region_1 (r_regionkey integer, r_name char(25), r_comment varchar(152))
with (distribution=hash(r_regionkey));
print 'empty tables created';
go

-- create tables
create table dbo.[customer_2] with (distribution=round_robin) as select * from dbo.[customer_0];
create table dbo.[orders_2] with (distribution=round_robin) as select * from dbo.[orders_0];
create table dbo.[lineitem_2] with (distribution=round_robin) as select * from dbo.[lineitem_0];
create table dbo.[part_2] with (distribution=round_robin) as select * from dbo.[part_0];
create table dbo.[partsupp_2] with (distribution=round_robin) as select * from dbo.[partsupp_0];
create table dbo.[supplier_2] with (distribution=round_robin) as select * from dbo.[supplier_0];
create table dbo.[nation_2] with (distribution=round_robin) as select * from dbo.[nation_0];
create table dbo.[region_2] with (distribution=round_robin) as select * from dbo.[region_0];
print 'second tables created';
go

create table dbo.[customer_3] with (distribution=hash(c_custkey)) as select * from dbo.[customer_2];
create table dbo.[orders_3] with (distribution=hash(o_orderkey)) as select * from dbo.[orders_2];
create table dbo.[lineitem_3] with (distribution=hash(l_orderkey)) as select * from dbo.[lineitem_2];
create table dbo.[part_3] with (distribution=hash(p_partkey)) as select * from dbo.[part_2];
create table dbo.[partsupp_3] with (distribution=hash(ps_partkey)) as select * from dbo.[partsupp_2];
create table dbo.[supplier_3] with (distribution=hash(s_suppkey)) as select * from dbo.[supplier_2];
create table dbo.[nation_3] with (distribution=hash(n_nationkey)) as select * from dbo.[nation_2];
create table dbo.[region_3] with (distribution=hash(r_regionkey)) as select * from dbo.[region_2];
print 'third tables created';
go

insert into dbo.[customer_1] select * from dbo.[customer_2];
insert into dbo.[orders_1] select * from dbo.[orders_2];
insert into dbo.[lineitem_1] select * from dbo.[lineitem_2];
insert into dbo.[part_1] select * from dbo.[part_2];
insert into dbo.[partsupp_1] select * from dbo.[partsupp_2];
insert into dbo.[supplier_1] select * from dbo.[supplier_2];
insert into dbo.[nation_1] select * from dbo.[nation_2];
insert into dbo.[region_1] select * from dbo.[region_2];
print 'data inserted';
go

create table dbo.[customer_4] with (distribution=hash(c_custkey)) as select * from dbo.[customer_2];
create table dbo.[orders_4] with (distribution=hash(o_orderkey)) as select * from dbo.[orders_2];
create table dbo.[lineitem_4] with (distribution=hash(l_orderkey)) as select * from dbo.[lineitem_2];
create table dbo.[part_4] with (distribution=hash(p_partkey)) as select * from dbo.[part_2];
create table dbo.[partsupp_4] with (distribution=hash(ps_partkey)) as select * from dbo.[partsupp_2];
create table dbo.[supplier_4] with (distribution=hash(s_suppkey)) as select * from dbo.[supplier_2];
create table dbo.[nation_4] with (distribution=hash(n_nationkey)) as select * from dbo.[nation_2];
create table dbo.[region_4] with (distribution=hash(r_regionkey)) as select * from dbo.[region_2];
print 'forth tables created';
go

create statistics stat_o_orderkey on dbo.orders_4 (o_orderkey); --join
create statistics stat_o_orderdate on dbo.orders_4 (o_orderdate); --where
create statistics stat_o_custkey on dbo.orders_4 (o_custkey); --join
create statistics stat_l_orderkey on dbo.lineitem_4 (l_orderkey); --join
create statistics stat_l_suppkey on dbo.lineitem_4 (l_suppkey); --join
create statistics stat_c_custkey on dbo.customer_4 (c_custkey); --join
create statistics stat_c_nationkey on dbo.customer_4 (c_nationkey); --join
create statistics stat_s_suppkey on dbo.supplier_4 (s_suppkey); --join
create statistics stat_s_nationkey on dbo.supplier_4 (s_nationkey); --join
create statistics stat_n_name on dbo.nation_4 (n_name); --group by
create statistics stat_n_nationkey on dbo.nation_4 (n_nationkey); --join
create statistics stat_n_regionkey on dbo.nation_4 (n_regionkey); --join
create statistics stat_r_regionkey on dbo.region_4 (r_regionkey); --join
create statistics stat_r_name on dbo.region_4 (r_name); --where
print 'stats created';
go

create table dbo.[lineitem_6] with (distribution=hash(l_linenumber)) as select * from dbo.[lineitem_2];
print 'data skew table created';
go
