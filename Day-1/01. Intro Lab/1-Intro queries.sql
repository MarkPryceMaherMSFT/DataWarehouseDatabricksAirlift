

-- Distributions scaling 
SELECT   *  FROM SYS.pdw_distributions

-- list the distributions on the compute nodes
SELECT distinct pdw_node_id, MIN(distribution_id) [min_distributions_id], MAX(distribution_id) [max_distributions_id]
FROM SYS.pdw_distributions
GROUP BY pdw_node_id
ORDER BY 2

-- Total number of compute nodes
SELECT count(distinct pdw_node_id) [No of Nodes] 
FROM SYS.pdw_distributions
GROUP BY pdw_node_id

-- resource class
SELECT name 
FROM   sys.database_principals
WHERE  name LIKE '%rc%' AND type_desc = 'DATABASE_ROLE';

/* Show DWU - Gen2
SELECT  db.name [Database]
,       ds.edition [Edition]
,       ds.service_objective [Service Objective]
FROM    sys.database_service_objectives   AS ds
JOIN    sys.databases                     AS db ON ds.database_id = db.database_id
;
*/

-- Scaling a SQL DW
--Gen1
CREATE DATABASE myElasticSQLDW
WITH
(    SERVICE_OBJECTIVE = 'DW1000'
)
;

--Gen2
CREATE DATABASE myComputeSQLDW
WITH
(    
SERVICE_OBJECTIVE = 'DW100c'
)
;


SELECT    *
FROM      sys.databases
;

/* gen 2
SELECT    *
FROM      sys.dm_operation_status
WHERE     resource_type_desc = 'Database'
AND       major_resource_id = 'MySQLDW'
; */



