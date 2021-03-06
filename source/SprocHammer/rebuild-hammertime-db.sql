
USE master

IF EXISTS(select * from sys.databases where name='HammerTime')
begin

	ALTER DATABASE [HammerTime] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
	DROP DATABASE [HammerTime]

end

CREATE DATABASE [HammerTime]
GO
ALTER DATABASE [HammerTime] MODIFY FILE ( NAME = N'HammerTime', SIZE = 1024000KB , MAXSIZE = 51200000KB , FILEGROWTH = 10% )
GO
ALTER DATABASE [HammerTime] MODIFY FILE ( NAME = N'HammerTime_log', SIZE = 204800KB , MAXSIZE = 10240000KB , FILEGROWTH = 10% )
GO


use HammerTime
go


create proc GetFragmentation as 

/*

Output: 
    TableName	    varchar
    IndexName	    varchar
    IndexType	    varchar
    fragment_count	int
    page_count	    int
    avg_fragmentation_in_percet     int
    db_name     varchar

*/



    SELECT OBJECT_NAME(ind.OBJECT_ID) AS TableName,
	    ind.name AS IndexName, indexstats.index_type_desc AS IndexType,
	    indexstats.fragment_count,
	    indexstats.page_count,
	    indexstats.avg_fragmentation_in_percent,
	    dbs.name as db_name
    FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, NULL) indexstats
    INNER JOIN sys.indexes ind 
	    ON ind.object_id = indexstats.object_id
	    AND ind.index_id = indexstats.index_id
    inner join sys.databases dbs
	    on indexstats.database_id = dbs.database_id
    --where OBJECT_NAME(ind.OBJECT_ID) = 'SampleData'
    --WHERE indexstats.avg_fragmentation_in_percent > 30--You can specify the percent as you want
    where indexstats.index_type_desc in ('CLUSTERED INDEX', 'HEAP')
    ORDER BY indexstats.avg_fragmentation_in_percent DESC
    ;

return
go

print 'done'