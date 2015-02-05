
use HammerTime
go

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'SetupSampleData') AND type in (N'P', N'PC'))
    drop procedure SetupSampleData
go

create procedure SetupSampleData as

    if exists (select * from INFORMATION_SCHEMA.TABLES where TABLE_NAME = 'SampleData')
        drop table SampleData;

    create table SampleData (
	    Id uniqueidentifier not null,
	    IntCol1 int not null,
	    IntCol2 int not null,
	    StringCol varchar(50) not null
        -- Clustered random primary key. You are a naughty boy!
	    constraint pk_SampleData primary key (Id)
    );

return
go


IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'InsertSampleData') AND type in (N'P', N'PC'))
    drop procedure InsertSampleData
go

create procedure InsertSampleData
as
    
    declare @int1 int = rand() * 100000;
    declare @int2 int = rand() * 100000;

	insert into SampleData
		(Id, IntCol1, IntCol2, StringCol)
	values
		(newid(), @int1, @int2, newid())
	;

return
go


IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'FindSampleData') AND type in (N'P', N'PC'))
    drop procedure FindSampleData
go

create procedure FindSampleData
as

    -- do some silly select
	select sd1.StringCol
	from SampleData sd1
	inner join SampleData sd2
		on sd1.IntCol1 = sd2.IntCol2 + 3
	where sd1.IntCol1 not in ( select distinct sd2.IntCol2 / 5 from SampleData )
    ;

return
go
