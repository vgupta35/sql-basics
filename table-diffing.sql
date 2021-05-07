USE [SBALoanDataStaging]
GO

/****** Object:  StoredProcedure [dbo].[SProductionDataRefresh]    Script Date: 5/7/2021 11:21:25 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO





--SELECT * FROM LOAN.LOANGNTYBORRTBL_New2mil;
--SELECT * FROM LOAN.LOANGNTYBORRTBL_NewProdSample;

--LOAN.LOANGNTYBORRTBL_NewProdSample --New Data
--LOAN.LOANGNTYBORRTBL_New2mil --Existing Data

CREATE PROCEDURE [dbo].[SProductionDataRefresh]

	@SourceInputID VARCHAR(38)
	,@TargetInputID VARCHAR(38)
	, @SourceTable VARCHAR(50)
	, @TargetTable VARCHAR(50)

	--, @NewFolderName VARCHAR(250)
	--, @FileCount int

AS 


DECLARE @SourceDB VARCHAR(20);
DECLARE @TargetDB VARCHAR(20);
--DECLARE @SourceTable VARCHAR(50);
--DECLARE @TargetTable VARCHAR(50);
DECLARE @InsertSQL NVARCHAR(MAX);
DECLARE @PivotColumns NVARCHAR(MAX);
--DECLARE @SourceInputID VARCHAR(38);

--Canbe paramterized by stored procedure
--DECLARE @TargetInputID VARCHAR(38);
DECLARE @NewFolderName VARCHAR(250);
DECLARE @FileCount int;


SET @SourceDB = 'SBALoanData';
SET @TargetDB = 'SBALoanDataStaging';
--SET @SourceTable = 'PARTNER.PRTLOCTBL';
--SET @TargetTable = 'PARTNER.PRTLOCTBL_NewProd';

--SET @TargetInputID = 'CA2C3DD3-B98E-4C35-B8A1-A94EA1FD91AA';
--SET @NewFolderName = 'NewProdTest_LOANGNTYBORRTBL'
--SET @FileCount = 29208;


/*
--Reset History for Test Run
TRUNCATE TABLE TSBAProductionBatchHistory;

INSERT INTO TSBAProductionBatchHistory
(
[FileName]
,ImportId
,[RowCount]
,CurrentInd
)
SELECT 'Test_LOANGNTYBORRTBL', @SourceInputID, (SELECT COUNT(*) FROM LOAN.LOANGNTYBORRTBL_New2mil), 1;

*/

--SET @SourceInputID =  '4A11E276-D3F7-44EA-9346-D139875E5A09';

--SELECT @SourceInputID = ImportID FROM TSBAProductionBatchHistory WHERE IsCurrent = 1;

DECLARE @PKSQL NVARCHAR(MAX); 

SET @PKSQL = 'WITH PKColumns as (
	select schema_name(tab.schema_id) as [schema_name], 
		pk.[name] as pk_name,
		substring(column_names, 1, len(column_names)-1) as [columns],
		tab.[name] as table_name
	from sys.tables tab
		inner join sys.indexes pk
			on tab.object_id = pk.object_id 
			and pk.is_primary_key = 1
	   cross apply (select col.[name] + '', ''
						from sys.index_columns ic
							inner join sys.columns col
								on ic.object_id = col.object_id
								and ic.column_id = col.column_id
						where ic.object_id = tab.object_id
							and ic.index_id = pk.index_id
							and col.[name] <> ''ItemID''
								order by col.column_id
								for xml path ('''') ) D (column_names)
	WHERE schema_name(tab.schema_id) IN (''LOAN'',''LOANAPP'',''PARTNER'')
	--order by schema_name(tab.schema_id), pk.[name]
	)


--, TempProposedTheme AS (
	SELECT TOP 1
		@PKs = [columns]
	FROM PKColumns
	WHERE schema_name+''.''+table_name = '''+@TargetTable+'''
	;';


--PRINT @PKSQL;
DECLARE @PKOut NVARCHAR(MAX); 
exec sp_executesql @PKSQL, N'@PKs NVARCHAR(MAX) out', @PKOut OUT;

--PRINT @PKOut;

IF OBJECT_ID('XPKList')		IS NOT NULL DROP TABLE XPKList; 

WITH XMLConvert AS (
SELECT  CONVERT(XML,'<x>' + REPLACE(@PKOut,',','</x><x>') + '</x>') AS PKXML
)



SELECT LTRIM(RTRIM(Element.Loc.value('.' , 'varchar(5000)'))) AS Value
INTO XPKList
FROM XMLConvert
CROSS APPLY PKXML.nodes('//x') Element(loc);

--SELECT * FROM #PKList;

DECLARE @OldIDSQL VARCHAR(MAX);

DECLARE @SourceCount nvarchar(10); 
DECLARE @TargetCount nvarchar(10);
DECLARE @RemovedCount nvarchar(10);


DECLARE @SourceSQL nvarchar(MAX); 
DECLARE @TargetSQL nvarchar(MAX);
DECLARE @RemovedSQL nvarchar(MAX);


--EXEC('SELECT @SourceCount = COUNT(*) FROM  '+@SourceDB+'.'+@SourceTable+';');
--EXEC('SELECT @TargetCount = COUNT(*) FROM  '+@TargetDB+'.'+@TargetTable+';');

DECLARE @OrigView VARCHAR(50);
SET @OrigView = 'V'+REPLACE(@SourceTable,'.','_');


DECLARE @Truncate VARCHAR(50);

SELECT @Truncate =  'THistory_'+RIGHT(@OrigView,LEN(@OrigView)-1);
--EXEC('TRUNCATE TABLE '+@Truncate+';');


SET @SourceSQL= 'SELECT @SourceC = CAST(COUNT(*) AS NVARCHAR) FROM '+@SourceDB+'.'+@SourceTable+';';
SET @TargetSQL= 'SELECT @TargetC = CAST(COUNT(*) AS NVARCHAR) FROM '+@TargetDB+'.'+@TargetTable+';';
SET @RemovedSQL= 'SELECT @RemovedC = CAST(COUNT(*) AS NVARCHAR) FROM '+@TargetDB+'..'+@Truncate+' WHERE Status = ''Removed'';';


exec sp_executesql @SourceSQL, N'@SourceC nvarchar(10) out', @SourceCount OUT;
exec sp_executesql @TargetSQL, N'@TargetC nvarchar(10) out', @TargetCount OUT;
exec sp_executesql @RemovedSQL, N'@RemovedC nvarchar(10) out', @RemovedCount OUT;

DECLARE @Joins VARCHAR(MAX);


SELECT @Joins = LEFT(UpdateJoins, LEN(UpdateJoins) -4) FROM (SELECT (SELECT 'Updated.'+[Value]+' = Previous.'+[Value]+' AND '
FROM XPKList FOR XML PATH ('')) AS UpdateJoins) a;


SET @OldIDSQL = '

--IF OBJECT_ID(''tempdb..#Updated'')	IS NOT NULL DROP TABLE #Updated;
--IF OBJECT_ID(''tempdb..#Previous'')	IS NOT NULL DROP TABLE #Previous;
--IF OBJECT_ID(''SBALoanDataSample..XTRemoved'')	IS NOT NULL DROP TABLE SBALoanDataSample..XTRemoved;

/*
WITH Removed AS (
SELECT TOP '+@RemovedCount+' '+@PKOut+',ItemID 
FROM '+@TargetDB+'..'+@Truncate+'
WHERE [Status] = ''Removed''
ORDER BY ItemID ASC
)

SELECT * INTO SBALoanDataSample..XTRemoved FROM Removed;
*/

WITH Updated AS (
SELECT TOP '+@TargetCount+' '+@PKOut+',ItemID, ROW_NUMBER() OVER (PARTITION BY '+@PKOut+' ORDER BY ItemID) AS RowNum
FROM '+@TargetDB+'.'+@TargetTable+'
ORDER BY ItemID ASC
)

, Removed AS (
SELECT TOP '+@RemovedCount+' '+@PKOut+',ItemID, ROW_NUMBER() OVER (PARTITION BY '+@PKOut+' ORDER BY ItemID) AS RowNum
FROM '+@TargetDB+'..'+@Truncate+'
WHERE [Status] = ''Removed''
ORDER BY ItemID ASC
)


--SELECT * INTO #Updated FROM Updated;

, Previous AS (
SELECT TOP '+@SourceCount+' '+@PKOut+',ItemID, ROW_NUMBER() OVER (PARTITION BY '+@PKOut+' ORDER BY ItemID) AS RowNum 
FROM '+@SourceDB+'.'+@SourceTable+' UNION ALL
SELECT TOP '+@RemovedCount+' * FROM Removed
ORDER BY ItemID ASC
)

--SELECT * INTO #Previous FROM Previous;

UPDATE Updated
SET ItemID = Previous.ItemID
FROM  Updated
JOIN  Previous
ON '+@Joins+' AND Updated.RowNum = Previous.RowNum;'


--PRINT(@OldIDSQL+@Joins);

--PRINT(@OldIDSQL);
EXEC(@OldIDSQL);





--SELECT * FROM LOAN.LOANGNTYBORRTBL_New2Mil EXCEPT SELECT * FROM LOAN.LOANGNTYBORRTBL_NewProdSample;



DECLARE @UpdateQuery VARCHAR(MAX);
DECLARE @InsertQuery VARCHAR(MAX);




DECLARE @HistoryColumns VARCHAR(MAX);
DECLARE @HistorySQL VARCHAR(MAX);

SELECT @HistoryColumns = LEFT(HistoryCols,LEN(HistoryCols)-1) FROM 
	(SELECT (SELECT COLUMN_NAME+',' FROM INFORMATION_SCHEMA.COLUMNS 
	WHERE TABLE_NAME = @Truncate
	FOR XML PATH('')) AS HistoryCols) a;

--PRINT @HistoryColumns;



/*
SET @HistorySQL = '
INSERT INTO '+@Truncate+' 
(
'+@HistoryColumns+'
)

SELECT * FROM '+@UpdateView+' UNION ALL
SELECT * FROM '+@InsertView+';'
*/



SET @HistorySQL = '
WITH Updated AS (
SELECT * FROM '+@SourceDB+'.'+@SourceTable+ ' EXCEPT '+'SELECT * FROM '+@TargetDB+'.'+@TargetTable+')

, PreInserts AS (
SELECT '+@PKOut+',ItemID FROM '+@TargetDB+'.'+@TargetTable+ ' EXCEPT '+'SELECT '+@PKOut+',ItemID FROM '+@SourceDB+'.'+@SourceTable+')

, Inserts AS (
SELECT * FROM PreInserts WHERE ItemID NOT IN (SELECT ItemID FROM '+@TargetDB+'..'+@Truncate+' WHERE [Status] = ''Removed'')
)

INSERT INTO '+@TargetDB+'..'+@Truncate+' 
(
'+@HistoryColumns+'
)

SELECT '''+@SourceInputID+''' AS ImportID
, *, CASE WHEN ItemID IN (SELECT ItemID FROM '+@TargetDB+'.'+@TargetTable+') THEN ''Updated'' ELSE ''Removed'' END AS Status
FROM Updated UNION ALL

SELECT '''+@TargetInputID+''' AS ImportID
, *, ''Added'' AS Status FROM '+@TargetDB+'.'+@TargetTable+ ' WHERE ItemID IN (SELECT ItemID FROM Inserts);

WITH CurrentTable AS (
SELECT TOP '+@TargetCount+' '+@PKOut+',ItemID 
FROM '+@TargetDB+'.'+@TargetTable+'
ORDER BY ItemID ASC
)

UPDATE '+@TargetDB+'..'+@Truncate+'
SET [Status] = ''Updated''
FROM '+@TargetDB+'..'+@Truncate+' Updated
JOIN CurrentTable Previous
ON '+@Joins+'
WHERE Updated.[Status] = ''Removed'';';

--PRINT(@HistorySQL);
EXEC(@HistorySQL);

DECLARE @BatchSQL VARCHAR(MAX);

SET @BatchSQL = '

WITH ChangesCount AS (
SELECT COUNT(*) AS ChangeCount FROM '+@TargetDB+'..'+@Truncate+' 
WHERE (ImportID = '''+@SourceInputID+''' AND Status <> ''Added'') OR ImportID = '''+@TargetInputID+'''
)

INSERT INTO TSBAProductionBatchHistory
(
[FileName]
,ImportDate
,ImportId
,[RowCount]
,IsCurrent
)
SELECT '''+@SourceTable+''', GETDATE(), '''+@TargetInputID+''', ChangeCount, 0 FROM ChangesCount;
'



--PRINT(@BatchSQL);
EXEC(@BatchSQL);



GO


