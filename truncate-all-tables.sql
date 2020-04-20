--Use the target Database
--DO: Set the Source Database as SourceDatabaseName

USE CreditSuisseKYCProd

IF OBJECT_ID('tempdb..#DataInsertionScripts')			IS NOT NULL DROP TABLE #DataInsertionScripts;

CREATE TABLE #DataInsertionScripts (Script NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL);

--A. Gather tables in db and comma separated list of columns
WITH TableSetup AS (
	SELECT 
		'CreditSuisseKYCDev' AS SourceDatabaseName
		, TABLE_CATALOG AS TargetDatabaseName
		, TABLE_NAME AS DatabaseTable
		, (SELECT '['+COLUMN_NAME+'], ' FROM INFORMATION_SCHEMA.COLUMNS ColumnReference 
			WHERE ColumnReference.TABLE_NAME = TableReference.TABLE_NAME 
				AND ColumnReference.TABLE_CATALOG = TableReference.TABLE_CATALOG
			FOR XML PATH('')) AS ColumnList
	FROM INFORMATION_SCHEMA.TABLES TableReference
	WHERE --TableReference.TABLE_CATALOG = 'CreditSuisseKYCDev' AND
		 TableReference.TABLE_TYPE = 'BASE TABLE'
)

INSERT INTO #DataInsertionScripts
(
    Script
)


--B. Create Scripts for trancating data and inserting new data
SELECT 'SET IDENTITY_INSERT '+TableSetup.TargetDatabaseName+'..'+TableSetup.DatabaseTable+' ON;'+CHAR(13)+CHAR(10)
	+'TRUNCATE TABLE '+TableSetup.TargetDatabaseName+'..'+TableSetup.DatabaseTable+';'+CHAR(13)+CHAR(10)
	+'INSERT INTO '+TableSetup.TargetDatabaseName+'..'+TableSetup.DatabaseTable+CHAR(13)+CHAR(10)
	+'('+LEFT(TableSetup.ColumnList,LEN(TableSetup.ColumnList)-1)+')'+CHAR(13)+CHAR(10)
	+'SELECT '+LEFT(TableSetup.ColumnList,LEN(TableSetup.ColumnList)-1)+CHAR(13)+CHAR(10)
	+'FROM '+TableSetup.SourceDatabaseName+'..'+TableSetup.DatabaseTable+';'+CHAR(13)+CHAR(10)
	+'SET IDENTITY_INSERT '+TableSetup.TargetDatabaseName+'..'+TableSetup.DatabaseTable+' OFF;'+CHAR(13)+CHAR(10)
	+CHAR(13)+CHAR(10) AS Script
--INTO #DataInsertionScripts
FROM TableSetup;


--C. Create cursor to loop through all table scripts
DECLARE @RefreshCursor AS CURSOR;
DECLARE @RefreshSQL NVARCHAR(MAX)='';

SET @RefreshCursor = CURSOR FOR SELECT * FROM #DataInsertionScripts

--Optional: Change to print to record entire script
OPEN @RefreshCursor;
	FETCH NEXT FROM @RefreshCursor INTO @RefreshSQL;
	WHILE @@FETCH_STATUS = 0
		BEGIN 
			--PRINT(@RefreshSQL)
			Exec Sp_executesql @RefreshSQL;
	FETCH NEXT FROM @RefreshCursor INTO @RefreshSQL;
END
