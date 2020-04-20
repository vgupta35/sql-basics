USE BancoPopularPR
GO

IF OBJECT_ID('tempdb..#CleanedAccount')			IS NOT NULL DROP TABLE [#CleanedAccount];
IF OBJECT_ID('tempdb..#CleanedCustomer')		IS NOT NULL DROP TABLE [#CleanedCustomer];
IF OBJECT_ID('tempdb..#CleanedTransaction')		IS NOT NULL DROP TABLE [#CleanedTransaction];
IF OBJECT_ID('tempdb..#OverallCustStats')			IS NOT NULL DROP TABLE [#OverallCustStats];
IF OBJECT_ID('tempdb..#OverallAcctStats')		IS NOT NULL DROP TABLE [#OverallAcctStats];
IF OBJECT_ID('tempdb..#OverallTxnStats')		IS NOT NULL DROP TABLE [#OverallTxnStats];
IF OBJECT_ID('tempdb..#RawDailyData')				IS NOT NULL DROP TABLE [#RawDailyData];
IF OBJECT_ID('tempdb..#AllStats')				IS NOT NULL DROP TABLE [#AllStats];

DECLARE @Database VARCHAR(15); 
SET @Database = 'BancoPopularPR';

DECLARE @PreviousMonth VARCHAR(10);

DECLARE @ValidationMonth VARCHAR(10); 
SET @ValidationMonth = '202002';


IF RIGHT(@ValidationMonth,2) = '01'
	SET @PreviousMonth = @ValidationMonth - 89
ELSE 
	SET @PreviousMonth = @ValidationMonth - 1;

DECLARE @AccountTable		VARCHAR(50); 
DECLARE @CustomerTable		VARCHAR(50); 
DECLARE @TransactionTable	VARCHAR(50);

SET  @AccountTable		= 'TRaw_Account_DIM_'+@ValidationMonth;
SET  @CustomerTable		= 'TRaw_Customer_DIM_'+@ValidationMonth;
SET  @TransactionTable	= 'TRaw_TRANSACTION_DIM_'+@ValidationMonth;

DECLARE @C_SQL VARCHAR(MAX); 
DECLARE @T_SQL VARCHAR(MAX); 
DECLARE @A_SQL VARCHAR(MAX); 



CREATE TABLE [#CleanedAccount]
(
[UPDATE_DATE] DATE,
[ACCOUNT_NUMBER] VARCHAR(20),
[ACCOUNT_SURR_KEY] BIGINT,
[ACCOUNT_TYPE] VARCHAR(10),
[ACCOUNT_DATE_OPENED] DATE,
[EFFECTIVE_START_DATE] DATE,
[EFFECTIVE_END_DATE] DATE
);

CREATE INDEX IX_CleanAccounts ON #CleanedAccount(ACCOUNT_TYPE, UPDATE_DATE, ACCOUNT_SURR_KEY);

CREATE TABLE [#CleanedCustomer]
(
[LAST_UPDATE_DATE] DATE,
[CUSTOMER_NUMBER] VARCHAR(20),
[CUSTOMER_SURR_KEY] BIGINT,
[CUSTOMER_NAME] VARCHAR(250),
[EFFECTIVE_START_DATE] DATE,
[EFFECTIVE_END_DATE] DATE
);

CREATE INDEX IX_CleanCustomers ON #CleanedCustomer(LAST_UPDATE_DATE, CUSTOMER_SURR_KEY);

CREATE TABLE [#CleanedTransaction]
(
[DATE_OF_TRANSACTION] DATE,
[NAV_PK] BIGINT NOT NULL,
[APPLICATION_CODE] VARCHAR(4),
[CODE_TYPE] VARCHAR(10),
[AMOUNT_BASE_CURRENCY] NUMERIC(15, 3),
[ACCOUNT_SURR_KEY] BIGINT,
[CUSTOMER_SURR_KEY] BIGINT
);

CREATE INDEX IX_CleanTransactions ON #CleanedTransaction(CUSTOMER_SURR_KEY, ACCOUNT_SURR_KEY, CODE_TYPE, DATE_OF_TRANSACTION);

CREATE TABLE [#OverallCustStats]
(
[MinCustDate] DATE,
[MaxCustDate] DATE,
[TotalCount] INT,
[DistinctCustomers] INT,
[CustomersWithManyNames] INT
);

CREATE TABLE [#OverallAcctStats]
(
[MinCustDate] DATE,
[MaxCustDate] DATE,
[TotalCount] INT,
[DistinctAccounts] INT,
[AcctsOpenedAfterStart] INT,
[AcctsWithMultipleTypes] INT
);

CREATE TABLE [#OverallTxnStats]
(
[MinDate] DATE,
[MaxDate] DATE,
[TotalCount] INT,
[ApplicationCodes] VARCHAR(MAX)
);

CREATE TABLE [#RawDailyData]
(
[MinDate] DATE,
[DailyAmount] NUMERIC(18,4),
[DailyCount] INT
)


CREATE TABLE [#AllStats]
(
[Label] VARCHAR(250),
[Metric] VARCHAR(100), 
[DisplayID] INT
);

CREATE INDEX IX_AllStats ON #AllStats(DisplayID);


SET @A_SQL = '
WITH AccountTypeMapping AS (
	SELECT ''PR'' AS DB, ''P'' AS AccountType, LTRIM(RTRIM(value)) as AccountCode
	FROM STRING_SPLIT(''00002080,00004080,00005073,00001073,00006073,00005009,00006009,00001009''
		+'',00006080,00001080,00006052,00001052,00005052,00006060,00002060,00004060,00001060,00005060,00005020,00001020''
		+'',00006020,00002087,00001053,00006053,00005053,00001087,00006087,00005015,00006015,00001015,00004081,00002081''
		+'',00001081,00006081,00002073,00004073,00005026,00006026,00002026,00004026,00001026,00002053,00004053,00006028''
		+'',00005028,00005030,00001028,00001030,00005025,00005027,00001027,00001025,00006025,00006027,00004009,00002009''
		+'',00002017,00004017,00006024,00004024,00001024,00002024,00005024,00004051,00002051,00001075,00005075,00001029''
		+'',00005029,00006029,00005048,00001048,00006048,00004002,00002002,00006064,00001064,00005064,00002058,00004058''
		+'',00001058,00006058,00005058,00001090,00002090,00006090,00004090'','','') --Personal
	UNION ALL	
		   
	SELECT ''PR'' AS DB, ''B'' AS AccountType, LTRIM(RTRIM(value)) as AccountCode
	FROM STRING_SPLIT(''00006010,00002010,00004010,00004013,00003013,00002013,00006013,00005013''
		+'',00001013,00002061,00004061,00001061,00005061,00004047,00002047,00005047''
		+'',00006021,00005021,00001021,00004021,00002021,00005016,00006016,00001016,00001031,00005031,00006031,00005044''
		+'',00001044,00006044,00006043,00005043,00002043,00001043,00002044,00002031,00004031'','','') --Business
	UNION ALL
	SELECT ''PR'' AS DB, ''S'' AS AccountType, LTRIM(RTRIM(value)) as AccountCode --BSmart
	FROM STRING_SPLIT(''00002086,00001086,00006086,00004086'','','')
	UNION ALL

	SELECT ''NA'' AS DB, ''P'' AS AccountType, LTRIM(RTRIM(value)) as AccountCode
	FROM STRING_SPLIT (''001,002,004,005,006,007,008,009,010,011,012,013,014,015,020,021,''
				+'',022,023,024,025,026,029,030,031,032,033,034,035,040,041,042,043,044,045,046,047''
				+'',052,053,054,055,056,057,058,059,060,061,062,063,064,070,071,072,073,084,087,088''
				+'',089,098,107,108,109,110,111,113,114,115,116,117,118,119,120,121,122,123,124,125''
				+'',126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145''
				+'',146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165''
				+'',166,167,168,169,170,171,172,179,180,186,187,188,189,190,191,192,193,194,195,196''
				+'',197,198,199,200,201,202,203,204,205,206,208,211,216,217,218,219,220,221,222,223''
				+'',224,225,226,227,228,229,230,231,232,234,235,236,237,238,239,240,241,242,243,244''
				+'',245,246,247,248,259,260,261,262,263,264,266,267,268,269,270,271,272,273,274,275''
				+'',276,277,278,279,280,281,282,283,284,285,286,287,288,289,290,291,311,312,313,314''
				+'',315,316,317,318,319,320,321,322,323,324,326,327,328,329,330,331,332,333,334,335''
				+'',336,337,338,351,352,353,354,355,356,357,358,359,360,361,362,363,364,365,366,367''
				+'',368,369,383,384,385,386,387,388,400,485,486,487,488,489,490,491,492,493,494,495''
				+'',496,601,602,603,605,606,607,609,610,612,618,620,622,623,624,630,636,640,641,642''
				+'',645,648,660,672,711,770,771,772,CON,ELD040,ELD070,ELD071,ELD072,ELD073,ELD074,ELD075''
				+'',ELD076,ELD077,ELD081,ELD082,ELD083,ELD086,LN200,LN201,LN202,LN210,LN211,LN212,LN213,LN214''
				+'',LN215,LN216,LN217,LN218,LN219,LN220,LN230,LN240,LN241,LN242,LN244,LN245,LN246,LN247,LN250''
				+'',LN251,LN252,LN254,LN270,LN280,LN281,LN282,LN285,LN287,LN298,LN299,LN400,LN401,LN402,LN403''
				+'',LN404,LN405,LN410,LN411,LN412,LN413,LN418,LN420,LN421,LN422,LN424,LN425,LN426,LN427,LN428''
				+'',LN429,LN450,LN462,LN464,LN465,LN470,LN471,LN480,LN481,LN490,LN491,LN492,LN498,LN499,LNH15''
				+'',LNH45,LNHEL,LNODP,LNOTH,POPD010,POPD300,POPD301,POPD400,POPD426,POPD601,POPD602,POPD603,POPD606''
				+'',POPD609,POPD612,POPD618,POPD624,POPD636,POPD648,POPD660,POPD672,POPD302,POPD900'','','') --Personal
	UNION ALL
	SELECT ''NA'' AS DB, ''B'' AS AccountType, LTRIM(RTRIM(value)) as AccountCode
	FROM STRING_SPLIT (''003,018,019,027,028,036,037,038,039,048,050,051,065,066,067,068''
				+'',069,075,076,078,079,081,082,083,086,090,091,092,095,097,099,100,101,102,103,104''
				+'',105,173,174,175,176,177,178,181,182,183,184,185,207,209,210,212,213,214,215,233''
				+'',250,251,252,253,254,255,257,258,293,294,295,296,297,298,299,300,301,302,304,305''
				+'',306,307,309,310,340,341,342,343,344,345,346,347,348,370,371,372,373,374,375,376''
				+'',377,378,379,380,381,382,389,391,433,434,435,436,437,438,439,440,441,442,443,444''
				+'',445,446,448,449,450,451,452,453,454,455,456,457,459,460,461,462,467,470,471,472''
				+'',473,474,475,476,477,478,479,480,481,482,483,484,675,676,681,682,683,686,687,688''
				+'',689,690,691,692,695,710,712,713,790,791,792,COM,LN100,LN101,LN102,LN103,LN104,LN110''
				+'',LN111,LN112,LN113,LN114,LN115,LN116,LN117,LN119,LN120,LN121,LN122,LN123,LN124,LN125,LN126''
				+'',LN127,LN128,LN129,LN130,LN131,LN132,LN133,LN134,LN136,LN140,LN141,LN145,LN146,LN150,LN151''
				+'',LN160,LN161,LN170,LN171,LN172,LN173,LN174,LN179,LN180,LN189,LN199,LN501,LN502,LN503,LN510''
				+'',LN515,LN516,LN523,LNCBL,LNCCL,LNCCS,LNCLC,LNCLP,LNCLT,LNCND,LNCNR,LNCNT,LNCOL,LNCSB,LNDOC''
				+'',LNSLC'','','') --Business
)

, RawAccount AS (
	SELECT --TOP 10000
	UPDATE_DATE
	, ACCOUNT_NUMBER
	, ACCOUNT_SURR_KEY
	, ACCOUNT_TYPE
	, ACCOUNT_DATE_OPENED
	, EFFECTIVE_START_DATE
	, EFFECTIVE_END_DATE
	FROM '+@Database+'..'+@AccountTable+'
)

INSERT INTO #CleanedAccount
(
    UPDATE_DATE,
    ACCOUNT_NUMBER,
    ACCOUNT_SURR_KEY,
    ACCOUNT_TYPE,
    ACCOUNT_DATE_OPENED,
    EFFECTIVE_START_DATE,
    EFFECTIVE_END_DATE
)

--, CleanedAccount AS (
SELECT CONVERT(DATE,(NULLIF(REPLACE(UPDATE_DATE,'':00:00:00'',''''),'''')),106) AS UPDATE_DATE,
       RawAccount.ACCOUNT_NUMBER,
       RawAccount.ACCOUNT_SURR_KEY,
       CASE COALESCE(AccountTypeMapping.AccountType, ''U'') 
		WHEN ''P'' THEN ''Personal'' WHEN ''B'' THEN ''Business'' WHEN ''S'' THEN ''B-Smart'' WHEN ''U'' THEN ''Unknown'' END  AS ACCOUNT_TYPE,
       CONVERT(DATE,(NULLIF(REPLACE([ACCOUNT_DATE_OPENED],'':00:00:00'',''''),'''')),106) AS [ACCOUNT_DATE_OPENED],
	   CONVERT(DATE,(NULLIF(REPLACE([EFFECTIVE_START_DATE],'':00:00:00'',''''),'''')),106) AS [EFFECTIVE_START_DATE], 
	   CONVERT(DATE,(NULLIF(REPLACE(EFFECTIVE_END_DATE,'':00:00:00'',''''),'''')),106) AS EFFECTIVE_END_DATE
FROM RawAccount
LEFT JOIN AccountTypeMapping ON DB = '+''''+RIGHT(@Database,2)+''''+' AND AccountTypeMapping.AccountCode = RawAccount.ACCOUNT_TYPE;'
--)

EXEC(@A_SQL);

SET @C_SQL = '
WITH RawCustomer AS (
	SELECT --TOP 1000000
	LAST_UPDATE_DATE
	, CUSTOMER_NUMBER
	, CUSTOMER_SURR_KEY
	, CUSTOMER_FIRST_NAME
	, CUSTOMER_LAST_NAME
	, EFFECTIVE_START_DATE
	, EFFECTIVE_END_DATE
	FROM '+@Database+'..'+@CustomerTable+'
	)

INSERT INTO #CleanedCustomer
(
    LAST_UPDATE_DATE,
	CUSTOMER_NUMBER,
    CUSTOMER_SURR_KEY,
    CUSTOMER_NAME,
    EFFECTIVE_START_DATE,
    EFFECTIVE_END_DATE
)

--, CleanedCustomer AS (
	SELECT 
	   CONVERT(DATE,(NULLIF(REPLACE(LAST_UPDATE_DATE,'':00:00:00'',''''),'''')),106) AS LAST_UPDATE_DATE
       , RawCustomer.CUSTOMER_NUMBER
       , RawCustomer.CUSTOMER_SURR_KEY
       , REPLACE(CASE 
		WHEN customer_first_name = customer_last_name THEN customer_first_name
		WHEN customer_first_name IS NOT NULL AND customer_last_name IS NULL THEN customer_first_name
		WHEN customer_first_name IS NULL AND customer_last_name IS NOT NULL THEN customer_last_name
			ELSE COALESCE(customer_first_name, '''') + '' '' + COALESCE(customer_last_name, '''') END,''-'','' '') AS CUSTOMER_NAME
       , CONVERT(DATE,(NULLIF(REPLACE(EFFECTIVE_START_DATE,'':00:00:00'',''''),'''')),106) AS EFFECTIVE_START_DATE
	   , CONVERT(DATE,(NULLIF(REPLACE(EFFECTIVE_END_DATE,'':00:00:00'',''''),'''')),106) AS EFFECTIVE_END_DATE
	FROM RawCustomer;'

--)

EXEC(@C_SQL);

SET @T_SQL = '
WITH RawTransaction AS (
	SELECT --TOP 10000
		DATE_OF_TRANSACTION
		, nav_PK
		, APPLICATION_CODE
		, CODE_TYPE
		, AMOUNT_BASE_CURRENCY
		, ACCOUNT_SURR_KEY
		, CUSTOMER_SURR_KEY
	FROM '+@Database+'..'+@TransactionTable+'
)


INSERT INTO #CleanedTransaction
(
    DATE_OF_TRANSACTION,
    NAV_PK,
    APPLICATION_CODE,
    CODE_TYPE,
    AMOUNT_BASE_CURRENCY,
    ACCOUNT_SURR_KEY,
    CUSTOMER_SURR_KEY
)

--, CleanedTransaction AS (
SELECT 
	   CONVERT(DATE,(NULLIF(REPLACE(DATE_OF_TRANSACTION,'':00:00:00'',''''),'''')),106) AS DATE_OF_TRANSACTION,
       RawTransaction.NAV_PK,
       RawTransaction.APPLICATION_CODE,
       CASE RawTransaction.CODE_TYPE WHEN ''I'' THEN ''Incoming'' WHEN ''O'' THEN ''Outgoing'' ELSE ''Error'' END AS CODE_TYPE,
       RawTransaction.AMOUNT_BASE_CURRENCY,
       RawTransaction.ACCOUNT_SURR_KEY,
       RawTransaction.CUSTOMER_SURR_KEY 
	FROM RawTransaction;'
--)
EXEC(@T_SQL);

--Customer Requirements
---Min LastUpdateDate
---Max LastUpdateDate
---Total Count
---Distinct CustomerNumber
---# of CustomerNumbers with Multiple Names

INSERT INTO #OverallCustStats 
		( MinCustDate,
          MaxCustDate,
          TotalCount,
          DistinctCustomers,
          CustomersWithManyNames)

--WITH OverallCustStats AS  (
	SELECT 
		MIN(CleanedCustomer.LAST_UPDATE_DATE) AS MinCustDate
		, MAX(CleanedCustomer.LAST_UPDATE_DATE) AS MaxCustDate
		, COUNT(*) AS TotalCount
		, COUNT(DISTINCT CleanedCustomer.CUSTOMER_NUMBER) AS DistinctCustomers
		, COUNT(DISTINCT Names.CUSTOMER_NUMBER) AS CustomersWithManyNames
	--INTO #OverallCustStats
	FROM #CleanedCustomer CleanedCustomer
	LEFT JOIN ( SELECT CUSTOMER_NUMBER
					, COUNT(DISTINCT CUSTOMER_NAME) AS NumNames
				FROM #CleanedCustomer
				WHERE CUSTOMER_NAME <> ' ' 
				GROUP BY #CleanedCustomer.CUSTOMER_NUMBER 
				HAVING COUNT(DISTINCT CUSTOMER_NAME) > 1) Names 
	ON CleanedCustomer.CUSTOMER_NUMBER = Names.CUSTOMER_NUMBER;
--)

--Account Requirements
---Min UpdateDate
---Max UpdateDate
---Total Count
---Distinct AccountNumber
---Check if AccountOpenDates > EffectiveStartDates
---Check AcctNumbers with multiple AcctTypes

INSERT INTO #OverallAcctStats
(
    MinCustDate,
    MaxCustDate,
    TotalCount,
    DistinctAccounts,
    AcctsOpenedAfterStart,
    AcctsWithMultipleTypes
)


--, OverallAcctStats AS  (
	SELECT 
		MIN(CleanedAccount.UPDATE_DATE) AS MinCustDate
		, MAX(CleanedAccount.UPDATE_DATE) AS MaxCustDate
		, COUNT(*) AS TotalCount
		, COUNT(DISTINCT CleanedAccount.ACCOUNT_NUMBER) AS DistinctAccounts
		, COUNT(CASE WHEN ACCOUNT_DATE_OPENED > EFFECTIVE_START_DATE THEN CleanedAccount.ACCOUNT_NUMBER END) AS AcctsOpenedAfterStart
		, COUNT(DISTINCT AcctTypes.ACCOUNT_NUMBER) AS AcctsWithMultipleTypes
	--INTO #OverallAcctStats
	FROM #CleanedAccount CleanedAccount
	LEFT JOIN ( SELECT ACCOUNT_NUMBER
					, COUNT(DISTINCT ACCOUNT_TYPE) AS NumTypes
				FROM #CleanedAccount CleanedAccount
				WHERE CleanedAccount.ACCOUNT_TYPE <> 'Unknown' 
				GROUP BY ACCOUNT_NUMBER 
				HAVING COUNT(DISTINCT ACCOUNT_TYPE) > 1) AcctTypes
		ON CleanedAccount.ACCOUNT_NUMBER = AcctTypes.ACCOUNT_NUMBER;
--)


--Transaction Requirements
---Min Date
---Max Date
---Total Count
---List of all Application Codes
---Sum of Incoming/Outgoing Txns
---Check transactions on all working days

INSERT INTO #OverallTxnStats
(
    MinDate,
    MaxDate,
    TotalCount,
    ApplicationCodes
)


--WITH OverallTxnStats AS (
--, OverallTxnStats AS (
	SELECT 
		MIN(DATE_OF_TRANSACTION) AS MinDate
		, MAX(DATE_OF_TRANSACTION) AS MaxDate
		, COUNT(*) AS TotalCount
		, (SELECT DISTINCT 
					CTNA.APPLICATION_CODE+', '
				FROM #CleanedTransaction CTNA
				ORDER BY 1
				FOR XML PATH('')) ApplicationCodes
	--INTO #OverallTxnStats
	FROM #CleanedTransaction CleanedTransaction;
--)


--Loading Days Requirements
---Check all weekdays for activity

WITH RecursiveDays AS (
	   SELECT MinDate, MaxDate FROM #OverallTxnStats
       UNION ALL
       SELECT DATEADD(day, 1 , MinDate), MaxDate
       FROM  RecursiveDays
       WHERE  DATEADD(day, 1, MinDate) <= MaxDate
	   )

, WeekDays AS (
	SELECT MinDate, DATENAME(WEEKDAY,MinDate) AS DATE_OF_WEEK
	FROM RecursiveDays
	WHERE  DATENAME(WEEKDAY,MinDate) NOT IN ('Saturday', 'Sunday')
)

INSERT INTO #RawDailyData
(
    MinDate,
    DailyAmount,
    DailyCount
)

--, RawDailyData AS (
	SELECT
		WeekDays.MinDate
		, NULLIF(SUM(CleanedTransaction.AMOUNT_BASE_CURRENCY),0) AS DailyAmount
		, NULLIF(COUNT(CleanedTransaction.NAV_PK),0) AS DailyCount 
	--INTO #RawDailyData
	FROM WeekDays 
	LEFT JOIN #CleanedTransaction CleanedTransaction ON WeekDays.MinDate = DATE_OF_TRANSACTION
	GROUP BY WeekDays.MinDate;
--)

WITH TxnDailyRunStats AS (
SELECT
	COUNT(MinDate) AS TotalDays
	, COUNT(COALESCE(DailyAmount,DailyCount)) AS DaysPopulated
	, MIN(DailyAmount) AS MinDailyAmount
	, AVG(DailyAmount) AS AvgDailyAmount
	, MIN(DailyCount) AS MinDailyCount
	, AVG(DailyCount) AS AvgDailyCount
	FROM  #RawDailyData
)

--Amount Profile Distributions
---Check Account Type Sums
---Check Incoming and Outgoing Txn Sums


, Distributions AS (
	SELECT
		'Transaction Direction: '+CleanedTransaction.CODE_TYPE AS Label
		, SUM(CleanedTransaction.AMOUNT_BASE_CURRENCY) AS Amount
		, 16 + ROW_NUMBER() OVER (ORDER BY CleanedTransaction.CODE_TYPE) AS ID
	FROM #CleanedTransaction CleanedTransaction
	GROUP BY CleanedTransaction.CODE_TYPE
	UNION ALL
	SELECT 
		'Account Type: '+COALESCE(CleanedAccount.ACCOUNT_TYPE, 'None') AS Label
		, SUM(CleanedTransaction.AMOUNT_BASE_CURRENCY) AS Amount
		, 19 + ROW_NUMBER() OVER (ORDER BY 'Account Type: '+COALESCE(CleanedAccount.ACCOUNT_TYPE, 'None'))
	FROM #CleanedTransaction CleanedTransaction 
	JOIN #CleanedAccount CleanedAccount 
		ON CleanedTransaction.ACCOUNT_SURR_KEY = CleanedAccount.ACCOUNT_SURR_KEY
	GROUP BY 'Account Type: ' + COALESCE(CleanedAccount.ACCOUNT_TYPE, 'None')
	
	)

--Account x Customer x Transaction Requirements
---# of Customers with Multiple Acctypes (with txns)
---Check all Accts have CustKeys in Cust Table
---Check all Custs have AcctKeys in AcctTable
---# of Txns Outside of Min/Max Account Effective Dates
---# of Txns Outside of Min/Max Cust Effective Dates

, CustWMultipleAcctTypes AS (
	SELECT CleanedCustomer.CUSTOMER_NUMBER, COUNT(DISTINCT CleanedAccount.ACCOUNT_TYPE) AS NumAcctTypes
	FROM (SELECT DISTINCT CleanedTransaction.ACCOUNT_SURR_KEY, CleanedTransaction.CUSTOMER_SURR_KEY FROM #CleanedTransaction CleanedTransaction) Mapping
	JOIN #CleanedAccount CleanedAccount ON Mapping.ACCOUNT_SURR_KEY = CleanedAccount.ACCOUNT_SURR_KEY
	JOIN #CleanedCustomer CleanedCustomer ON Mapping.CUSTOMER_SURR_KEY = CleanedCustomer.CUSTOMER_SURR_KEY
	GROUP BY CleanedCustomer.CUSTOMER_NUMBER HAVING COUNT(DISTINCT CleanedAccount.ACCOUNT_TYPE) > 1
)

, LinkedOverallStats AS (
SELECT 
	ROUND(COUNT(DISTINCT CleanedAccount.ACCOUNT_SURR_KEY)*1.00/COUNT(DISTINCT CleanedTransaction.ACCOUNT_SURR_KEY),2) AS TxnAcctCoverage
	, ROUND(COUNT(DISTINCT CleanedCustomer.CUSTOMER_SURR_KEY)*1.00/COUNT(DISTINCT CleanedTransaction.CUSTOMER_SURR_KEY),2) AS TxnCustCoverage
	, COUNT(CASE WHEN NOT (CleanedTransaction.DATE_OF_TRANSACTION  BETWEEN CleanedAccount.EFFECTIVE_START_DATE AND CleanedAccount.EFFECTIVE_END_DATE) THEN CleanedTransaction.NAV_PK END) AS TxnsOutsideAcctDate
	, COUNT(CASE WHEN NOT (CleanedTransaction.DATE_OF_TRANSACTION  BETWEEN CleanedCustomer.EFFECTIVE_START_DATE AND CleanedCustomer.EFFECTIVE_END_DATE) THEN CleanedTransaction.NAV_PK END) AS TxnsOutsideCustDate
	, COUNT(DISTINCT CustWMultipleAcctTypes.CUSTOMER_NUMBER) AS CustsWManyAcctTypes
FROM #CleanedTransaction CleanedTransaction
LEFT JOIN #CleanedAccount CleanedAccount ON CleanedTransaction.ACCOUNT_SURR_KEY = CleanedAccount.ACCOUNT_SURR_KEY
LEFT JOIN #CleanedCustomer CleanedCustomer ON CleanedTransaction.CUSTOMER_SURR_KEY = CleanedCustomer.CUSTOMER_SURR_KEY
LEFT JOIN CustWMultipleAcctTypes ON CustWMultipleAcctTypes.CUSTOMER_NUMBER = CleanedCustomer.CUSTOMER_NUMBER
)



, AllNumbers AS (
	SELECT Distributions.Label, CAST(Amount AS VARCHAR) AS Metric, ID FROM Distributions
	UNION ALL
	SELECT c.Label, CAST(c.Metric AS VARCHAR), c.ID FROM LinkedOverallStats 
	CROSS APPLY(VALUES('Proportion of Transaction Table Account Keys in Accounts Table', LinkedOverallStats.TxnAcctCoverage,30)
				, ('Proportion of Transaction Table Customer Keys in Customers Table', LinkedOverallStats.TxnCustCoverage,31)
				, ('Count of Transactions Outside of Account Effective Dates', LinkedOverallStats.TxnsOutsideAcctDate,33)
				, ('Count of Transactions Outside of Customer Effective Dates', LinkedOverallStats.TxnsOutsideCustDate,32)	
				, ('Number of Customers with Multiple Account Types', LinkedOverallStats.CustsWManyAcctTypes, 26) ) c(Label, Metric, ID)
	UNION ALL
	SELECT c.Label, CAST(c.Metric AS VARCHAR), c.ID FROM #OverallAcctStats OverallAcctStats
	CROSS APPLY(VALUES
				('Account SQL Raw Table Row Count', OverallAcctStats.TotalCount, 1)
				, ('Number of Unique Accounts', OverallAcctStats.DistinctAccounts, 16)
				, ('Number of Account Records Where Open Date is Later Than Start Date', OverallAcctStats.AcctsOpenedAfterStart, 25)
				, ('Number of Accounts that Change Type Over Time',OverallAcctStats.AcctsWithMultipleTypes, 27)) c(Label, Metric, ID)
	UNION ALL


	SELECT c.Label, CAST(c.Metric AS VARCHAR), c.ID FROM #OverallCustStats OverallCustStats
	CROSS APPLY(VALUES('Customer SQL Raw Table Row Count', OverallCustStats.TotalCount, 2)
				, ('Number of Unique Customers', OverallCustStats.DistinctCustomers, 15)
				, ('Number of Customers with Multiple Names',OverallCustStats.CustomersWithManyNames,29)) c(Label, Metric, ID)
	   
	UNION ALL
	SELECT c.Label, CAST(c.Metric AS VARCHAR), c.ID FROM #OverallTxnStats OverallTxnStats
	CROSS APPLY(VALUES('Transaction SQL Raw Table Row Count', OverallTxnStats.TotalCount, 3)
				, ('Number of Unique Transactions', OverallTxnStats.TotalCount, 14)
				) c(Label, Metric, ID)
			
	UNION ALL
	SELECT c.Label, CAST(c.Metric AS VARCHAR), c.ID FROM TxnDailyRunStats
	CROSS APPLY(VALUES('Proportion of Weekdays with Activity', ROUND(DaysPopulated*1.00/TotalDays,2),24)) c(Label, Metric, ID)
	)

, AllDates AS (
	SELECT c.Label, CAST(c.Metric AS VARCHAR) AS Metric, ID FROM #OverallAcctStats OverallAcctStats
	CROSS APPLY(VALUES('Account Beginning Time Horizon for Data Extract', OverallAcctStats.MinCustDate, 4)
				, ('Account Ending Time Horizon for Data Extract', OverallAcctStats.MaxCustDate, 7)
				) c(Label, Metric, ID)
	UNION ALL

	SELECT c.Label, CAST(c.Metric AS VARCHAR), ID FROM #OverallCustStats  OverallCustStats
	CROSS APPLY(VALUES('Customer Beginning Time Horizon for Data Extract', OverallCustStats.MinCustDate, 5)
				, ('Customer Ending Time Horizon for Data Extract', OverallCustStats.MaxCustDate, 8)
				) c(Label, Metric, ID)
	   
	UNION ALL
	SELECT c.Label, CAST(c.Metric AS VARCHAR), ID FROM #OverallTxnStats OverallTxnStats
	CROSS APPLY(VALUES('Transaction Beginning Time Horizon for Data Extract',OverallTxnStats.MinDate, 6)
				, ('Transaction Ending Time Horizon for Data Extract', OverallTxnStats.MaxDate, 9)
				--, ('List of Source Systems in Scope', OverallTxnStats.ApplicationCodes)
				) c(Label, Metric, ID)
)

, AllVarChar AS (
SELECT 'List of Source Systems in Scope' AS Label, LEFT(ApplicationCodes,LEN(ApplicationCodes)-1) AS Metric, 10 AS ID FROM #OverallTxnStats UNION ALL
SELECT 'Transaction Time Horizon', CAST(#OverallTxnStats.MinDate AS VARCHAR)+' - '+CAST(#OverallTxnStats.MaxDate AS VARCHAR), 11 FROM #OverallTxnStats UNION ALL
SELECT 'Account Time Horizon', CAST(#OverallAcctStats.MinCustDate AS VARCHAR)+' - '+CAST(#OverallAcctStats.MaxCustDate AS VARCHAR), 12 FROM #OverallAcctStats UNION ALL
SELECT 'Customer Time Horizon', CAST(#OverallCustStats.MinCustDate AS VARCHAR)+' - '+CAST(#OverallCustStats.MaxCustDate AS VARCHAR), 13 FROM #OverallCustStats UNION ALL
SELECT 'Uniqueness of Transaction IDs', 'No Transaction IDs', 28
)


INSERT INTO #AllStats
(
    
	Label,
    Metric, 
	DisplayID
)

SELECT * FROM AllDates UNION ALL
SELECT * FROM AllNumbers UNION ALL
SELECT * FROM AllVarChar;


DECLARE @StoreDV_SQL VARCHAR(MAX);
SET @StoreDV_SQL = 
'
TRUNCATE TABLE BancoPopularAnalysis..TXDailyTransactions'+RIGHT(@Database,2)+';

INSERT INTO BancoPopularAnalysis..TXDailyTransactions'+RIGHT(@Database,2)+'
(
    MinDate,
    DailyAmount'+RIGHT(@Database,2)+',
    DailyCount'+RIGHT(@Database,2)+'
)

SELECT
	MinDate
	, DailyAmount
	, DailyCount
FROM #RawDailyData;

DELETE FROM BancoPopularAnalysis..TXDataValidationMetrics WHERE [Month] = '''+@ValidationMonth+''' AND [Database] = '''+@Database+''';

INSERT INTO BancoPopularAnalysis..TXDataValidationMetrics
(
    [Database],
	[Month],
	DisplayID,
    Metric,
    Label
)

SELECT 
'''+@Database+'''
, '''+@ValidationMonth+'''
, DisplayID
, Metric
, Label
FROM #AllStats;


WITH CurrentMonth AS (
	SELECT * 
	FROM BancoPopularAnalysis..TXDataValidationMetrics 
	WHERE [Database] = '''+@Database+''' 
		AND [Month] = '''+@ValidationMonth+'''
	)

, PreviousMonth AS (
	SELECT * 
	FROM BancoPopularAnalysis..TXDataValidationMetrics 
	WHERE [Database] = '''+@Database+''' 
		AND [Month] = '''+@PreviousMonth+'''

)

SELECT 
	CurrentMonth.DisplayID
	, CurrentMonth.Label
	, CurrentMonth.Metric AS CurrentMonth
	, PreviousMonth.Metric AS PreviousMonth
	, CASE WHEN NOT ((CurrentMonth.DisplayID BETWEEN 4 AND 13) OR CurrentMonth.Label =  ''Uniqueness of Transaction IDs'') 
		THEN ROUND((CAST(CurrentMonth.Metric AS FLOAT) - CAST(PreviousMonth.Metric AS FLOAT))/NULLIF(CAST(PreviousMonth.Metric AS FLOAT),0),4)
		ELSE NULL END AS PercentChange
FROM CurrentMonth
JOIN PreviousMonth 
ON CurrentMonth.Label = PreviousMonth.Label
--WHERE CurrentMonth.DisplayID BETWEEN 1 AND 27
ORDER BY CurrentMonth.DisplayID;
';
--PRINT(@StoreDV_SQL);
EXEC(@StoreDV_SQL);

--SELECT * FROM BancoPopularAnalysis..TXDailyTransactionsNA ORDER BY MinDate
--SELECT * FROM BancoPopularAnalysis..TXDailyTransactionsPR ORDER BY MinDate
--SELECT * FROM BancoPopularAnalysis..TXDataValidationMetrics ORDER BY [Month] DESC,[Database], DisplayID




