/****** 

<!! DATABASE NAME !!>.[dbo].[broker_PSMAQ]
Upsert for the prefix GPLAIN_PS1

CONSTRAINTS: None

******/


-- create rowcount table for python to access
create table #rowcount (rows int);


-- create a temp table
create table #pstrade (
		ACCOUNT_NAME					varchar(60)
	,	ACCOUNT							varchar(30)
	,	Trade_Date						varchar(30)
	,	MARKET							varchar(30)
	,	EXCHANGE_SYMBOL					varchar(30)
	,	INSTRUMENT_NAME					varchar(90)
	,	CONTRACT_YYYY					decimal(18, 8)
	,	CONTRACT_MM						decimal(18, 8)
	,	CONTRACT_DAY					decimal(18, 8)
	,	FIRST_PRICING_DATE				varchar(30)
	,	LAST_PRICING_DATE				varchar(30)
	,	TRADE_PRICE						decimal(18, 8)
	,	CURRENCY						varchar(30)
	,	QUANTITY_LOTS					decimal(18, 8)
	,	NOTIONAL_QUANTITY				decimal(18, 8)
	,	PS_DATE							varchar(30)
	,	SETTLEMENT_PRICE				decimal(18, 8)
	,	OTE								decimal(18, 8)
	,	EXECUTION_COMM					decimal(18, 8)
	,	REF_SUFFIX						varchar(60)		-- unused variable
	,	CLEARING_COMM					decimal(18, 8)
	,	EXCHANGE_FEES					decimal(18, 8)
	,	NFA_FEES						decimal(18, 8)
	,	dt								varchar(60)
	,	lastUpdate						varchar(60)
						)


-- bulk insert into temp table
bulk insert #pstrade 
from 'file_path'		-- placeholder
with (	
		fieldterminator = ',',
		rowterminator = '\n'
	  );


-- convert and cast
update #pstrade set
		dt			=	convert(datetime, dt, 112)
	,	lastUpdate	=	convert(datetime, lastUpdate)


-- delete old data
delete from broker_PSMAQ where dt = (select top 1 dt from #pstrade)


-- insert new data
insert into broker_PSMAQ
select
		ACCOUNT_NAME		
	,	ACCOUNT				
	,	Trade_Date			
	,	MARKET				
	,	EXCHANGE_SYMBOL		
	,	INSTRUMENT_NAME		
	,	CONTRACT_YYYY		
	,	CONTRACT_MM			
	,	CONTRACT_DAY		
	,	FIRST_PRICING_DATE	
	,	LAST_PRICING_DATE	
	,	TRADE_PRICE			
	,	CURRENCY			
	,	QUANTITY_LOTS		
	,	NOTIONAL_QUANTITY	
	,	PS_DATE				
	,	SETTLEMENT_PRICE	
	,	OTE					
	,	isnull(EXECUTION_COMM, 0) as EXECUTION_COMM		
	,	CLEARING_COMM		
	,	EXCHANGE_FEES	
	,	NFA_FEES			
	,	dt					
	,	lastUpdate			

from #pstrade


-- grab rowcount for python to access
insert into #rowcount select @@rowcount;

-- NOTE: temp table drop executed via python (in order to grab rows)