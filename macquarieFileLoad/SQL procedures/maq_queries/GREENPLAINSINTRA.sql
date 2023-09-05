/****** 

<!! DATABASE NAME !!>.[dbo].[broker_intradayMaq]
Upsert for the prefix GREENPLAINSINTRA

CONSTRAINTS: None

******/




-- create rowcount table for python to access
create table #rowcount (rows int);


-- create a temp table
create table #intraday (
		TradeDate                       varchar(30)
	,	Account                         varchar(30)
	,	B_S                             varchar(30)
	,	Quantity                        int
	,	ProductDescription              varchar(60)
	,	DeliveryMonth                   int
	,	ProductType                     varchar(30)
	,	StrikePrice                     varchar(30)
	,	LastTradingDate					varchar(30)                
	,	Multiplication_factor           decimal(18, 8)
	,	ExchangeName                    varchar(30)
	,	MacquarieProductCode            varchar(30)
	,	ExchangeProductCode             varchar(30)
	,	TradePrice                      decimal(18, 8)
	,	ExecBroker                      varchar(30)
	,	SettlementType                  varchar(30)
	,	ExchangeTradeID                 varchar(30)
	,	LegalEntityName                 varchar(60)
	,	Currency                        varchar(30)
	,	Trader                          varchar(30)
	,	Trader2                         varchar(30)
	,	trading_venue                   varchar(30)
	,	fileDt                          varchar(30)
	,	lastUpdate						datetime
						)



-- bulk insert into temp table
bulk insert #intraday 
from 'file_path'		-- placeholder
with (	
		fieldterminator = ',',
		rowterminator = '\n'
	  );


-- convert and cast
update #intraday set
		TradeDate		=	cast(replace(TradeDate, '_', '-') as datetime)
	,	LastTradingDate =	cast(replace(LastTradingDate, '_', '') as int)


-- truncate current table
truncate table broker_intradayMaq


-- insert new data into table
insert into <!! DATABASE NAME !!>.dbo.broker_intradayMaq	-- insert new data
select
		TradeDate                
	,	Account                  
	,	B_S                      
	,	Quantity                 
	,	ProductDescription       
	,	DeliveryMonth            
	,	ProductType              
	,	StrikePrice              
	,	LastTradingDate          
	,	Multiplication_factor    
	,	ExchangeName             
	,	MacquarieProductCode     
	,	ExchangeProductCode      
	,	TradePrice               
	,	ExecBroker               
	,	SettlementType           
	,	ExchangeTradeID          
	,	LegalEntityName          
	,	Currency                 
	,	Trader                   
	,	Trader2                  
	,	trading_venue            
	,	lastUpdate				
	
from #intraday;


-- grab rowcount for python to access
insert into #rowcount select @@rowcount;

-- NOTE: temp table drop executed via python (in order to grab rows)