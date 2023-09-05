/****** 

<!! DATABASE NAME !!>.[dbo].[broker_maq_cftcpositions]
Upsert for the prefix CONSENSYS_GP

CONSTRAINTS: None

******/


-- create rowcount table for python to access
create table #rowcount (rows int);


-- create a temp table
create table #cftcpos (
		SourceSystem                        varchar(30)
	,	Account                             varchar(30)
	,	AccountName                         varchar(60)
	,	AccountType                         varchar(30)
	,	Trader                              varchar(30)
	,	ContractMonth                       varchar(30)
	,	ExchangeCode                        varchar(30)
	,	ProductCode                         varchar(30)
	,	ProductType                         varchar(30)
	,	OptionType                          varchar(30)
	,	ExchangeClearingID                  varchar(30)
	,	ProductDescription                  varchar(60)
	,	Currency                            varchar(30)
	,	Strike                              varchar(30)
	,	StrikeType                          varchar(30)
	,	ExpiryDate                          varchar(30)
	,	LastTradeDate                       varchar(30)
	,	OptionDelta                         varchar(30)
	,	BuySell                             varchar(30)
	,	Quantity							decimal(18, 8)
	,	InitialMargin                       decimal(18, 8)
	,	dt                                  varchar(30)
	,	ReportingID                         varchar(30)
	,	UnclearedFlag                       varchar(30)
	,	NotionalValue                       decimal(18, 8)
	,	FuturesEquivalent                   varchar(30)
	,	SettlementType                      varchar(30)
	,	TransactionID                       varchar(30)
	,	HedgingExemptionFlag                varchar(30)
	,	ExemptionType                       varchar(30)
	,	Counterparty                        varchar(30)
	,	InternalLegalEntity                 varchar(30)
	,	CRPPositionTypeIndicator            varchar(30)
	,	fileDt								varchar(60)		-- unused variable
	,	LastUpdate							datetime
)


-- bulk insert into temp table
bulk insert #cftcpos 
from 'file_path'		-- placeholder
with (	
		fieldterminator = ',',
		rowterminator = '\n'
	  );


-- convert and cast
update #cftcpos set
		ExpiryDate		=	convert(date, left(ExpiryDate, 8), 112)
	,	LastTradeDate	=	convert(date, left(LastTradeDate, 8), 112)
	,	dt				=	convert(datetime, dt) 


-- delete old data
delete from broker_MAQ_CFTCPositions where dt = (select top 1 dt from #cftcpos)


-- insert new data
insert into broker_MAQ_CFTCPositions
select
		SourceSystem                 
	,	Account                      
	,	AccountName                  
	,	AccountType                  
	,	Trader                       
	,	ContractMonth                
	,	ExchangeCode                 
	,	ProductCode                  
	,	ProductType                  
	,	OptionType                   
	,	ExchangeClearingID           
	,	ProductDescription           
	,	Currency                     
	,	Strike                       
	,	StrikeType                   
	,	ExpiryDate                   
	,	LastTradeDate                
	,	OptionDelta                  
	,	BuySell                      
	,	Quantity                 	
	,	InitialMargin                
	,	dt                           
	,	ReportingID                  
	,	UnclearedFlag                
	,	NotionalValue                
	,	FuturesEquivalent            
	,	SettlementType               
	,	TransactionID                
	,	HedgingExemptionFlag         
	,	ExemptionType                
	,	Counterparty                 
	,	InternalLegalEntity          
	,	CRPPositionTypeIndicator     				
	,	LastUpdate             

from #cftcpos

-- grab rowcount for python to access
insert into #rowcount select @@rowcount;


-- NOTE: temp table drop executed via python (in order to grab rows)
