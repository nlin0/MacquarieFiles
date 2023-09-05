/****** 

<!! DATABASE NAME !!>.[dbo].[broker_moneylineMAQ]
Upsert for the prefix GPLAIN_LOAN1. Upserts where Account is 'GPLNLN%'.

CONSTRAINTS: Account, Business_Date, fileDt	

******/


-- create output table
create table #merge_output (
    	action								 varchar(10)
    ,	tBusiness_Date						 datetime
    ,	tAccount_Group						 varchar(30)
    ,	tAccount							 varchar(30)
    ,	tCurrency							 varchar(30)
    ,	tBeginning_Balance					 decimal(18, 8)
    ,	tCommission							 decimal(18, 8)
    ,	tExecuting_Fee						 decimal(18, 8)
    ,	tExchange_Fee						 decimal(18, 8)
    ,	tNFA_Fees							 decimal(18, 8)
    ,	tRealised_PnS						 decimal(18, 8)
    ,	tOption_Premium						 decimal(18, 8)
    ,	tCash								 decimal(18, 8)
    ,	tEnding_Balance						 decimal(18, 8)
    ,	tOpen_Trade_Equity_Futures			 decimal(18, 8)
    ,	tTotal_Equity						 decimal(18, 8)
    ,	tLong_Option_Value					 decimal(18, 8)
    ,	tShort_Option_Value					 decimal(18, 8)
    ,	tNet_Liquidating_Value				 decimal(18, 8)
    ,	tInitial_Margin_Amount				 decimal(18, 8)
    ,	tGroup_Initial_Margin_Amount		 decimal(18, 8)
    ,	tCollateral							 decimal(18, 8)
    ,	tExcess_Shortage					 decimal(18, 8)
    ,	tINTEREST							 decimal(18, 8)
    ,	tfileDt								 int
    ,	tlastUpdate							 datetime
    ,	sBusiness_Date						 datetime
    ,	sAccount_Group						 varchar(30)
    ,	sAccount							 varchar(30)
    ,	sCurrency							 varchar(30)
    ,	sBeginning_Balance					 decimal(18, 8)
    ,	sCommission							 decimal(18, 8)
    ,	sExecuting_Fee						 decimal(18, 8)
    ,	sExchange_Fee						 decimal(18, 8)
    ,	sNFA_Fees							 decimal(18, 8)
    ,	sRealised_PnS						 decimal(18, 8)
    ,	sOption_Premium						 decimal(18, 8)
    ,	sCash								 decimal(18, 8)
    ,	sEnding_Balance						 decimal(18, 8)
    ,	sOpen_Trade_Equity_Futures			 decimal(18, 8)
    ,	sTotal_Equity						 decimal(18, 8)
    ,	sLong_Option_Value					 decimal(18, 8)
    ,	sShort_Option_Value					 decimal(18, 8)
    ,	sNet_Liquidating_Value				 decimal(18, 8)
    ,	sInitial_Margin_Amount				 decimal(18, 8)
    ,	sGroup_Initial_Margin_Amount		 decimal(18, 8)
    ,	sCollateral							 decimal(18, 8)
    ,	sExcess_Shortage					 decimal(18, 8)
    ,	sINTEREST							 decimal(18, 8)
    ,	sfileDt								 int
    ,	slastUpdate							 datetime
);



-- create a temp table
create table #loan (
		 Business_Date                      datetime
	,	 Account_Group                      varchar(30)
	,	 Account                            varchar(30)
	,	 Currency                           varchar(30)
	,	 Beginning_Balance					decimal(18, 8)
	,	 Commission							decimal(18, 8)
	,	 Executing_Fee						decimal(18, 8)
	,	 Exchange_Fee						decimal(18, 8)
	,	 NFA_Fees							decimal(18, 8)
	,	 Realised_PnS						decimal(18, 8)
	,	 Option_Premium						decimal(18, 8)
	,	 Cash								decimal(18, 8)
	,	 Ending_Balance						decimal(18, 8)
	,	 Open_Trade_Equity_Futures			decimal(18, 8)
	,	 Total_Equity						decimal(18, 8)
	,	 Long_Option_Value					decimal(18, 8)
	,	 Short_Option_Value					decimal(18, 8)
	,	 Net_Liquidating_Value				decimal(18, 8)
	,	 Initial_Margin_Amount				decimal(18, 8)
	,	 Group_Initial_Margin_Amount		decimal(18, 8)
	,	 Collateral							decimal(18, 8)
	,	 Excess_Shortage					decimal(18, 8)
	,	 INTEREST                           decimal(18, 8)
	,	 fileDt								int
	,	 lastUpdate							datetime
						)



-- bulk insert into temp table
bulk insert #loan 
from 'file_path'		-- placeholder
with (	
		fieldterminator = ',',
		rowterminator = '\n'
	  );



-- merge (upsert)
merge <!! DATABASE NAME !!>.dbo.broker_moneyLineMAQ as target
using #loan as source

on (					-- check constraint
		target.Account = source.Account
	and target.Business_Date = source.Business_Date
	and target.fileDt = source.fileDt	
	)

when matched			-- when there is a match
and (					-- and there is a change
		target.Account_Group              	 <>		source.Account_Group                                
	or	target.Currency                   	 <>		source.Currency                   
	or	target.Beginning_Balance			 <>		source.Beginning_Balance			
	or	target.Commission					 <>		source.Commission					
	or	target.Executing_Fee				 <>		source.Executing_Fee				
	or	target.Exchange_Fee					 <>		source.Exchange_Fee				
	or	target.NFA_Fees						 <>		source.NFA_Fees					
	or	target.Realised_PnS					 <>		source.Realised_PnS				
	or	target.Option_Premium				 <>		source.Option_Premium				
	or	target.Cash							 <>		source.Cash						
	or	target.Ending_Balance				 <>		source.Ending_Balance				
	or	target.Open_Trade_Equity_Futures	 <>		source.Open_Trade_Equity_Futures	
	or	target.Total_Equity					 <>		source.Total_Equity				
	or	target.Long_Option_Value			 <>		source.Long_Option_Value			
	or	target.Short_Option_Value			 <>		source.Short_Option_Value			
	or	target.Net_Liquidating_Value		 <>		source.Net_Liquidating_Value		
	or	target.Initial_Margin_Amount		 <>		source.Initial_Margin_Amount		
	or	target.Group_Initial_Margin_Amount	 <>		source.Group_Initial_Margin_Amount
	or	target.Collateral					 <>		source.Collateral					
	or	target.Excess_Shortage				 <>		source.Excess_Shortage							
	or	target.INTEREST                   	 <>		source.INTEREST         -- skipped lastUpdate <> check
	) 
then 
	update set				-- update data from source to target
		target.Account_Group              	 =		source.Account_Group                
	,	target.Currency                   	 =		source.Currency                   
	,	target.Beginning_Balance			 =		source.Beginning_Balance			
	,	target.Commission					 =		source.Commission					
	,	target.Executing_Fee				 =		source.Executing_Fee				
	,	target.Exchange_Fee					 =		source.Exchange_Fee				
	,	target.NFA_Fees						 =		source.NFA_Fees					
	,	target.Realised_PnS					 =		source.Realised_PnS				
	,	target.Option_Premium				 =		source.Option_Premium				
	,	target.Cash							 =		source.Cash						
	,	target.Ending_Balance				 =		source.Ending_Balance				
	,	target.Open_Trade_Equity_Futures	 =		source.Open_Trade_Equity_Futures	
	,	target.Total_Equity					 =		source.Total_Equity				
	,	target.Long_Option_Value			 =		source.Long_Option_Value			
	,	target.Short_Option_Value			 =		source.Short_Option_Value			
	,	target.Net_Liquidating_Value		 =		source.Net_Liquidating_Value		
	,	target.Initial_Margin_Amount		 =		source.Initial_Margin_Amount		
	,	target.Group_Initial_Margin_Amount	 =		source.Group_Initial_Margin_Amount
	,	target.Collateral					 =		source.Collateral					
	,	target.Excess_Shortage				 =		source.Excess_Shortage				
	,	target.lastUpdate					 =		source.lastUpdate					
	,	target.INTEREST                   	 =		source.INTEREST      


when not matched by target	--	if data from source does not exist in target
then						--  insert from source to target
	insert (
		 Business_Date              
	,	 Account_Group              
	,	 Account                    
	,	 Currency                   
	,	 Beginning_Balance			
	,	 Commission					
	,	 Executing_Fee				
	,	 Exchange_Fee				
	,	 NFA_Fees					
	,	 Realised_PnS				
	,	 Option_Premium				
	,	 Cash						
	,	 Ending_Balance				
	,	 Open_Trade_Equity_Futures	
	,	 Total_Equity				
	,	 Long_Option_Value			
	,	 Short_Option_Value			
	,	 Net_Liquidating_Value		
	,	 Initial_Margin_Amount		
	,	 Group_Initial_Margin_Amount
	,	 Collateral					
	,	 Excess_Shortage			
	,	 fileDt						
	,	 lastUpdate					
	,	 INTEREST                   
			)

	values(
		source.Business_Date
	,	source.Account_Group       
	,	source.Account
	,	source.Currency                   
	,	source.Beginning_Balance			
	,	source.Commission					
	,	source.Executing_Fee				
	,	source.Exchange_Fee				
	,	source.NFA_Fees					
	,	source.Realised_PnS				
	,	source.Option_Premium				
	,	source.Cash						
	,	source.Ending_Balance				
	,	source.Open_Trade_Equity_Futures	
	,	source.Total_Equity				
	,	source.Long_Option_Value			
	,	source.Short_Option_Value			
	,	source.Net_Liquidating_Value		
	,	source.Initial_Margin_Amount		
	,	source.Group_Initial_Margin_Amount
	,	source.Collateral					
	,	source.Excess_Shortage		
	,	source.fileDt
	,	source.lastUpdate					
	,	source.INTEREST      
		)

when not matched by source	-- if data from database does not exist in source
and target.fileDt = (select top 1 fileDt from #loan)
and target.Account like 'GPLNLN%'	-- only for loan

then 
	delete					-- delete from database

-- store action output
OUTPUT 
		$action 
	,	deleted.Business_Date                  	as	tBusiness_Date                  
	,	deleted.Account_Group                  	as	tAccount_Group                  
	,	deleted.Account                        	as	tAccount                        
	,	deleted.Currency                       	as	tCurrency                       
	,	deleted.Beginning_Balance				as	tBeginning_Balance				
	,	deleted.Commission						as	tCommission						
	,	deleted.Executing_Fee					as	tExecuting_Fee					
	,	deleted.Exchange_Fee					as	tExchange_Fee					
	,	deleted.NFA_Fees						as	tNFA_Fees						
	,	deleted.Realised_PnS					as	tRealised_PnS					
	,	deleted.Option_Premium					as	tOption_Premium					
	,	deleted.Cash							as	tCash							
	,	deleted.Ending_Balance					as	tEnding_Balance					
	,	deleted.Open_Trade_Equity_Futures		as	tOpen_Trade_Equity_Futures		
	,	deleted.Total_Equity					as	tTotal_Equity					
	,	deleted.Long_Option_Value				as	tLong_Option_Value				
	,	deleted.Short_Option_Value				as	tShort_Option_Value				
	,	deleted.Net_Liquidating_Value			as	tNet_Liquidating_Value			
	,	deleted.Initial_Margin_Amount			as	tInitial_Margin_Amount			
	,	deleted.Group_Initial_Margin_Amount		as	tGroup_Initial_Margin_Amount	
	,	deleted.Collateral						as	tCollateral						
	,	deleted.Excess_Shortage					as	tExcess_Shortage				
	,	deleted.INTEREST                       	as	tINTEREST                       
	,	deleted.fileDt							as	tfileDt							
	,	deleted.lastUpdate						as	tlastUpdate		
	
	,	inserted.Business_Date                  as	sBusiness_Date                  
	,	inserted.Account_Group                 	as	sAccount_Group                  
	,	inserted.Account                       	as	sAccount                        
	,	inserted.Currency                      	as	sCurrency                       
	,	inserted.Beginning_Balance				as	sBeginning_Balance				
	,	inserted.Commission						as	sCommission						
	,	inserted.Executing_Fee					as	sExecuting_Fee					
	,	inserted.Exchange_Fee					as	sExchange_Fee					
	,	inserted.NFA_Fees						as	sNFA_Fees						
	,	inserted.Realised_PnS					as	sRealised_PnS					
	,	inserted.Option_Premium					as	sOption_Premium					
	,	inserted.Cash							as	sCash							
	,	inserted.Ending_Balance					as	sEnding_Balance					
	,	inserted.Open_Trade_Equity_Futures		as	sOpen_Trade_Equity_Futures		
	,	inserted.Total_Equity					as	sTotal_Equity					
	,	inserted.Long_Option_Value				as	sLong_Option_Value				
	,	inserted.Short_Option_Value				as	sShort_Option_Value				
	,	inserted.Net_Liquidating_Value			as	sNet_Liquidating_Value			
	,	inserted.Initial_Margin_Amount			as	sInitial_Margin_Amount			
	,	inserted.Group_Initial_Margin_Amount	as	sGroup_Initial_Margin_Amount	
	,	inserted.Collateral						as	sCollateral						
	,	inserted.Excess_Shortage				as	sExcess_Shortage				
	,	inserted.INTEREST                       as	sINTEREST                       
	,	inserted.fileDt							as	sfileDt							
	,	inserted.lastUpdate						as	slastUpdate	
	into #merge_output;

-- drop temp table
drop table #loan
