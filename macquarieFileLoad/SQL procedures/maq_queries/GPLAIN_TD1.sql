/****** 

<!! DATABASE NAME !!>.[dbo].[broker_dailytradeMAQ]
Upsert for the prefixe GPLAIN_TD1

CONSTRAINTS: TRADETYPE, CON_REF_NO, fileDt

******/


-- create output table
CREATE TABLE #merge_output (
    	action								varchar(10)
	,	tTRADEDATE							int
	,	tACCT1								varchar(30)
	,	tACCT2								varchar(30)
	,	tTRADEPRICE							decimal(18, 8)
	,	tMATURITY							int
	,	tPRODUCTNAME						varchar(60)
	,	tEXCHANGE							varchar(30)
	,	tFUTURESCODE						varchar(30)
	,	tB_S								int
	,	tQUANTITY							decimal(18, 8)
	,	tTRADETYPE							varchar(30)
	,	tPUTORCALL							varchar(30)
	,	tSTRIKEPRICE						decimal(18, 8)
	,	tCURRENCY							varchar(30)
	,	tCOMMISSION							decimal(18, 8)
	,	tEXCHANGEFEE						decimal(18, 8)
	,	tNFAFEE								decimal(18, 8)
	,	tBROKERAGE							decimal(18, 8)
	,	tEXEC_COMMS							decimal(18, 8)
	,	tTOTAL_FEES							decimal(18, 8)
	,	tOTHERFEE							decimal(18, 8)
	,	tGIVEINFEE							decimal(18, 8)
	,	tEXPIRYDATE							int
	,	tNOTIONAL_VALUE						decimal(18, 8)
	,	tNOTIONAL_VALUE_PLUS_FEES			decimal(18, 8)
	,	tLASTTRADEDATE						int
	,	tSETTLEMENTDATE						int
	,	tBUSINESSDATE						int
	,	tOPT_EXERCISE_STYLE					varchar(30)
	,	tTRADE_TYPE							varchar(30)
	,	tTRADING_VENUE						varchar(30)
	,	tLOT_SIZE							decimal(18, 8)
	,	tCON_REF_NO							varchar(30)
	,	tSETTLEMENT_PRICE					decimal(18, 8)
	,	tUNDERLYING_SETTLEMENT_PRICE		decimal(18, 8)
	,	tDELTA								decimal(18, 8)
	,	tfileDt								int
	,	tlastUpdate							datetime
	,	tACCOUNT_NAME						varchar(60)
	,	tEXEC_BROKER						varchar(30)
	,	sTRADEDATE							int
	,	sACCT1								varchar(30)
	,	sACCT2								varchar(30)
	,	sTRADEPRICE							decimal(18, 8)
	,	sMATURITY							int
	,	sPRODUCTNAME						varchar(60)
	,	sEXCHANGE							varchar(30)
	,	sFUTURESCODE						varchar(30)
	,	sB_S								int
	,	sQUANTITY							decimal(18, 8)
	,	sTRADETYPE							varchar(30)
	,	sPUTORCALL							varchar(30)
	,	sSTRIKEPRICE						decimal(18, 8)
	,	sCURRENCY							varchar(30)
	,	sCOMMISSION							decimal(18, 8)
	,	sEXCHANGEFEE						decimal(18, 8)
	,	sNFAFEE								decimal(18, 8)
	,	sBROKERAGE							decimal(18, 8)
	,	sEXEC_COMMS							decimal(18, 8)
	,	sTOTAL_FEES							decimal(18, 8)
	,	sOTHERFEE							decimal(18, 8)
	,	sGIVEINFEE							decimal(18, 8)
	,	sEXPIRYDATE							int
	,	sNOTIONAL_VALUE						decimal(18, 8)
	,	sNOTIONAL_VALUE_PLUS_FEES			decimal(18, 8)
	,	sLASTTRADEDATE						int
	,	sSETTLEMENTDATE						int
	,	sBUSINESSDATE						int
	,	sOPT_EXERCISE_STYLE					varchar(30)
	,	sTRADE_TYPE							varchar(30)
	,	sTRADING_VENUE						varchar(30)
	,	sLOT_SIZE							decimal(18, 8)
	,	sCON_REF_NO							varchar(30)
	,	sSETTLEMENT_PRICE					decimal(18, 8)
	,	sUNDERLYING_SETTLEMENT_PRICE		decimal(18, 8)
	,	sDELTA								decimal(18, 8)
	,	sfileDt								int
	,	slastUpdate							datetime
	,	sACCOUNT_NAME						varchar(60)
	,	sEXEC_BROKER						varchar(30)
);



-- create a temp table
create table #dailytrade (
		TRADEDATE							int
	,	ACCT1								varchar(30)
	,	ACCT2								varchar(30)
	,	TRADEPRICE							decimal(18, 8)
	,	MATURITY							int
	,	PRODUCTNAME							varchar(60)
	,	EXCHANGE							varchar(30)
	,	FUTURESCODE							varchar(30)
	,	B_S									int
	,	QUANTITY							decimal(18, 8)
	,	TRADETYPE							varchar(30)
	,	PUTORCALL							varchar(30)
	,	STRIKEPRICE							decimal(18, 8)
	,	CURRENCY							varchar(30)
	,	COMMISSION							decimal(18, 8)
	,	EXCHANGEFEE							decimal(18, 8)
	,	NFAFEE								decimal(18, 8)
	,	BROKERAGE							decimal(18, 8)
	,	EXEC_COMMS							decimal(18, 8)
	,	TOTAL_FEES							decimal(18, 8)
	,	OTHERFEE							decimal(18, 8)
	,	GIVEINFEE							decimal(18, 8)
	,	EXPIRYDATE							int
	,	NOTIONAL_VALUE						decimal(18, 8)
	,	NOTIONAL_VALUE_PLUS_FEES			decimal(18, 8)
	,	LASTTRADEDATE						int
	,	SETTLEMENTDATE						int
	,	BUSINESSDATE						int
	,	OPT_EXERCISE_STYLE					varchar(30)
	,	TRADE_TYPE							varchar(30)
	,	TRADING_VENUE						varchar(30)
	,	LOT_SIZE							decimal(18, 8)
	,	CON_REF_NO							varchar(30)
	,	SETTLEMENT_PRICE					decimal(18, 8)
	,	UNDERLYING_SETTLEMENT_PRICE			decimal(18, 8)
	,	DELTA								decimal(18, 8)
	,	ACCOUNT_NAME						varchar(60)
	,	EXEC_BROKER							varchar(30)
	,	fileDt								varchar(60)
	,	lastUpdate							varchar(30)
						)



-- bulk insert into temp table
bulk insert #dailytrade 
from 'file_path'		-- placeholder
with (	
		fieldterminator = ',',
		rowterminator = '\n'
	  );


-- merge (upsert)
merge <!! DATABASE NAME !!>.dbo.broker_dailyTradesMAQ as target
using #dailytrade as source

on (					-- check constraint
		target.TRADETYPE	= source.TRADETYPE
	and target.CON_REF_NO	= source.CON_REF_NO
	and target.fileDt		= cast(source.fileDt as int)	
	)

when matched			-- when there is a match
and (					-- and there is a change
		target.TRADEDATE					 <>		source.TRADEDATE					
	or	target.ACCT1						 <>		source.ACCT1						
	or	target.ACCT2						 <>		source.ACCT2						
	or	target.TRADEPRICE					 <>		source.TRADEPRICE					
	or	target.MATURITY						 <>		source.MATURITY					
	or	target.PRODUCTNAME					 <>		source.PRODUCTNAME					
	or	target.EXCHANGE						 <>		source.EXCHANGE					
	or	target.FUTURESCODE					 <>		source.FUTURESCODE					
	or	target.B_S							 <>		source.B_S							
	or	target.QUANTITY						 <>		source.QUANTITY									
	or	target.PUTORCALL					 <>		source.PUTORCALL					
	or	target.STRIKEPRICE					 <>		source.STRIKEPRICE					
	or	target.CURRENCY						 <>		source.CURRENCY					
	or	target.COMMISSION					 <>		source.COMMISSION					
	or	target.EXCHANGEFEE					 <>		source.EXCHANGEFEE					
	or	target.NFAFEE						 <>		source.NFAFEE						
	or	target.BROKERAGE					 <>		source.BROKERAGE					
	or	target.EXEC_COMMS					 <>		source.EXEC_COMMS					
	or	target.TOTAL_FEES					 <>		source.TOTAL_FEES					
	or	target.OTHERFEE						 <>		source.OTHERFEE					
	or	target.GIVEINFEE					 <>		source.GIVEINFEE					
	or	target.EXPIRYDATE					 <>		source.EXPIRYDATE					
	or	target.NOTIONAL_VALUE				 <>		source.NOTIONAL_VALUE				
	or	target.NOTIONAL_VALUE_PLUS_FEES		 <>		source.NOTIONAL_VALUE_PLUS_FEES	
	or	target.LASTTRADEDATE				 <>		source.LASTTRADEDATE				
	or	target.SETTLEMENTDATE				 <>		source.SETTLEMENTDATE				
	or	target.BUSINESSDATE					 <>		source.BUSINESSDATE				
	or	target.OPT_EXERCISE_STYLE			 <>		source.OPT_EXERCISE_STYLE			
	or	target.TRADE_TYPE					 <>		source.TRADE_TYPE					
	or	target.TRADING_VENUE				 <>		source.TRADING_VENUE				
	or	target.LOT_SIZE						 <>		source.LOT_SIZE									
	or	target.SETTLEMENT_PRICE				 <>		source.SETTLEMENT_PRICE			
	or	target.UNDERLYING_SETTLEMENT_PRICE	 <>		source.UNDERLYING_SETTLEMENT_PRICE	
	or	target.DELTA						 <>		source.DELTA											
	or	target.lastUpdate					 <>		convert(datetime, source.lastUpdate) 					
	or	target.ACCOUNT_NAME					 <>		source.ACCOUNT_NAME				
	or	target.EXEC_BROKER					 <>		source.EXEC_BROKER					
	)
then 
	update set				-- update data from source to target
		target.TRADEDATE					 =		source.TRADEDATE					
	,	target.ACCT1						 =		source.ACCT1						
	,	target.ACCT2						 =		source.ACCT2									
	,	target.MATURITY						 =		source.MATURITY					
	,	target.PRODUCTNAME					 =		source.PRODUCTNAME					
	,	target.EXCHANGE						 =		source.EXCHANGE					
	,	target.FUTURESCODE					 =		source.FUTURESCODE					
	,	target.B_S							 =		source.B_S							
	,	target.QUANTITY						 =		source.QUANTITY					
	,	target.TRADETYPE					 =		source.TRADETYPE					
	,	target.PUTORCALL					 =		source.PUTORCALL					
	,	target.STRIKEPRICE					 =		source.STRIKEPRICE					
	,	target.CURRENCY						 =		source.CURRENCY					
	,	target.COMMISSION					 =		source.COMMISSION					
	,	target.EXCHANGEFEE					 =		source.EXCHANGEFEE					
	,	target.NFAFEE						 =		source.NFAFEE						
	,	target.BROKERAGE					 =		source.BROKERAGE					
	,	target.EXEC_COMMS					 =		source.EXEC_COMMS					
	,	target.TOTAL_FEES					 =		source.TOTAL_FEES					
	,	target.OTHERFEE						 =		source.OTHERFEE					
	,	target.GIVEINFEE					 =		source.GIVEINFEE					
	,	target.EXPIRYDATE					 =		source.EXPIRYDATE					
	,	target.NOTIONAL_VALUE				 =		source.NOTIONAL_VALUE				
	,	target.NOTIONAL_VALUE_PLUS_FEES		 =		source.NOTIONAL_VALUE_PLUS_FEES	
	,	target.LASTTRADEDATE				 =		source.LASTTRADEDATE				
	,	target.SETTLEMENTDATE				 =		source.SETTLEMENTDATE				
	,	target.BUSINESSDATE					 =		source.BUSINESSDATE				
	,	target.OPT_EXERCISE_STYLE			 =		source.OPT_EXERCISE_STYLE			
	,	target.TRADE_TYPE					 =		source.TRADE_TYPE					
	,	target.TRADING_VENUE				 =		source.TRADING_VENUE				
	,	target.LOT_SIZE						 =		source.LOT_SIZE										
	,	target.SETTLEMENT_PRICE				 =		source.SETTLEMENT_PRICE			
	,	target.UNDERLYING_SETTLEMENT_PRICE	 =		source.UNDERLYING_SETTLEMENT_PRICE	
	,	target.DELTA						 =		source.DELTA									
	,	target.lastUpdate					 =		convert(datetime, source.lastUpdate) 					
	,	target.ACCOUNT_NAME					 =		source.ACCOUNT_NAME				
	,	target.EXEC_BROKER					 =		source.EXEC_BROKER								


when not matched by target	--	if data from source does not exist in target
then						--  insert from source to target
	insert (
		TRADEDATE					
	,	ACCT1						
	,	ACCT2						
	,	TRADEPRICE					
	,	MATURITY					
	,	PRODUCTNAME					
	,	EXCHANGE					
	,	FUTURESCODE					
	,	B_S							
	,	QUANTITY					
	,	TRADETYPE					
	,	PUTORCALL					
	,	STRIKEPRICE					
	,	CURRENCY					
	,	COMMISSION					
	,	EXCHANGEFEE					
	,	NFAFEE						
	,	BROKERAGE					
	,	EXEC_COMMS					
	,	TOTAL_FEES					
	,	OTHERFEE					
	,	GIVEINFEE					
	,	EXPIRYDATE					
	,	NOTIONAL_VALUE				
	,	NOTIONAL_VALUE_PLUS_FEES	
	,	LASTTRADEDATE				
	,	SETTLEMENTDATE				
	,	BUSINESSDATE				
	,	OPT_EXERCISE_STYLE			
	,	TRADE_TYPE					
	,	TRADING_VENUE				
	,	LOT_SIZE					
	,	CON_REF_NO					
	,	SETTLEMENT_PRICE			
	,	UNDERLYING_SETTLEMENT_PRICE	
	,	DELTA						
	,	fileDt						
	,	lastUpdate					
	,	ACCOUNT_NAME				
	,	EXEC_BROKER					               
			)

	values(
		source.TRADEDATE					
	,	source.ACCT1						
	,	source.ACCT2						
	,	source.TRADEPRICE					
	,	source.MATURITY					
	,	source.PRODUCTNAME					
	,	source.EXCHANGE					
	,	source.FUTURESCODE					
	,	source.B_S							
	,	source.QUANTITY					
	,	source.TRADETYPE					
	,	source.PUTORCALL					
	,	source.STRIKEPRICE					
	,	source.CURRENCY					
	,	source.COMMISSION					
	,	source.EXCHANGEFEE					
	,	source.NFAFEE						
	,	source.BROKERAGE					
	,	source.EXEC_COMMS					
	,	source.TOTAL_FEES					
	,	source.OTHERFEE					
	,	source.GIVEINFEE					
	,	source.EXPIRYDATE					
	,	source.NOTIONAL_VALUE				
	,	source.NOTIONAL_VALUE_PLUS_FEES	
	,	source.LASTTRADEDATE				
	,	source.SETTLEMENTDATE				
	,	source.BUSINESSDATE				
	,	source.OPT_EXERCISE_STYLE			
	,	source.TRADE_TYPE					
	,	source.TRADING_VENUE				
	,	source.LOT_SIZE					
	,	source.CON_REF_NO					
	,	source.SETTLEMENT_PRICE			
	,	source.UNDERLYING_SETTLEMENT_PRICE	
	,	source.DELTA						
	,	cast(source.fileDt as int)						
	,	convert(datetime, source.lastUpdate) 					
	,	source.ACCOUNT_NAME				
	,	source.EXEC_BROKER	  
		)

when not matched by source	-- if data from database does not exist in source
and target.fileDt = (select top 1 fileDt from #dailytrade)	
then 
	delete					-- delete from database

-- store action output
OUTPUT
		$action 
	,	deleted.TRADEDATE						as		tTRADEDATE					
	,	deleted.ACCT1							as		tACCT1						
	,	deleted.ACCT2							as		tACCT2						
	,	deleted.TRADEPRICE						as		tTRADEPRICE					
	,	deleted.MATURITY						as		tMATURITY					
	,	deleted.PRODUCTNAME						as		tPRODUCTNAME					
	,	deleted.EXCHANGE						as		tEXCHANGE					
	,	deleted.FUTURESCODE						as		tFUTURESCODE					
	,	deleted.B_S								as		tB_S							
	,	deleted.QUANTITY						as		tQUANTITY					
	,	deleted.TRADETYPE						as		tTRADETYPE					
	,	deleted.PUTORCALL						as		tPUTORCALL					
	,	deleted.STRIKEPRICE						as		tSTRIKEPRICE					
	,	deleted.CURRENCY						as		tCURRENCY					
	,	deleted.COMMISSION						as		tCOMMISSION					
	,	deleted.EXCHANGEFEE						as		tEXCHANGEFEE					
	,	deleted.NFAFEE							as		tNFAFEE						
	,	deleted.BROKERAGE						as		tBROKERAGE					
	,	deleted.EXEC_COMMS						as		tEXEC_COMMS					
	,	deleted.TOTAL_FEES						as		tTOTAL_FEES					
	,	deleted.OTHERFEE						as		tOTHERFEE					
	,	deleted.GIVEINFEE						as		tGIVEINFEE					
	,	deleted.EXPIRYDATE						as		tEXPIRYDATE					
	,	deleted.NOTIONAL_VALUE					as		tNOTIONAL_VALUE				
	,	deleted.NOTIONAL_VALUE_PLUS_FEES		as		tNOTIONAL_VALUE_PLUS_FEES	
	,	deleted.LASTTRADEDATE					as		tLASTTRADEDATE				
	,	deleted.SETTLEMENTDATE					as		tSETTLEMENTDATE				
	,	deleted.BUSINESSDATE					as		tBUSINESSDATE				
	,	deleted.OPT_EXERCISE_STYLE				as		tOPT_EXERCISE_STYLE			
	,	deleted.TRADE_TYPE						as		tTRADE_TYPE					
	,	deleted.TRADING_VENUE					as		tTRADING_VENUE				
	,	deleted.LOT_SIZE						as		tLOT_SIZE					
	,	deleted.CON_REF_NO						as		tCON_REF_NO					
	,	deleted.SETTLEMENT_PRICE				as		tSETTLEMENT_PRICE			
	,	deleted.UNDERLYING_SETTLEMENT_PRICE		as		tUNDERLYING_SETTLEMENT_PRICE	
	,	deleted.DELTA							as		tDELTA						
	,	deleted.fileDt							as		tfileDt						
	,	deleted.lastUpdate						as		tlastUpdate					
	,	deleted.ACCOUNT_NAME					as		tACCOUNT_NAME				
	,	deleted.EXEC_BROKER						as		tEXEC_BROKER	

	,	inserted.TRADEDATE						as		iTRADEDATE					
	,	inserted.ACCT1							as		iACCT1						
	,	inserted.ACCT2							as		iACCT2						
	,	inserted.TRADEPRICE						as		iTRADEPRICE					
	,	inserted.MATURITY						as		iMATURITY					
	,	inserted.PRODUCTNAME					as		iPRODUCTNAME				
	,	inserted.EXCHANGE						as		iEXCHANGE					
	,	inserted.FUTURESCODE					as		iFUTURESCODE				
	,	inserted.B_S							as		iB_S						
	,	inserted.QUANTITY						as		iQUANTITY					
	,	inserted.TRADETYPE						as		iTRADETYPE					
	,	inserted.PUTORCALL						as		iPUTORCALL					
	,	inserted.STRIKEPRICE					as		iSTRIKEPRICE				
	,	inserted.CURRENCY						as		iCURRENCY					
	,	inserted.COMMISSION						as		iCOMMISSION					
	,	inserted.EXCHANGEFEE					as		iEXCHANGEFEE				
	,	inserted.NFAFEE							as		iNFAFEE						
	,	inserted.BROKERAGE						as		iBROKERAGE					
	,	inserted.EXEC_COMMS						as		iEXEC_COMMS					
	,	inserted.TOTAL_FEES						as		iTOTAL_FEES					
	,	inserted.OTHERFEE						as		iOTHERFEE					
	,	inserted.GIVEINFEE						as		iGIVEINFEE					
	,	inserted.EXPIRYDATE						as		iEXPIRYDATE					
	,	inserted.NOTIONAL_VALUE					as		iNOTIONAL_VALUE				
	,	inserted.NOTIONAL_VALUE_PLUS_FEES		as		iNOTIONAL_VALUE_PLUS_FEES	
	,	inserted.LASTTRADEDATE					as		iLASTTRADEDATE				
	,	inserted.SETTLEMENTDATE					as		iSETTLEMENTDATE				
	,	inserted.BUSINESSDATE					as		iBUSINESSDATE				
	,	inserted.OPT_EXERCISE_STYLE				as		iOPT_EXERCISE_STYLE			
	,	inserted.TRADE_TYPE						as		iTRADE_TYPE					
	,	inserted.TRADING_VENUE					as		iTRADING_VENUE				
	,	inserted.LOT_SIZE						as		iLOT_SIZE					
	,	inserted.CON_REF_NO						as		iCON_REF_NO					
	,	inserted.SETTLEMENT_PRICE				as		iSETTLEMENT_PRICE			
	,	inserted.UNDERLYING_SETTLEMENT_PRICE	as		iUNDERLYING_SETTLEMENT_PRICE
	,	inserted.DELTA							as		iDELTA						
	,	inserted.fileDt							as		ifileDt						
	,	inserted.lastUpdate						as		ilastUpdate					
	,	inserted.ACCOUNT_NAME					as		iACCOUNT_NAME				
	,	inserted.EXEC_BROKER					as		iEXEC_BROKER				

into #merge_output;

-- drop temp table
drop table #dailytrade