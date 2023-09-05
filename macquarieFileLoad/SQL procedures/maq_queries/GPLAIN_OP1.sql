/****** 

<!! DATABASE NAME !!>.[dbo].[broker_openPositionsMAQ]
Upsert for the prefix GPLAIN_OP1

CONSTRAINTS: UNIQUE_TRADE_NUMBER, fileDt

******/


-- create output table
create table #merge_output (
    	action						varchar(10)
	,	tACCOUNT1					varchar(60)
	,	tACCOUNT2					varchar(60)
	,	tQUANTITY					varchar(60)
	,	tSECURITY_DESC				varchar(60)
	,	tBUY_SELL					int
	,	tTRADE_PRICE				decimal(18, 8)
	,	tTRADEDATE					int
	,	tCLOSING_PRICE				decimal(18, 8)
	,	tPREVIOUS_CLOSING_PRICE		decimal(18, 8)
	,	tCURRENT_DT					varchar(60)
	,	tEXCHANGE					varchar(60)
	,	tCODE						varchar(60)
	,	tCURRENCY					varchar(60)
	,	tTICK_VALUE					decimal(18, 8)
	,	tNOMINAL_VALUE				decimal(18, 8)
	,	tNOMINAL_VALUE_USD			decimal(18, 8)
	,	tPROMPT						varchar(60)
	,	tUSD_RATE					decimal(18, 8)
	,	tBB_CODE					varchar(60)
	,	tBB_NAME					varchar(60)
	,	tMARK_TO_MARKET				decimal(18, 8)
	,	tCALL_PUT					varchar(60)
	,	tSTRIKE						decimal(18, 8)
	,	tEXECUTING_BROKER			varchar(60)
	,	tTRADE_TYPE					varchar(60)
	,	tEXPIRATION_DATE			varchar(60)
	,	tUNIQUE_TRADE_NUMBER		varchar(60)
	,	tCONTRACT_SIZE				decimal(18, 8)
	,	tDELTA						decimal(18, 8)
	,	tfileDt						int
	,	tlastUpdate					datetime
	,	sACCOUNT1					varchar(60)
	,	sACCOUNT2					varchar(60)
	,	sQUANTITY					varchar(60)
	,	sSECURITY_DESC				varchar(60)
	,	sBUY_SELL					int
	,	sTRADE_PRICE				decimal(18, 8)
	,	sTRADEDATE					int
	,	sCLOSING_PRICE				decimal(18, 8)
	,	sPREVIOUS_CLOSING_PRICE		decimal(18, 8)
	,	sCURRENT_DT					varchar(60)
	,	sEXCHANGE					varchar(60)
	,	sCODE						varchar(60)
	,	sCURRENCY					varchar(60)
	,	sTICK_VALUE					decimal(18, 8)
	,	sNOMINAL_VALUE				decimal(18, 8)
	,	sNOMINAL_VALUE_USD			decimal(18, 8)
	,	sPROMPT						varchar(60)
	,	sUSD_RATE					decimal(18, 8)
	,	sBB_CODE					varchar(60)
	,	sBB_NAME					varchar(60)
	,	sMARK_TO_MARKET				decimal(18, 8)
	,	sCALL_PUT					varchar(60)
	,	sSTRIKE						decimal(18, 8)
	,	sEXECUTING_BROKER			varchar(60)
	,	sTRADE_TYPE					varchar(60)
	,	sEXPIRATION_DATE			varchar(60)
	,	sUNIQUE_TRADE_NUMBER		varchar(60)
	,	sCONTRACT_SIZE				decimal(18, 8)
	,	sDELTA						decimal(18, 8)
	,	sfileDt						int
	,	slastUpdate					datetime
);



-- create a temp table
create table #openpos (
		ACCOUNT1					varchar(60)			
	,	ACCOUNT2					varchar(60)
	,	QUANTITY					varchar(60)
	,	SECURITY_DESC				varchar(60)
	,	BUY_SELL					int
	,	TRADE_PRICE					decimal(18, 8)
	,	TRADEDATE					int
	,	CLOSING_PRICE				decimal(18, 8)
	,	PREVIOUS_CLOSING_PRICE		decimal(18, 8)
	,	UNDER_CLOSING				varchar(60)  
	,	CURRENT_DT					varchar(60)
	,	EXCHANGE					varchar(60)
	,	CODE						varchar(60)
	,	CURRENCY					varchar(60)
	,	TICK_VALUE					varchar(60)
	,	NOMINAL_VALUE				decimal(18, 8)
	,	NOMINAL_VALUE_USD			decimal(18, 8)
	,	PROMPT						varchar(60)
	,	USD_RATE					varchar(60)
	,	BB_CODE						varchar(60)
	,	BB_NAME						varchar(60)
	,	MARK_TO_MARKET				varchar(60)
	,	CALL_PUT					varchar(60)
	,	STRIKE						varchar(60)
	,	EXECUTING_BROKER			varchar(60)
	,	TRADE_TYPE					varchar(60)
	,	EXPIRATION_DATE				varchar(60)
	,	UNIQUE_TRADE_NUMBER			varchar(60)
	,	CONTRACT_SIZE				decimal(18, 8)
	,	DELTA						decimal(18, 8)
	,	fileDt						varchar(60)
	,	lastUpdate					varchar(60)
						)



-- bulk insert into temp table
bulk insert #openpos 
from 'file_path'		-- placeholder
with (	
		fieldterminator = ',',
		rowterminator = '\n'
	  );



-- merge (upsert)
merge <!! DATABASE NAME !!>.dbo.broker_openPositionsMAQ as target
using #openpos as source

on (					-- check constraint
		target.UNIQUE_TRADE_NUMBER = source.UNIQUE_TRADE_NUMBER
	and target.fileDt = cast(source.fileDt as int)	
	)

when matched			-- when there is a match
and (					-- and there is a change
		target.ACCOUNT1					<>	source.ACCOUNT1				
	or	target.ACCOUNT2					<>	source.ACCOUNT2				
	or	target.QUANTITY					<>	source.QUANTITY				
	or	target.SECURITY_DESC			<>	source.SECURITY_DESC			
	or	target.BUY_SELL					<>	source.BUY_SELL				
	or	target.TRADE_PRICE				<>	source.TRADE_PRICE				
	or	target.TRADEDATE				<>	source.TRADEDATE				
	or	target.CLOSING_PRICE			<>	source.CLOSING_PRICE			
	or	target.PREVIOUS_CLOSING_PRICE	<>	source.PREVIOUS_CLOSING_PRICE	
	or	target.CURRENT_DT				<>	source.CURRENT_DT				
	or	target.EXCHANGE					<>	source.EXCHANGE				
	or	target.CODE						<>	source.CODE					
	or	target.CURRENCY					<>	source.CURRENCY				
	or	target.TICK_VALUE				<>	cast(source.TICK_VALUE as decimal(18, 8)) 				
	or	target.NOMINAL_VALUE			<>	source.NOMINAL_VALUE			
	or	target.NOMINAL_VALUE_USD		<>	source.NOMINAL_VALUE_USD		
	or	target.PROMPT					<>	source.PROMPT					
	or	target.USD_RATE					<>	cast(source.USD_RATE as decimal(18, 8))				
	or	target.BB_CODE					<>	source.BB_CODE					
	or	target.BB_NAME					<>	source.BB_NAME					
	or	target.MARK_TO_MARKET			<>	cast(source.MARK_TO_MARKET as decimal(18, 8))			
	or	target.CALL_PUT					<>	source.CALL_PUT				
	or	target.STRIKE					<>	cast(source.STRIKE as decimal(18,8))				
	or	target.EXECUTING_BROKER			<>	source.EXECUTING_BROKER		
	or	target.TRADE_TYPE				<>	source.TRADE_TYPE				
	or	target.EXPIRATION_DATE			<>	source.EXPIRATION_DATE			
	or	target.CONTRACT_SIZE			<>	source.CONTRACT_SIZE			
	or	target.DELTA					<>	source.DELTA			-- skip check for lastUpdate <>
	) 
then 
	update set				-- update data from source to target
		target.ACCOUNT1					=	source.ACCOUNT1				
	,	target.ACCOUNT2					=	source.ACCOUNT2				
	,	target.QUANTITY					=	source.QUANTITY				
	,	target.SECURITY_DESC			=	source.SECURITY_DESC			
	,	target.BUY_SELL					=	source.BUY_SELL				
	,	target.TRADE_PRICE				=	source.TRADE_PRICE				
	,	target.TRADEDATE				=	source.TRADEDATE				
	,	target.CLOSING_PRICE			=	source.CLOSING_PRICE			
	,	target.PREVIOUS_CLOSING_PRICE	=	source.PREVIOUS_CLOSING_PRICE	
	,	target.CURRENT_DT				=	source.CURRENT_DT				
	,	target.EXCHANGE					=	source.EXCHANGE				
	,	target.CODE						=	source.CODE					
	,	target.CURRENCY					=	source.CURRENCY				
	,	target.TICK_VALUE				=	cast(source.TICK_VALUE as decimal(18, 8))
	,	target.NOMINAL_VALUE			=	source.NOMINAL_VALUE			
	,	target.NOMINAL_VALUE_USD		=	source.NOMINAL_VALUE_USD		
	,	target.PROMPT					=	source.PROMPT					
	,	target.USD_RATE					=	cast(source.USD_RATE as decimal(18, 8))
	,	target.BB_CODE					=	source.BB_CODE					
	,	target.BB_NAME					=	source.BB_NAME					
	,	target.MARK_TO_MARKET			=	cast(source.MARK_TO_MARKET as decimal(18, 8))			
	,	target.CALL_PUT					=	source.CALL_PUT				
	,	target.STRIKE					=	cast(source.STRIKE as decimal(18,8))				
	,	target.EXECUTING_BROKER			=	source.EXECUTING_BROKER		
	,	target.TRADE_TYPE				=	source.TRADE_TYPE				
	,	target.EXPIRATION_DATE			=	source.EXPIRATION_DATE			
	,	target.UNIQUE_TRADE_NUMBER		=	source.UNIQUE_TRADE_NUMBER		
	,	target.CONTRACT_SIZE			=	source.CONTRACT_SIZE			
	,	target.DELTA					=	source.DELTA					
	,	target.fileDt					=	cast(source.fileDt as int)					
	,	target.lastUpdate				=	source.lastUpdate				

	
when not matched by target	--	if data from source does not exist in target
then						--  insert from source to target
	insert (
		ACCOUNT1				
	,	ACCOUNT2				
	,	QUANTITY				
	,	SECURITY_DESC			
	,	BUY_SELL				
	,	TRADE_PRICE				
	,	TRADEDATE				
	,	CLOSING_PRICE			
	,	PREVIOUS_CLOSING_PRICE	
	,	CURRENT_DT				
	,	EXCHANGE				
	,	CODE					
	,	CURRENCY				
	,	TICK_VALUE				
	,	NOMINAL_VALUE			
	,	NOMINAL_VALUE_USD		
	,	PROMPT					
	,	USD_RATE				
	,	BB_CODE					
	,	BB_NAME					
	,	MARK_TO_MARKET			
	,	CALL_PUT				
	,	STRIKE					
	,	EXECUTING_BROKER		
	,	TRADE_TYPE				
	,	EXPIRATION_DATE			
	,	UNIQUE_TRADE_NUMBER		
	,	CONTRACT_SIZE			
	,	DELTA					
	,	fileDt					
	,	lastUpdate				               
			)

	values(
		source.ACCOUNT1				
	,	source.ACCOUNT2				
	,	source.QUANTITY				
	,	source.SECURITY_DESC			
	,	source.BUY_SELL				
	,	source.TRADE_PRICE				
	,	source.TRADEDATE				
	,	source.CLOSING_PRICE			
	,	source.PREVIOUS_CLOSING_PRICE	
	,	source.CURRENT_DT				
	,	source.EXCHANGE				
	,	source.CODE					
	,	source.CURRENCY				
	,	cast(source.TICK_VALUE as decimal(18, 8)) 				
	,	source.NOMINAL_VALUE			
	,	source.NOMINAL_VALUE_USD		
	,	source.PROMPT					
	,	cast(source.USD_RATE as decimal(18, 8))				
	,	source.BB_CODE					
	,	source.BB_NAME					
	,	cast(source.MARK_TO_MARKET as decimal(18, 8))			
	,	source.CALL_PUT				
	,	cast(source.STRIKE as decimal(18,8))				
	,	source.EXECUTING_BROKER		
	,	source.TRADE_TYPE				
	,	source.EXPIRATION_DATE			
	,	source.UNIQUE_TRADE_NUMBER		
	,	source.CONTRACT_SIZE			
	,	source.DELTA					
	,	cast(source.fileDt as int)					
	,	source.lastUpdate				    
		)

when not matched by source	-- if data from database does not exist in source
and target.fileDt = (select top 1 fileDt from #openpos)
then 
	delete					-- delete from database

-- store action output
OUTPUT
		$action 
	,	deleted.ACCOUNT1					as	tACCOUNT1				
	,	deleted.ACCOUNT2					as	tACCOUNT2				
	,	deleted.QUANTITY					as	tQUANTITY				
	,	deleted.SECURITY_DESC				as	tSECURITY_DESC			
	,	deleted.BUY_SELL					as	tBUY_SELL				
	,	deleted.TRADE_PRICE					as	tTRADE_PRICE				
	,	deleted.TRADEDATE					as	tTRADEDATE				
	,	deleted.CLOSING_PRICE				as	tCLOSING_PRICE			
	,	deleted.PREVIOUS_CLOSING_PRICE		as	tPREVIOUS_CLOSING_PRICE	
	,	deleted.CURRENT_DT					as	tCURRENT_DT				
	,	deleted.EXCHANGE					as	tEXCHANGE				
	,	deleted.CODE						as	tCODE					
	,	deleted.CURRENCY					as	tCURRENCY				
	,	deleted.TICK_VALUE					as	tTICK_VALUE				
	,	deleted.NOMINAL_VALUE				as	tNOMINAL_VALUE			
	,	deleted.NOMINAL_VALUE_USD			as	tNOMINAL_VALUE_USD		
	,	deleted.PROMPT						as	tPROMPT					
	,	deleted.USD_RATE					as	tUSD_RATE				
	,	deleted.BB_CODE						as	tBB_CODE					
	,	deleted.BB_NAME						as	tBB_NAME					
	,	deleted.MARK_TO_MARKET				as	tMARK_TO_MARKET			
	,	deleted.CALL_PUT					as	tCALL_PUT				
	,	deleted.STRIKE						as	tSTRIKE					
	,	deleted.EXECUTING_BROKER			as	tEXECUTING_BROKER		
	,	deleted.TRADE_TYPE					as	tTRADE_TYPE				
	,	deleted.EXPIRATION_DATE				as	tEXPIRATION_DATE			
	,	deleted.UNIQUE_TRADE_NUMBER			as	tUNIQUE_TRADE_NUMBER		
	,	deleted.CONTRACT_SIZE				as	tCONTRACT_SIZE			
	,	deleted.DELTA						as	tDELTA					
	,	deleted.fileDt						as	tfileDt					
	,	deleted.lastUpdate					as	tlastUpdate				
	
	,	inserted.ACCOUNT1					as	sACCOUNT1				
	,	inserted.ACCOUNT2					as	sACCOUNT2				
	,	inserted.QUANTITY					as	sQUANTITY				
	,	inserted.SECURITY_DESC				as	sSECURITY_DESC			
	,	inserted.BUY_SELL					as	sBUY_SELL				
	,	inserted.TRADE_PRICE				as	sTRADE_PRICE					
	,	inserted.TRADEDATE					as	sTRADEDATE				
	,	inserted.CLOSING_PRICE				as	sCLOSING_PRICE			
	,	inserted.PREVIOUS_CLOSING_PRICE		as	sPREVIOUS_CLOSING_PRICE	
	,	inserted.CURRENT_DT					as	sCURRENT_DT				
	,	inserted.EXCHANGE					as	sEXCHANGE				
	,	inserted.CODE						as	sCODE					
	,	inserted.CURRENCY					as	sCURRENCY				
	,	inserted.TICK_VALUE					as	sTICK_VALUE				
	,	inserted.NOMINAL_VALUE				as	sNOMINAL_VALUE			
	,	inserted.NOMINAL_VALUE_USD			as	sNOMINAL_VALUE_USD		
	,	inserted.PROMPT						as	sPROMPT					
	,	inserted.USD_RATE					as	sUSD_RATE				
	,	inserted.BB_CODE					as	sBB_CODE						
	,	inserted.BB_NAME					as	sBB_NAME						
	,	inserted.MARK_TO_MARKET				as	sMARK_TO_MARKET			
	,	inserted.CALL_PUT					as	sCALL_PUT				
	,	inserted.STRIKE						as	sSTRIKE					
	,	inserted.EXECUTING_BROKER			as	sEXECUTING_BROKER		
	,	inserted.TRADE_TYPE					as	sTRADE_TYPE				
	,	inserted.EXPIRATION_DATE			as	sEXPIRATION_DATE				
	,	inserted.UNIQUE_TRADE_NUMBER		as	sUNIQUE_TRADE_NUMBER			
	,	inserted.CONTRACT_SIZE				as	sCONTRACT_SIZE			
	,	inserted.DELTA						as	sDELTA					
	,	inserted.fileDt						as	sfileDt					
	,	inserted.lastUpdate					as	slastUpdate				
	into #merge_output;						

-- drop temp table
drop table #openpos