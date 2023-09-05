

-- start delete here when testing is finished
set nocount on
set ansi_warnings off

create table #msg(disregard varchar(100), this varchar(100), email varchar(100))

insert into #msg(disregard, this, email)
values ('testing testing 123, sql query doesnt work on test server so this is the output',
		'moneyline has finished processing, and has called margin_deficit_control automatically.',
		'I have not figured out how to run flows in parallel')

select * from #msg

drop table #msg

-- end delete here




-- same as old sql procedure, uncomment when want to use
/****** 

declare @dt as date;
set @dt = dbo.fnPreviousBusinessDay(getdate());

-- removed cattle data 2021-07-14 (MID)

select
pl.dt
, pl.AcctGroup
, Margin
, pl.PnL
, Limit
, Status = case 
			when Margin < Limit then 'Review' 
			else 'Good' 
			end + 
		   case 
			when Margin >= 0 then ' - No Deficit' 
			else '' 
			end 

from  (
	 select
	 'GPG' AcctGroup, dt, sum(totalDailyPnL) PnL, -3500000 Limit

	  from pnlMasterFinancial

	  where
	 dt = @dt
	 and acct like '%206%'

	  group by dt
)  pl

left  join (
	 select
	 dt			   = cast(lastUpdate as date)
   , account_number
   , Margin		   = cast(floor(( case
								 when margin_excess_deficit < 100000
									  and margin_excess_deficit > 0 then 0
								 else margin_excess_deficit
								 end
						   ) / 1000
					 ) * 1000 as money)

	  from <!! DATABASE NAME !!>.[dbo].[broker_moneyLineRJO]

	  where
	 account_number in ( 'NATGA', 'GRAIN', 'GPCM1', 'BEEF1', 'CRUSH', 'ETHAN' )
	 and account_type_code = 'F1'
	 and cast(lastUpdate as date) = @dt
	 and dbo.fnIsBusinessDay(cast(lastUpdate as date)) = 1

	  group by
	 account_number, cast(lastUpdate as date), margin_excess_deficit

	 union all

	 select
	 Business_Date
   , account_group = case
						 when Account_Group = 'GPREGPG ' then 'GPG'
						 when Account_Group = 'gpregpcm' then 'GPTG'
						 else Account_Group
						 end
   , Excess		   = cast(floor(( sum(Total_Equity) + ( sum(Group_Initial_Margin_Amount) / count(Group_Initial_Margin_Amount)))
						   / 1000
					 ) * 1000 as money)

	  from <!! DATABASE NAME !!>.[dbo].[broker_moneyLineMAQ]

	  where
	 Business_Date = @dt
	 and account_group in ( /*'gpregpcm',*/ 'gpregpg', 'gpr1000', 'gprnatga', 'gpr3000' )

	  group by
	 Business_Date, account_group
)  margins
	 on rtrim(ltrim(account_number)) = ltrim(rtrim(AcctGroup))
		and margins.dt = pl.dt

union all

select
dt		  = business_date
, AcctGroup = 'GPTG'
, Margin  = cast(case when LoanBalance < -40000000 then LoanBalance + 40000000 else 0 end + LoanExcessDeficit as money) /*When loan is over limit, add difference to excess/deficit */
, pnl	  = null
, limit	  = null
, Status  = 'Good'

from  (
	 select
	 business_date			  = business_date
   , LoanAcctOpenBalance	  = sum(LoanAcctOpenBalance)
   , CashBalance			  = sum(CashBalance)
   , VariationMargin		  = sum(VariationMargin)
   , ExcessVariationMargin	  = sum(ExcessVariationMargin)
   , InitialMargin			  = sum(InitialMargin)
   , MarginExcessDeficit	  = sum(MarginExcessDeficit)
   , LoanBalanceDrawn		  = sum(LoanBalanceDrawn)
   , ProjectedLoanAcctBalance = sum(MarginExcessDeficit)
								- ( sum(CashBalance) + sum(LoanAcctOpenBalance) + sum(ExcessVariationMargin))
								- ( case when sum(VariationMargin) > 0 then sum(VariationMargin)else 0 end )
   , LoanExcessDeficit		  = sum(CashBalance) + sum(LoanAcctOpenBalance) + sum(ExcessVariationMargin)
								+ case when sum(VariationMargin) > 0 then sum(VariationMargin)else 0 end
   , LoanBalance			  = sum(LoanAcctOpenBalance) + sum(MarginExcessDeficit)
								- ( sum(CashBalance) + sum(LoanAcctOpenBalance) + sum(ExcessVariationMargin)
									+ case when sum(VariationMargin) > 0 then sum(VariationMargin)else 0 end
								  )
   , VariationMarginHeld	  = case
									when sum(VariationMargin) < -11250000 then -11250000
									else sum(VariationMargin)
									end
   , isToday				  = isToday
   , Interest				  = sum(Interest)
   , CumulativeInterest		  = sum(CumulativeInterest)
	  
	  from (
		  select
		  Account_Group
		, business_date			   = cast(Business_Date as datetime)
		, LoanAcctOpenBalance	   = 0 /*(sum(beginning_balance) + sum(cash)) */
		, CashBalance			   = sum(Ending_Balance)
		, VariationMargin		   = sum(Open_Trade_Equity_Futures)
		, ExcessVariationMargin	   = case
										 when sum(Open_Trade_Equity_Futures) < -11250000 then
											 sum(Open_Trade_Equity_Futures) - -11250000
										 else 0
										 end
		, InitialMargin			   = avg(Group_Initial_Margin_Amount)
		, MarginExcessDeficit	   = ( sum(Ending_Balance) + sum(Open_Trade_Equity_Futures)
									   + avg(Group_Initial_Margin_Amount)
									 )
		, LoanBalanceDrawn		   = ( case
										   when sum(Open_Trade_Equity_Futures) > 0 then 0
										   else sum(Open_Trade_Equity_Futures)
										   end
									 ) + avg(Group_Initial_Margin_Amount)
		, ProjectedLoanAcctBalance = 0 /* ( sum(Open_Trade_Equity_Futures)   +   avg(Group_Initial_Margin_Amount)   +   sum(beginning_balance)   +   sum(cash) ) */
		, LoanExcessDeficit		   = sum(Ending_Balance) - ( sum(Beginning_Balance) + sum(Cash))
									 + ( case
											 when sum(Open_Trade_Equity_Futures) < -11250000 then
												 sum(Open_Trade_Equity_Futures) - -11250000
											 else 0
											 end
									   )
		, LoanBalance			   = abs(case
											 when sum(Open_Trade_Equity_Futures) > 0 then 0
											 else sum(Open_Trade_Equity_Futures)
											 end + avg(Group_Initial_Margin_Amount)
									 )
		, isToday				   = case when Business_Date = @dt then 'TRUE' else 'FALSE' end
		, Interest				   = 0
		, CumulativeInterest	   = 0
		 
		 from <!! DATABASE NAME !!>.[dbo].[broker_moneyLineMAQ]
		
		where
		  business_date = @dt
		  and Account_Group in ( 'gpregpcm' )
		
		group by
		  business_date, Account_Group
		
		union all
	
		select
		  Account_Group
		, business_date			   = cast(Business_Date as datetime)
		, LoanAcctOpenBalance	   = ( sum(Beginning_Balance) + sum(Cash))
		, CashBalance			   = 0 /*sum(ending_balance)*/
		, VariationMargin		   = sum(Open_Trade_Equity_Futures)
		, ExcessVariationMargin	   = case
										 when sum(Open_Trade_Equity_Futures) < -11250000 then
											 sum(Open_Trade_Equity_Futures) - -11250000
										 else 0
										 end
		, InitialMargin			   = avg(Group_Initial_Margin_Amount)
		, MarginExcessDeficit	   = 0 /*( sum(ending_balance)   +   sum(Open_Trade_Equity_Futures)   +   avg(Group_Initial_Margin_Amount) ) */
		, LoanBalanceDrawn		   = ( case
										   when sum(Open_Trade_Equity_Futures) > 0 then 0
										   else sum(Open_Trade_Equity_Futures)
										   end
									 ) + avg(Group_Initial_Margin_Amount)
		, ProjectedLoanAcctBalance = 0 /* ( sum(Open_Trade_Equity_Futures)   +   avg(Group_Initial_Margin_Amount)   +   sum(beginning_balance)   +   sum(cash) ) */
		, LoanExcessDeficit		   = sum(Ending_Balance) - ( sum(Beginning_Balance) + sum(Cash))
									 + ( case
											 when sum(Open_Trade_Equity_Futures) < -11250000 then
												 sum(Open_Trade_Equity_Futures) - -11250000
											 else 0
											 end
									   )
		, LoanBalance			   = abs(case
											 when sum(Open_Trade_Equity_Futures) > 0 then 0
											 else sum(Open_Trade_Equity_Futures)
											 end + avg(Group_Initial_Margin_Amount)
									 )
		, isToday				   = case when Business_Date = @dt then 'TRUE' else 'FALSE' end
		, Interest				   = sum(INTEREST)
		, CumulativeInterest	   = sum(INTEREST) over ( partition by month(dbo.fnFollowingBusinessDay(Business_Date))
														  order by Business_Date
														  rows between unbounded preceding and current row
												   )
		   
		from <!! DATABASE NAME !!>.[dbo].[broker_moneyLineMAQ]
		   
		where
		  business_date = @dt
		  and Account_Group in ( 'GPLNCEN1', 'GPLNCEN' )
		   
		group by
		  business_date, Account_Group, INTEREST
	  ) X

	  group by
	 business_date, isToday
)  z;

******/