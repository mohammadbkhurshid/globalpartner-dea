USE [GlobalPartner]
GO

Select count(*)
from dbo.date_dim
where (date_key is null)    OR date_key = ' '
;

Select date_key, count(*) counts
from dbo.date_dim
group by date_key 
having count(*) > 1
;

Select min(date_key) as min_date,
	max(date_key)    as max_date
from dbo.date_dim
;

select distinct --year, month
	--, day_of_week
	 is_holiday
	 , holiday_name
from dbo.date_dim
;

select *
from dbo.date_dim
;

---===================

Select * --count(*)
from dbo.order_items
where (lineitem_id is null OR lineitem_id = ' ') --(order_id is null)    OR order_id = ' '
--  and IS_LOYALTY = 1 --True
;

Select order_id, lineitem_id--, count(*) counts
from dbo.order_items
--group by order_id, lineitem_id
--having count(*) > 1
--order by 3 desc
;

Select min(creation_time_utc) as min_date,
	max(creation_time_utc)    as max_date
from dbo.order_items	--	2020-04-21 14:56:44.2440000	2024-02-20 21:52:35.3660000
;

select min(item_price) as min_price,
	max(item_price)    as max_price,
	min(item_quantity) as min_qty,
	max(item_quantity) as max_qty
from dbo.order_items
;	-- 0.00		5000.00		0	500

select *
from dbo.order_items
;

select ITEM_CATEGORY, ITEM_NAME, ITEM_QUANTITY, 	ITEM_PRICE
from dbo.order_items
--order by ITEM_PRICE desc
;

---===================
select min(OPTION_PRICE) as min_price,
	max(OPTION_PRICE)    as max_price,
	min(OPTION_QUANTITY) as min_qty,
	max(OPTION_QUANTITY) as max_qty
from dbo.order_item_options
;	--0.00	8.00	1	1