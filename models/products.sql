{{ config(materialized='table') }}
with hourlyusages as (
    select * from (values 
        
        ('2025-08-05 12:00:00', 1, 1, 2998, 4), 
        ('2025-08-05 12:00:00', 2, 2, 2998, 5), 
        ('2025-08-05 12:00:00', 3, 3, 2998, 3), 

        ('2025-08-05 13:00:00', 1, 2, 2999, 6), 
        ('2025-08-05 13:00:00', 2, 3, 2999, 7), 
        ('2025-08-05 13:00:00', 3, 1, 2999, 2),

        ('2025-08-05 14:00:00', 2, 1, 3000, 8),
        ('2025-08-05 14:00:00', 1, 3, 3000, 1), 
        ('2025-08-05 14:00:00', 3, 2, 3000, 5)  
    ) as t(hour, lockerstatuslookupid, productlookupid, siteid, numberoflockers)
),
lockerstatuslookups as (
    select * from (values
        (1, 'outofservice'),
        (2, 'available'),
        (3, 'occupied')
    ) as t(id, status)
),

productlookups as (
    select * from (values
        (1, 'helmet'),
        (2, 'umralla'),
        (3, 'bag')
    ) as t(id, name)
),
kioskrevenue as (
    select * from (values
        ('2025-08-04 07:00:00', 1, 1, 1, 10),
        ('2025-08-03 06:00:00', 1, 1, 1, 5),
        ('2025-08-05 08:00:00', 1, 1, 1, 12)
    ) as t(revenuedate, kiosklid, productlookupid, rentaltypelookupid, numberofrentals)
),

kiosks as(
    select * from (values
        (1, 2998),
        (2, 2999),
        (3, 3000)
    ) as t(id, site_id)
),


rentaltypelookups as (
    select * from (values
        (1, 'hourly'),
        (2, 'test'),
        (3, 'daily')
    ) as t(id, type)
),

sites as(
    select * from (values
        (2998, 'main site'),
        (2999, 'backup site'),
        (3000, 'third site')
    ) as t(id, sitename)
),

hourlyavailable as (
    select
        cast(hu.hour as date) as usage_date,
        pl.name as product_name,
        hu.siteid as site_id,
        SUM(hu.numberoflockers) as available_lockers
    from hourlyusages hu 
    join lockerstatuslookups ls on ls.id = hu.lockerstatuslookupid
    join productlookups pl on pl.id = hu.productlookupid
    where ls.status != 'outofservice'
        and substring(hu.hour, 12, 8) = '12:00:00'
    group by cast(hu.hour as date), pl.name, hu.siteid
)

select
    hu.hour,
    hu.siteid,
    s.sitename,
    pl.name as product_name,
    ls.status as locker_status,
    hu.numberoflockers
from hourlyusages hu
join sites s on hu.siteid = s.id
join productlookups pl on hu.productlookupid = pl.id
join lockerstatuslookups ls on hu.lockerstatuslookupid = ls.id
order by hu.hour, hu.siteid, product_name, locker_status
