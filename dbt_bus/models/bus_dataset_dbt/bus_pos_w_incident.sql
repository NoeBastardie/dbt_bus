-- this table displays the position (coordinates) of all the buses 
-- currently tracked with information about incident on their route

with full_info as (
select  distinct 
    b.VehicleID 
    , b.TripID
    , b.BlockNumber
    , concat(b.Lat,' ',b.Lon) as bus_coordinate
    , b.TripStartTime
    , b.RouteID
    , a.IncidentType
    , a.Description
    , a.IncidentID
    , case 
        when a.IncidentID is null then 'False'
        else 'True' end as is_bus_impacted
    , b.DateTime
    , ROW_NUMBER() OVER (
    PARTITION BY VehicleID 
    ORDER BY b.DateTime desc ) as row_n     
from 
    {{source('bus_dataset','staging__bus_loc')}} as b 
left join {{source('bus_dataset','staging__incident')}} as a
    on a.RoutesAffected = b.RouteID
    and date(a.DateUpdated) = date(b.DateTime)
),
last_pos as (
select 
    *
from full_info
where row_n = 1
),
last_update as (
select
max(a.DateTime) as last_date
from 
    {{source('bus_dataset','staging__bus_loc')}} as a
)
select
    *
from last_pos as a
    join last_update as b 
        on a.Datetime <= DATETIME_SUB(b.last_date, INTERVAL 10 MINUTE) 
        and a.Datetime <= DATETIME_ADD(b.last_date, INTERVAL 10 MINUTE)