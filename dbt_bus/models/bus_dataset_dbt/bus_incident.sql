select  
    a.DateTime as date_analysis
    , a.RoutesAffected as route_id
    , a.IncidentType
    , a.Description
    , a.IncidentID
    , count(distinct b.VehicleID) as number_of_bus
from 
    {{source('bus_dataset','staging__incident')}} as a
left join {{source('bus_dataset','staging__bus_loc')}} as b
    on a.RoutesAffected = b.RouteID
    and date(a.DateUpdated) = date(b.DateTime)
group by 1,2,3,4,5