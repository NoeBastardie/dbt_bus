select  distinct 
    b.VehicleID 
    , b.TripID
    , b.BlockNumber
    , concat(b.Lat,' ',b.Lon) as bus_coordinate
    , a.DateTime as date_analysis
    , b.TripStartTime
    , a.RoutesAffected as route_id
    , a.IncidentType
    , a.Description
    , a.IncidentID
from 
    {{source('bus_dataset','staging__bus_loc')}} as b 
left join {{source('bus_dataset','staging__incident')}} as a
    on a.RoutesAffected = b.RouteID
    and date(a.DateUpdated) = date(b.DateTime)