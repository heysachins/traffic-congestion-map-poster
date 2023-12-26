

select distinct service_id from bus;

select count(*) from bus where vehicle_id='1082';
ALTER TABLE busdata
RENAME TO bus;

select vehicle_id,count(vehicle_id) from bus group by vehicle_id order by count;

select * from bus

select service_id, count(distinct destination) from bus group by service_id;


Select journey_id,service_id,count(distinct nextstop_id) AS bus_stops from bus group by journey_id,service_id 

SELECT
    service_id,
    COUNT(DISTINCT nextstop_id) AS bus_stops
FROM
    bus
GROUP BY
    service_id
ORDER BY bus_stops DESC
LIMIT 1;

select * from bus where journey_id='2087'

SELECT  service_id,journey_id, COUNT( nextstop_id) AS bus_stops FROM bus GROUP BY  service_id,journey_id ORDER BY bus_stops DESC LIMIT 1;
db.bus.aggregate([
  {
    $group: {
      _id: "$journey_id",
      avg_speed: {
        $avg: "$speed",
      },
    },
  },
  {
    $sort: {
      avg_speed: -1,
    },
  },
  {
    $limit: 1,
  },
  {
    $project: {
      _id: 0,
      avg_speed: 1,
      journey_id: "$_id",
    },
  },
])
select avg(speed),journey_id from bus group by journey_id order by avg desc limit 1
select * from bus where vehicle_id='1091'
select vehicle_id,count(distinct journey_id) from bus group by vehicle_id order by count 
select vehicle_id, count(*) as count from bus group by vehicle_id having count(*) < '150' order by count
select nextstop_id,count(distinct service_id) from bus group by nextstop_id order by count desc

db.bus.aggregate([
  { $group: {
      _id: "$nextstop_id",
      service_count: { $addToSet: "$service_id" },
      count: { $sum: 1 }
    }
  },
  { $sort: { count: -1 } }
])



db.bus.aggregate([
   {
      $group : {
         _id : "$vehicle_id",
         count: { $sum: 1 }
      }
   },
   {
      $match : { count : { $lt : 150 } }
   },
   {
      $project: {
         vehicle_id: "$_id",
         _id: 0,
         count: 1
      }
   }
])