import json
from dataclasses import dataclass


@dataclass
class Ride:
    PULocationID: int
    DOLocationID: int
    passenger_count: int
    trip_distance: float
    total_amount: float
    tip_amount: float
    lpep_pickup_datetime: str
    lpep_dropoff_datetime: str
   

def ride_from_row(row):
    return Ride(
        PULocationID=int(row['PULocationID']),
        DOLocationID=int(row['DOLocationID']),
        passenger_count=int(row['passenger_count']),
        trip_distance=float(row['trip_distance']),
        total_amount=float(row['total_amount']),
        tip_amount=float(row['tip_amount']),
        lpep_pickup_datetime=row['lpep_pickup_datetime'].strftime("%Y-%m-%d %H:%M:%S"),
        lpep_dropoff_datetime=row['lpep_dropoff_datetime'].strftime("%Y-%m-%d %H:%M:%S"),

    )


def ride_deserializer(data):
    json_str = data.decode('utf-8')
    ride_dict = json.loads(json_str)
    return Ride(**ride_dict)