import asyncio
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
import time
import uvloop
import threading

THREADS = 1
# IS_UV = 1;

# Since I can't use a RDBMS for this I'm using Pandas, even though using a
# RDBMS is more scalable, more efficient and easier to use.
def vehicle_task(vehicle_data):
    # This will give us 'Active Duration' by month filtered by ignition_on
    seconds_active = vehicle_data.loc[vehicle_data['ignition_on'] == 1] \
        .groupby(pd.Grouper(key='timestamp', freq='1M'))['duration_s'].sum() \
        .to_frame()

    # This will give us all other aggregations needed
    months_aggregations = vehicle_data \
        .groupby(pd.Grouper(key='timestamp', freq='1M')) \
        .agg({
              'car_number': ['mean'],
              'duration_s':['sum'],
              'speed_km_h': ['mean'],
              'distance_m': ['sum']
        })

    intermediate = months_aggregations.merge(seconds_active, how='inner',
                                             left_on=['timestamp'],
                                             right_on=['timestamp'])

    intermediate.columns.values[0] = 'car_number'
    intermediate.columns.values[1] = 'total_duration_s'
    intermediate.columns.values[2] = 'average_speed_km_h'
    intermediate.columns.values[3] = 'total_distance_m'
    intermediate.columns.values[4] = 'total_operating_time_s'

    intermediate['utilization'] = \
        intermediate['total_operating_time_s'] / intermediate['total_duration_s']

    return intermediate.drop(['total_duration_s'], axis=1)

async def vehicle_tasks(vehicle_data_raw):
    uvloop.install()
    loop = asyncio.get_event_loop()
    vehicle_ids = vehicle_data_raw.car_number.unique()
    executor = ProcessPoolExecutor(max_workers=THREADS);

    tasks = [
        loop.run_in_executor(executor, vehicle_task, vehicle_data_raw.loc[vehicle_data_raw['car_number'] == vehicle_ids[x]])
        for x in range(len(vehicle_ids))
    ]

    results = await asyncio.gather(*tasks)

    return pd.concat(results)


def main():
    uvloop
    loop = asyncio.get_event_loop()
    executor = ProcessPoolExecutor(max_workers=THREADS);
    loop.set_default_executor(executor)

    vehicle_data = pd.read_csv("vehicle_information.csv")
    vehicle_data['timestamp'] = pd.to_datetime(vehicle_data['timestamp'])

    start_time = time.time()

    data = loop.run_until_complete(vehicle_tasks(vehicle_data))
    print("--- %s seconds ---" % (time.time() - start_time))

    data.to_csv('aggregations.csv', index=False)


if __name__ == "__main__":
    main()
