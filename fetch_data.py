import fastf1
import pandas as pd
import numpy as np


CACHE_FOLDER = '.cache'
SESSION = 'R'

SEASONS = [2018, 2019, 2020, 2021]
# TYPE = 'train'
# TYPE = 'test'
TYPE = 'new'

# ROUNDS = range(1, 25)
ROUNDS = ['british', 'austrian', 'brazilian']

fastf1.Cache.enable_cache(CACHE_FOLDER)

headers = ['race_id', 'circuit_id', 'year', 'round', 'race_length', 'driver_id', 'lap', 'position', 'milliseconds', 'pit_stop_count', 'pit_stop_milliseconds', 'pit_stop', 'rating', 'fcy', 'battle', 'next_lap_time', 'fitted_tire', 'tire_age', 'starting_position', 'gap_from_the_car_in_front', 'gap_from_the_following_car', 'lap_time_of_the_car_in_front', 'lap_time_of_the_following_car', 'drs']


def fetch_preprocess_data():
    for year in SEASONS:
        for round in ROUNDS:
            try:
                dataFrame = pd.DataFrame(columns=headers)
                session_data = fastf1.get_session(year, round, SESSION)
                session_data.load()
                if not session_data.weather_data['Rainfall'].any():
                    race_id = session_data.session_info['Meeting']['Key']
                    circuit_id = session_data.session_info['Meeting']['Circuit']['Key']
                    race_length = session_data.total_laps
                    laps_data = session_data.laps
                    results = session_data.results
                    # tyre_compound = laps_data['Compound'].unique().tolist()
                    # tyre_compound.remove('nan')
                    for driver_id in session_data.drivers:
                        laps_driver_data = laps_data[laps_data['DriverNumber'] == driver_id]
                        pit_stop_count = laps_driver_data['PitInTime'].dropna().count()
                        rating = results[results['DriverNumber'] == driver_id]['Points'].values[-1]
                        previous_lap_time = 0
                        starting_position = laps_driver_data['Position'].iloc[0]
                        pit_in_time = laps_driver_data['PitInTime'].iloc[0].total_seconds()
                        for idx, row in laps_driver_data.iterrows():
                            df_row = []
                            df_row.append(race_id)      # race_id
                            df_row.append(circuit_id)   # circuit_id
                            df_row.append(year)         # year
                            df_row.append(round)        # round
                            df_row.append(race_length)  # race_length
                            df_row.append(int(driver_id))   # driver_id
                            df_row.append(row['LapNumber']) # lap
                            df_row.append(row['Position'])  # position
                            df_row.append(previous_lap_time)    # milliseconds
                            previous_lap_time = row['LapTime'].total_seconds() * 1000
                            df_row.append(pit_stop_count)   # pit_stop_count
                            if pd.isnull(row['PitOutTime'].total_seconds()):
                                df_row.append(0)
                            else:
                                df_row.append((row['PitOutTime'].total_seconds() - pit_in_time)*1000)
                            pit_in_time = row['PitInTime'].total_seconds()
                            df_row.append(0 if pd.isnull(row['PitOutTime']) else 1)  # pit_stop
                            df_row.append(rating)
                            fcy = 0
                            if row['TrackStatus'] == 1:
                                fcy = 1
                            df_row.append(fcy)  # fcy
                            current_driver_time = row['Time'].total_seconds() * 1000
                            try:
                                following_driver_time = laps_data[(laps_data['Position'] == row['Position'] + 1) & (laps_data['LapNumber'] == row['LapNumber'])]['Time'].iloc[0].total_seconds() * 1000
                            except:
                                following_driver_time = 0
                            df_row.append(1 if (abs(current_driver_time - following_driver_time) <= 2000.0) else 0) # battle
                            df_row.append(row['LapTime'].total_seconds()*1000)  # next_lap_time
                            df_row.append(row['Compound'])  # fitted_tire
                            df_row.append(row['TyreLife'])  # tire_age
                            df_row.append(starting_position)    # starting_position
                            if row['Position'] == 1:
                                df_row.append(0)
                            else:
                                try:
                                    gap_from_the_car_in_front = laps_data[(laps_data['Position'] == row['Position'] - 1) & (laps_data['LapNumber'] == row['LapNumber'])]['Time'].iloc[0].total_seconds() * 1000
                                    df_row.append(abs(gap_from_the_car_in_front - current_driver_time))  # gap_from_the_car_in_front
                                except:
                                    df_row.append(0)
                            if row['Position'] == 20:
                                df_row.append(0)
                            else:
                                try:
                                    gap_from_the_following_car = laps_data[(laps_data['Position'] == row['Position'] + 1) & (laps_data['LapNumber'] == row['LapNumber'])]['Time'].iloc[0].total_seconds() * 1000
                                    df_row.append(abs(gap_from_the_following_car - current_driver_time)) # gap_from_the_following_car
                                except:
                                    df_row.append(0)
                            if row['Position'] == 1:
                                df_row.append(0)
                            else:
                                try:
                                    lap_time_of_the_car_in_front = laps_data[(laps_data['Position'] == row['Position'] - 1) & (laps_data['LapNumber'] == row['LapNumber'])]['LapTime'].iloc[0].total_seconds() * 1000
                                    df_row.append(lap_time_of_the_car_in_front)     # lap_time_of_the_car_in_front
                                except:
                                    df_row.append(0)
                            if row['Position'] == 20:
                                df_row.append(0)
                            else:
                                try:
                                    lap_time_of_the_following_car = laps_data[(laps_data['Position'] == row['Position'] + 1) & (laps_data['LapNumber'] == row['LapNumber'])]['LapTime'].iloc[0].total_seconds() * 1000
                                    df_row.append(lap_time_of_the_following_car)    # lap_time_of_the_following_car
                                except:
                                    df_row.append(0)
                            if row['Position'] == 1:
                                df_row.append(0)
                            else:
                                try:
                                    gap = abs(laps_data[(laps_data['Position'] == row['Position'] - 1) & (laps_data['LapNumber'] == row['LapNumber']) & (row['TrackStatus'] == 1)]['Time'].iloc[0].total_seconds() * 1000 - current_driver_time)
                                except:
                                    gap = 10000
                                if gap <= 1000:
                                    df_row.append(1)        # drs
                                else:
                                    df_row.append(0)
                            dataFrame.loc[len(dataFrame)] = df_row
                    dataFrame.to_csv(f'data/{TYPE}/{year}_{round}_data.csv', index=False)
            except Exception as e:
                print(f"Error fetching data for {year} {SESSION} {round}: {e}")
                continue


if __name__ == '__main__':
    fetch_preprocess_data()
