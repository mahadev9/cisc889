import fastf1

fastf1.Cache.enable_cache('.cache')

seasons = [2018, 2019, 2020, 2021]
session = 'R'

def fetch_data():
    for year in seasons:
        for round in range(1, 25):
            try:
                session_data = fastf1.get_session(year, round, session)
                session_data.load()
            except Exception as e:
                print(f"Error fetching data for {year} {session} {round}: {e}")
                continue

if __name__ == '__main__':
    fetch_data()
