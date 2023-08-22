from locust import HttpUser, task
from random import randrange, choice
from datetime import datetime, timedelta


class HelloWorldUser(HttpUser):
    @task(10)
    def LocationsV3(self):
        id = randrange(30000)
        self.client.get(f"/v3/locations/{id}", name="v3/locations/:id")

    @task(2)
    def ProvidersV3(self):
        limit = randrange(1000)
        self.client.get(f"/v3/providers?{limit}", name="v3/providers")

    @task(2)
    def ParametersV3(self):
        self.client.get("/v3/parameters?", name="v3/parameters")

    @task(1)
    def load_tiles(self):
        z = choice([1, 2, 3])
        x = choice([0, 1, 2])
        y = choice([0, 1, 2])
        parameter_ids = [
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            # 11,
            # 19,
            # 27,
            # 35,
            # 95,
            # 98,
            # 100,
            # 125,
            # 126,
            # 128,
            # 129,
            # 130,
            # 132,
            # 133,
            # 134,
            # 135,
            # 150,
            # 676,
            # 19840,
            # 19843,
            # 19844,
            # 19860,
        ]
        parameters_id = choice(parameter_ids)
        self.client.get(
            f"/v3/locations/tiles/{z}/{x}/{y}.pbf?parameters_id={parameters_id}&active=true",
            name="/v3/locations/tiles",
        )

    @task(20)
    def LocationsV3PeriodAndParameter(self):
        id = randrange(3000)
        limit = randrange(1000)
        base_date = datetime(2022, 6, 1)
        random_days = randrange(
            30
        )  # Changed from 300 to 30 to avoid exceeding the maximum of 30 days in the month of June
        random_hours = randrange(24)
        date_from = base_date + timedelta(days=random_days, hours=random_hours)

        # Ensure there are remaining days in the month for the date_to
        remaining_days_in_month = 30 - random_days
        if remaining_days_in_month > 1:
            random_days_to = randrange(1, remaining_days_in_month)
        else:
            random_days_to = 1
        random_hours_to = randrange(24)
        date_to = date_from + timedelta(days=random_days_to, hours=random_hours_to)
        date_from_str = date_from.isoformat(timespec="seconds")
        date_to_str = date_to.isoformat(timespec="seconds")
        parameter_ids = [
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            # 11,
            # 19,
            # 27,
            # 35,
            # 95,
            # 98,
            # 100,
            # 125,
            # 126,
            # 128,
            # 129,
            # 130,
            # 132,
            # 133,
            # 134,
            # 135,
            # 150,
            # 676,
            # 19840,
            # 19843,
            # 19844,
            # 19860,
        ]
        parameter_id = choice(parameter_ids)
        self.client.get(
            f"/v3/locations/{id}/measurements?period_name=hour&limit={limit}&parameters_id={parameter_id}&date_from={date_from_str}&date_to={date_to_str}",
            name="v3/locations/:id/measurements/",
        )
