from locust import HttpUser, task
# import time
from random import randrange


class HelloWorldUser(HttpUser):

    @task(1)
    def LocationsV2(self):
        id = randrange(30000)
        self.client.get(f"/v2/locations/{id}", name='v2/locations/:id')

    @task(0)
    def LocationsRadiusSourceV2(self):
        lon = randrange(35000, 45000)/1000
        lat = randrange(-100000, -80000)/1000
        self.client.get(
            f"/v2/locations?coordinates={lon},{lat}&sourceName=us-epa-airnow&radius=10000",
            name='v2/locations?radius'
        )

    @task(40)
    def LatestV2(self):
        lon = randrange(40000, 60000)/1000
        lat = randrange(50000, 60000)/1000
        rad = 80000
        self.client.get(
            f"/v2/latest?limit=10",
            name='v2/latest/empty'
        )


    @task(40)
    def LatestRadiusV2b(self):
        lon = randrange(40000, 60000)/1000
        lat = randrange(50000, 60000)/1000
        rad = 8000
        self.client.get(
            f"/v2/latest?coordinates={lon},{lat}&radius={rad}",
            name='v2/latest/8K'
        )

    @task(2)
    def LatestLocationV1(self):
        id = randrange(30000)
        self.client.get(f"/v1/latest?location={id}", name='v1/latest?location')

    @task(0)
    def LatestCoordinatesV1(self):
        coords = randrange(50000, 60000)/1000
        self.client.get(
            f"/v1/latest?coordinates={coords}",
            name='v1/latest?coordinates'
        )

    @task(2)
    def LatestRadiusV1(self):
        lon = randrange(40000, 60000)/1000
        lat = randrange(50000, 60000)/1000
        rad = 8000
        self.client.get(
            f"/v1/latest?coordinates={lon},{lat}&radius={rad}",
            name='v1/latest?radius'
        )

    @task(1)
    def LocationsV1(self):
        self.client.get("/v1/locations", name='v1/locations')

    @task(0)
    def GetMeasurementsV3(self):
        id = randrange(30000)
        self.client.get(
            f"/v3/locations/{id}/measurements",
            name='v3/locations/:id/meausurements'
        )
