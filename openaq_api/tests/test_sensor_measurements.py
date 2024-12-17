from fastapi.testclient import TestClient
import json
import time
import pytest
from openaq_api.main import app


@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c


sensors_id = 1

class TestMeasurements:
    def test_default_good(self, client):
        response = client.get(f"/v3/sensors/{sensors_id}/measurements")
        assert response.status_code == 200
        data = json.loads(response.content).get('results', [])
        assert len(data) > 0, "response did not have at least one record"

    def test_date_filter_good(self, client):
        ## 7 is the only hourly sensor
        response = client.get(f"/v3/sensors/7/measurements?datetime_from=2023-03-05&datetime_to=2023-03-06")
        assert response.status_code == 200
        data = json.loads(response.content).get('results', [])
        row = data[0]
        assert len(data) == 24
        assert row['coverage']['expectedCount'] == 1
        assert row['coverage']['observedCount'] == 1
        assert row['coverage']['datetimeFrom']['local'] == '2023-03-05T00:00:00-08:00'
        assert row['coverage']['datetimeTo']['local'] == '2023-03-05T01:00:00-08:00'
        assert row['period']['datetimeFrom']['local'] == '2023-03-05T00:00:00-08:00'
        assert row['period']['datetimeTo']['local'] == '2023-03-05T01:00:00-08:00'


    def test_aggregated_hourly_good(self, client):
        response = client.get(f"/v3/sensors/{sensors_id}/measurements/hourly")
        assert response.status_code == 200
        data = json.loads(response.content).get('results', [])
        assert len(data) > 0

    def test_date_filter_aggregated_hourly_good(self, client):
        response = client.get(f"/v3/sensors/{sensors_id}/measurements/hourly?datetime_from=2023-03-05&datetime_to=2023-03-06")
        assert response.status_code == 200
        data = json.loads(response.content).get('results', [])
        assert len(data) == 24

        row = data[0]
        period = row['period']['label']

        assert row['coverage']['datetimeFrom']['local'] == '2023-03-05T00:00:00-10:00'
        assert row['coverage']['datetimeTo']['local'] == '2023-03-05T01:00:00-10:00'
        assert row['period']['datetimeFrom']['local'] == '2023-03-05T00:00:00-10:00'
        assert row['period']['datetimeTo']['local'] == '2023-03-05T01:00:00-10:00'

        assert row['coverage']['expectedCount'] == 2
        assert row['coverage']['observedCount'] == 2
        assert row['coverage']['percentComplete'] == 100
        assert row['coverage']['percentComplete'] == row['coverage']['percentCoverage']


    def test_aggregated_daily_good(self, client):
        response = client.get(f"/v3/sensors/{sensors_id}/measurements/daily")
        assert response.status_code == 200
        data = json.loads(response.content).get('results', [])
        assert len(data) > 0


class TestHours:
    def test_default_good(self, client):
        response = client.get(f"/v3/sensors/{sensors_id}/hours")
        assert response.status_code == 200
        data = json.loads(response.content).get('results', [])
        assert len(data) > 0

    def test_date_filter_good(self, client):
        ## 7 is the only hourly sensor
        response = client.get(f"/v3/sensors/7/hours?datetime_from=2023-03-05T00:00:00&datetime_to=2023-03-06T00:00:00")
        assert response.status_code == 200
        data = json.loads(response.content).get('results', [])
        row = data[0]
        assert len(data) == 24
        assert row['coverage']['expectedCount'] == 1
        assert row['coverage']['observedCount'] == 1
        assert row['coverage']['datetimeFrom']['local'] == '2023-03-05T00:00:00-08:00'
        assert row['coverage']['datetimeTo']['local'] == '2023-03-05T01:00:00-08:00'
        assert row['period']['datetimeFrom']['local'] == '2023-03-05T00:00:00-08:00'
        assert row['period']['datetimeTo']['local'] == '2023-03-05T01:00:00-08:00'

    def test_aggregated_daily_good(self, client):
        response = client.get(f"/v3/sensors/{sensors_id}/hours/daily")
        assert response.status_code == 200
        data = json.loads(response.content).get('results', [])
        assert len(data) > 0

    def test_aggregated_daily_reversed_dates_bad(self, client):
        response = client.get(f"/v3/sensors/{sensors_id}/hours/daily?datetime_from=2023-03-06&datetime_to=2023-03-05")
        assert response.status_code == 422

    def test_aggregated_daily_dates_good(self, client):
        response = client.get(f"/v3/sensors/{sensors_id}/hours/daily?datetime_from=2023-03-05&datetime_to=2023-03-06")
        assert response.status_code == 200
        data = json.loads(response.content).get('results', [])
        assert len(data) > 0

    def test_aggregated_monthly_good(self, client):
        response = client.get(f"/v3/sensors/{sensors_id}/hours/monthly")
        assert response.status_code == 200
        data = json.loads(response.content).get('results', [])
        assert len(data) > 0

    def test_aggregated_yearly_good(self, client):
        response = client.get(f"/v3/sensors/{sensors_id}/hours/yearly")
        assert response.status_code == 200
        data = json.loads(response.content).get('results', [])
        assert len(data) > 0

    def test_aggregated_daily_good_with_dates(self, client):
        response = client.get(f"/v3/sensors/{sensors_id}/hours/daily?datetime_from=2023-03-05&datetime_to=2023-03-06")
        assert response.status_code == 200
        data = json.loads(response.content).get('results', [])
        assert len(data) == 1
        row = data[0]

        assert row['coverage']['datetimeFrom']['local'] == '2023-03-05T00:00:00-10:00'
        assert row['coverage']['datetimeTo']['local'] == '2023-03-06T00:00:00-10:00'
        assert row['period']['datetimeFrom']['local'] == '2023-03-05T00:00:00-10:00'
        assert row['period']['datetimeTo']['local'] == '2023-03-06T00:00:00-10:00'

        assert row['coverage']['expectedCount'] == 24
        assert row['coverage']['observedCount'] == 24
        assert row['coverage']['percentComplete'] == 100
        assert row['coverage']['percentComplete'] == row['coverage']['percentCoverage']


    def test_aggregated_yearly_good_with_dates(self, client):
        response = client.get(f"/v3/sensors/{sensors_id}/hours/yearly?datetime_from=2022-01-01&datetime_to=2023-01-01")
        assert response.status_code == 200
        data = json.loads(response.content).get('results', [])
        assert len(data) == 1
        row = data[0]
        assert row.get('coverage', {}).get('expectedCount') == (365 * 24)
        assert row.get('coverage', {}).get('observedCount') == 365 * 24

    def test_aggregated_hod_good(self, client):
        response = client.get(f"/v3/sensors/{sensors_id}/hours/hourofday")
        assert response.status_code == 200
        data = json.loads(response.content).get('results', [])
        assert len(data) == 24

    def test_aggregated_hod_dates_good(self, client):
        response = client.get(f"/v3/sensors/{sensors_id}/hours/hourofday?datetime_from=2023-03-01&datetime_to=2023-04-01")
        assert response.status_code == 200
        data = json.loads(response.content).get('results', [])
        assert len(data) == 24

    def test_aggregated_hod_timestamps_good(self, client):
        response = client.get(f"/v3/sensors/{sensors_id}/hours/hourofday?datetime_from=2023-03-01T00:00:01&datetime_to=2023-04-01T00:00:01")
        assert response.status_code == 200
        data = json.loads(response.content).get('results', [])
        assert len(data) == 24

    def test_aggregated_hod_timestamptzs_good(self, client):
        response = client.get(f"/v3/sensors/{sensors_id}/hours/hourofday?datetime_from=2023-03-01T00:00:00Z&datetime_to=2023-04-01T00:00:00Z")
        assert response.status_code == 200
        data = json.loads(response.content).get('results', [])
        assert len(data) == 24


    def test_aggregated_dow_good(self, client):
        response = client.get(f"/v3/sensors/{sensors_id}/hours/dayofweek")
        assert response.status_code == 200
        data = json.loads(response.content).get('results', [])
        assert len(data) == 7

    def test_aggregated_moy_good(self, client):
        response = client.get(f"/v3/sensors/{sensors_id}/hours/monthofyear?datetime_from=2022-01-01T00:00:00Z&datetime_to=2023-01-01T00:00:00Z")
        assert response.status_code == 200
        data = json.loads(response.content).get('results', [])
        assert len(data) == 12

        row = data[0]
        # hours are time ending
        assert row['coverage']['datetimeFrom']['local'] == '2022-01-01T00:00:00-10:00'
        assert row['coverage']['datetimeTo']['local'] == '2022-02-01T00:00:00-10:00'
        assert row['period']['datetimeFrom']['local'] == '2022-01-01T00:00:00-10:00'
        assert row['period']['datetimeTo']['local'] == '2022-02-01T00:00:00-10:00'



class TestDays:

    def test_default_good(self, client):
        response = client.get(f"/v3/sensors/{sensors_id}/days")
        assert response.status_code == 200

    def test_good_with_dates(self, client):
        response = client.get(f"/v3/sensors/{sensors_id}/days?date_from=2023-03-05&limit=1")
        assert response.status_code == 200
        data = json.loads(response.content).get('results', [])
        assert len(data) == 1
        row = data[0]
        assert row['coverage']['expectedCount'] == 24
        assert row['coverage']['observedCount'] == 24
        assert row['coverage']['percentComplete'] == 100
        assert row['coverage']['percentComplete'] == row['coverage']['percentCoverage']

    def test_aggregated_monthly_good(self, client):
        response = client.get(f"/v3/sensors/{sensors_id}/days/monthly?date_from=2022-01-01&date_to=2022-12-31")
        assert response.status_code == 200
        data = json.loads(response.content).get('results', [])
        assert len(data) == 12
        row = data[0]
        assert row['coverage']['datetimeFrom']['local'] == '2022-01-01T00:00:00-10:00'
        assert row['coverage']['datetimeTo']['local'] == '2022-02-01T00:00:00-10:00'
        assert row['period']['datetimeFrom']['local'] == '2022-01-01T00:00:00-10:00'
        assert row['period']['datetimeTo']['local'] == '2022-02-01T00:00:00-10:00'

        assert row['coverage']['expectedCount'] == 31
        assert row['coverage']['observedCount'] == 31
        assert row['coverage']['percentComplete'] == 100
        assert row['coverage']['percentComplete'] == row['coverage']['percentCoverage']

    def test_aggregated_yearly_good(self, client):
        response = client.get(f"/v3/sensors/{sensors_id}/days/yearly?date_from=2022-01-01&date_to=2022-12-31")
        assert response.status_code == 200
        data = json.loads(response.content).get('results', [])
        for d in data:
            print(d.get('period'))
        # dates should mean only one year is returned
        assert len(data) == 1
        row = data[0]
        # we should expect 365 days
        assert row['coverage']['expectedCount'] == 365
        assert row['coverage']['observedCount'] == 365
        assert row['coverage']['percentComplete'] == 100
        assert row['coverage']['percentComplete'] == row['coverage']['percentCoverage']

        assert row['coverage']['datetimeFrom']['local'] == '2022-01-01T00:00:00-10:00'
        assert row['coverage']['datetimeTo']['local'] == '2023-01-01T00:00:00-10:00'
        assert row['period']['datetimeFrom']['local'] == '2022-01-01T00:00:00-10:00'
        assert row['period']['datetimeTo']['local'] == '2023-01-01T00:00:00-10:00'

    def test_aggregated_dow_good(self, client):
        response = client.get(f"/v3/sensors/{sensors_id}/days/dayofweek?date_from=2022-01-01&date_to=2022-12-31")
        assert response.status_code == 200
        data = json.loads(response.content).get('results', [])
        assert len(data) == 7
        row = data[0]
        period = row['period']['label']
        # just in case we are out of order
        expected = 53 if period == '6' else 52
        assert row['coverage']['expectedCount'] == expected
        assert row['coverage']['observedCount'] == expected
        assert row['coverage']['percentComplete'] == 100
        assert row['coverage']['percentComplete'] == row['coverage']['percentCoverage']


    def test_aggregated_moy_good(self, client):
        response = client.get(f"/v3/sensors/{sensors_id}/days/monthofyear?date_from=2022-01-01&date_to=2022-12-31")
        assert response.status_code == 200
        data = json.loads(response.content).get('results', [])
        assert len(data) == 12
        row = data[0]
        period = row['period']['label']

        assert row['coverage']['datetimeFrom']['local'] == '2022-01-01T00:00:00-10:00'
        assert row['coverage']['datetimeTo']['local'] == '2022-02-01T00:00:00-10:00'
        assert row['period']['datetimeFrom']['local'] == '2022-01-01T00:00:00-10:00'
        assert row['period']['datetimeTo']['local'] == '2022-02-01T00:00:00-10:00'

        assert row['coverage']['expectedCount'] == 31
        assert row['coverage']['observedCount'] == 31
        assert row['coverage']['percentComplete'] == 100
        assert row['coverage']['percentComplete'] == row['coverage']['percentCoverage']


class TestYears:

    def test_default_good(self, client):
        response = client.get(f"/v3/sensors/{sensors_id}/years")
        data = json.loads(response.content).get('results', [])
        assert response.status_code == 200


    def test_good_with_dates(self, client):
        response = client.get(f"/v3/sensors/{sensors_id}/years?date_from=2022-01-01&limit=1")
        data = json.loads(response.content).get('results', [])
        assert len(data) == 1
        row = data[0]
        assert row['coverage']['expectedCount'] == 8760
        assert row['coverage']['observedCount'] == 365*24
        assert row['coverage']['percentComplete'] == 100
        assert row['coverage']['percentComplete'] == row['coverage']['percentCoverage']
