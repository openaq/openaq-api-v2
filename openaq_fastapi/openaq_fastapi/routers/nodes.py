import logging
import time

import jq
import orjson as json
from aiocache import cached
from fastapi import APIRouter, Depends

from .base import DB, Filters, Paging

logger = logging.getLogger("nodes")
logger.setLevel(logging.DEBUG)

router = APIRouter()


@cached(900)
def runjq(compiled, data):
    start_time = time.time()
    ret = compiled.input(data).all()
    data_time = time.time() - start_time
    logger.debug(f"data: {data_time}")
    return ret


class Nodes:
    def __init__(
        self,
        db: DB = Depends(),
        filters: Filters = Depends(),
        paging: Paging = Depends(),
    ):
        self.db = db
        self.filters = filters
        self.paging = paging

    async def fetch_data(self):
        db = self.db
        filters = self.filters
        paging = self.paging

        paging_q = await paging.sql()
        where_q = await filters.sql()
        params = {}
        params.update(paging_q["params"])
        params.update(where_q["params"])

        where_sql = where_q["q"]
        paging_sql = paging_q["q"]

        q = f"""
        SELECT
            count(*) over () as count,
            json
        FROM sensor_nodes_json
        WHERE {where_sql}
        {paging_sql}
        """

        meta = {
            "name": "openaq-api",
            "license": "CC BY 4.0",
            "website": "https://docs.openaq.org/",
            "page": paging.page,
            "limit": paging.limit,
            "found": None,
        }
        rows = await db.fetch(q, params)
        if len(rows) == 0:
            meta["found"] = 0
            return {"meta": meta, "results": []}
        meta["found"] = rows[0]["count"]
        json_rows = [json.loads(r[1]) for r in rows]

        return {"meta": meta, "results": json_rows}


# @router.get("/nodes")
async def get_data_nodes(nodes: Nodes = Depends()):
    data = await nodes.fetch_data()
    return data


locations_jq = jq.compile(
    """
    . | {
        meta: .meta,
        results: [
            .results[] |
            {
                id: .sensor_nodes_id,
                country: .country,
                city: .city,
                cities: .cities,
                location: .site_name,
                locations: .site_names,
                soureName: .source_name,
                sourceNames: .source_names,
                sourceType: .source_type,
                sourceTypes: .source_types,
                coordinates: {
                    longitude: .geom.coordinates[0],
                    latitude: .geom.coordinates[1]
                },
                firstUpdated: .sensor_systems[0].sensors[0].first_datetime,
                lastUpdated: .sensor_systems[0].sensors[0].last_datetime,
                parameters : [ .sensor_systems[].sensors[].measurand ],
                countsByMeasurement: [
                    .sensor_systems[].sensors[] | {
                        parameter: .measurand,
                        count: .value_count
                    }
                ],
                count: .sensor_systems[].sensors | map(.value_count) | add
            }
        ]
    }
    """
)


# @router.get("/locations")
async def get_data(nodes: Nodes = Depends()):
    data = await nodes.fetch_data()
    return locations_jq.input(data).first()


latest_jq = jq.compile(
    """
    . | {
        meta: .meta,
        results: [
            .results[] |
            {
                location: .site_name,
                city: .city,
                country: .country,
                measurements: [
                    .sensor_systems[].sensors[] | {
                        parameter: .measurand,
                        value: .last_value,
                        lastUpdated: .last_datetime,
                        unit: .units,
                        sourceName: .source_name?
                    }
                ]
            }
        ]
    }
    """
)


# @router.get("/latest")
async def get_data_latest(nodes: Nodes = Depends()):
    data = await nodes.fetch_data()
    ret = latest_jq.input(data).first()
    return ret
