from dotenv import dotenv_values
from viam.rpc.dial import DialOptions
from viam.app.viam_client import ViamClient
from viam.logging import getLogger
from viam.proto.app.data import TabularDataBySQLRequest, TabularDataBySQLResponse
from viam.utils import struct_to_dict, create_filter

config = dotenv_values(".env")

LOGGER = getLogger(__name__)
VIAM_API_KEY_ID = config.get("VIAM_API_KEY_ID") or ""
VIAM_API_KEY = config.get("VIAM_API_KEY") or ""
COMPONENT_NAME = config.get("MOVEMENT_SENSOR_NAME") or ""
ROBOT_ID = config.get("ROBOT_ID") or ""

REQUESTED_ENTRY_COUNT = 500


async def connect() -> ViamClient:
    dial_options = DialOptions.with_api_key(
        api_key=VIAM_API_KEY, api_key_id=VIAM_API_KEY_ID
    )
    return await ViamClient.create_from_dial_options(dial_options)


async def main() -> int:
    LOGGER.info("Connecting to Viam app")
    viam_client = await connect()
    org = await viam_client.app_client.get_organization()
    LOGGER.info(f"Org ID: {org.id} {org.name}")
    data_client = viam_client.data_client
    LOGGER.info("Got data client")

    sensor_filter = create_filter(
        component_name=COMPONENT_NAME,
        robot_id=ROBOT_ID,
    )
    total_count = 0
    sensor_data = []
    while total_count < REQUESTED_ENTRY_COUNT:
        tabular_data, count, last = await data_client.tabular_data_by_filter(
            filter=sensor_filter
        )
        if not tabular_data:
            break
        sensor_data += tabular_data
        total_count += count

    LOGGER.info(
        f"Got the following data: {tabular_data} with count {count} and last {last}"
    )
    # query = f"SELECT * from readings WHERE component_name = '{COMPONENT_NAME}' limit {REQUESTED_ENTRY_COUNT}"
    # sql_request = TabularDataBySQLRequest(sql_query=query, organization_id=org.id)
    # sql_response: TabularDataBySQLResponse = (
    #     await data_client._data_client.TabularDataBySQL(
    #         sql_request, metadata=data_client._metadata
    #     )
    # )
    # data = [struct_to_dict(struct) for struct in sql_response.data]

    # LOGGER.info(f"Query {query} returned the following data: {len(data)}")

    viam_client.close()
    return 0
