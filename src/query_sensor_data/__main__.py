import asyncio
import query_sensor_data
import sys

sys.exit(asyncio.run(query_sensor_data.main()))
