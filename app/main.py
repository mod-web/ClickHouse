import asyncio

from utils.check import CheckData


cd = CheckData()
asyncio.run(cd.factory())