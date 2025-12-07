import clickhouse_connect

from .timer import async_timed
from .create import CreateData
from .config import settings


class CheckData(CreateData):
    """ Check query execution """

    async def init(self) -> None:
        """ Init DB connection """

        self.client = await clickhouse_connect.get_async_client(
            host=settings.ch.host,
            port=settings.ch.port,
            username=settings.ch.username,
            password=settings.ch.password,
            database=settings.ch.db,
        )

    async def factory(self) -> None:
        """ Main operations """

        # Init & Create | DB & Data
        await self.init()
        await self.create_factory()

        # Test | Query
        await self.example_query()
        await self.distinct_query()
        await self.group_query()
        await self.distinct_query_with_cte()
        await self.group_query_with_cte()

    @async_timed()
    async def example_query(self) -> None:
        """ Query from example """

        query = """
            SELECT product_id 
            FROM products p
            FINAL JOIN remainders r
            FINAL USING(product_id) 
            WHERE p.updated = today() 
            AND r.date = today()-1
        """

        await self.client.query(query)

    @async_timed()
    async def distinct_query(self) -> None:
        """ Query with DISTINCT """

        query = """            
            SELECT DISTINCT product_id
            FROM products
            WHERE updated = today() 
            AND product_id IN (
                SELECT product_id
                FROM remainders
                WHERE date = today() - 1
            )
        """

        await self.client.query(query)

    @async_timed()
    async def group_query(self) -> None:
        """ Query with GROUP BY """

        query = """
            SELECT product_id
            FROM products
            WHERE updated=today() 
            AND product_id IN (
                SELECT product_id
                FROM remainders
                WHERE date = today() - 1
            )
            GROUP BY product_id
        """

        await self.client.query(query)

    @async_timed()
    async def distinct_query_with_cte(self) -> None:
        """ Query with DISTINCT with CTE """

        query = """
            WITH
            cte_remainders AS (
                SELECT product_id
                FROM remainders
                WHERE date = today() - 1
            )
            SELECT DISTINCT product_id
            FROM products
            WHERE updated = today()
            AND product_id IN (
                SELECT product_id FROM cte_remainders
            )
        """

        await self.client.query(query)

    @async_timed()
    async def group_query_with_cte(self) -> None:
        """ Query with GROUP BY with CTE """

        query = """
            WITH 
            cte_remainders AS (
                SELECT product_id
                FROM remainders
                WHERE date = today() - 1
            )
            SELECT product_id
            FROM products
            WHERE updated = today()
            AND product_id IN (
                SELECT product_id FROM cte_remainders
            )
            GROUP BY product_id
        """

        await self.client.query(query)
