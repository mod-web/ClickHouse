import asyncio
import random
from datetime import date, timedelta
from typing import AsyncGenerator


class CreateData:
    """ Prepare tables and data """

    async def create_factory(self) -> None:
        """ Main operations """

        await self.create_table()

        async for (
                block_product,
                block_remainders
        ) in self.create_data(50000, 200):

            await asyncio.gather(
                self.client.insert('wb.products', block_product),
                self.client.insert('wb.remainders', block_remainders)
            )

    async def create_table(self) -> None:
        """ Prepare tables """

        query_products = """
            CREATE TABLE IF NOT EXISTS wb.products (
                product_id Int32,
                product_name String,
                brand_id Int32,
                seller_id Int32,
                updated Date
            )
            ENGINE = ReplacingMergeTree
            ORDER BY product_id
        """

        query_remainders = """
            CREATE TABLE IF NOT EXISTS wb.remainders (
                date Date,
                product_id Int32,
                remainder Int32,
                price Int32,
                discount Int32,
                pics Int32,
                rating Int32,
                reviews Int32,
                new Bool
            )
            ENGINE = ReplacingMergeTree
            ORDER BY (date, product_id)
        """

        await self.client.query(query_products)
        await self.client.query(query_remainders)

    async def create_data(
        self,
        block_size: int = 50000,
        iterate: int = 200,
        unique_ratio: float = 0.5,
    ) -> AsyncGenerator[tuple[list, list], None]:
        """ Prepare fake data for tables """

        today = date.today()
        yesterday = today - timedelta(days=1)

        unique_count = int(block_size * unique_ratio)

        for _ in range(iterate):
            block_product = []
            block_remainders = []

            for _ in range(block_size):
                product_id = random.randint(0, unique_count - 1)
                product_name = ''.join(
                    random.choice('abcdefghijklmnopqrstuvwxyz')
                    for _ in range(random.randint(4, 10))
                ).title()

                block_product.append((
                    product_id,
                    product_name,
                    random.randint(1, 100),
                    random.randint(1, 50),
                    random.choice([today, yesterday])
                ))

                block_remainders.append((
                    random.choice([today, yesterday]),
                    product_id,
                    random.randint(0, 1000),
                    random.randint(100, 10000),
                    random.randint(0, 50),
                    random.randint(0, 20),
                    random.randint(1, 5),
                    random.randint(0, 1000),
                    random.choice([True, False])
                ))

            yield block_product, block_remainders
