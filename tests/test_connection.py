# tests/test_connection.py
import asyncpg
import asyncio

async def test_db():
    try:
        conn = await asyncpg.connect(
            host='127.0.0.1',
            port=5432,
            user='prefect_user',
            password='StrongPassword123!',
            database='prefect_prod'
        )
        print("Connection successful!")
        await conn.close()
    except Exception as e:
        print(f"Connection failed: {str(e)}")

asyncio.run(test_db())