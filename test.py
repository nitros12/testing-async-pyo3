import async_py_rust
import asyncio

async_py_rust.rust_side_worker()

async def t():
    print("hi")
    fut = async_py_rust.delay_test(3, "lol")
    print(fut)
    r = await fut
    print(r)

asyncio.run(t())
