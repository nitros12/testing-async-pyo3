import async_py_rust
import asyncio

async_py_rust.rust_side_worker()

async def rust_side_test():
    print("rst before")
    await asyncio.sleep(3)
    print("rst after")
    return "hi from py"

async def t():
    print("hi")
    r = await async_py_rust.delay_test(3, "lol")
    print(f"rust coro completed returning")
    print("waiting on python coro from rust")
    async_py_rust.wait_for_py_coro(rust_side_test())
    print("done, waiting to keep the loop alive")
    await asyncio.sleep(7)

asyncio.run(t())
