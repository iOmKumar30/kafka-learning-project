import asyncio
import aiohttp

actions = [
    "LOGIN",
    "SIGNUP",
    "PURCHASE",
    "LOGOUT",
    "UPDATE_PROFILE"
]

async def send_event(session, user_id, action):
    payload = {
        "user_id": str(user_id),
        "action": action
    }

    async with session.post(
        "http://localhost:8000/events",
        json=payload
    ) as response:
        print(payload, response.status)

async def main():
    async with aiohttp.ClientSession() as session:
        tasks = []

        for i in range(15):
            tasks.append(
                send_event(
                    session,
                    101 + i,
                    actions[i % 5]
                )
            )

        await asyncio.gather(*tasks)

asyncio.run(main())