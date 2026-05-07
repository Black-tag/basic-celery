from fastapi import FastAPI, Query
from pydantic import BaseModel
import redis.asyncio as aioredis
from contextlib import asynccontextmanager, contextmanager
import json
from fastapi import Request, Depends, HTTPException
import asyncio










app = FastAPI()

class UserEmail(BaseModel):
    userName: str
    emailDescr: str
    
class UserEmailJob(UserEmail):
    retries: int 


@app.on_event("startup")
async def startup():
    app.state.redis = aioredis.Redis(username="blacktag", password="161718", decode_responses=True)
    while await app.state.redis.llen("email_queue:processing") > 0:
        await app.state.redis.lmove(
            "email_queue:processing",
            "email_queue",
            "LEFT",
            "RIGHT"
        )
    await start_workers(7)



def get_redis(request: Request):
    return request.app.state.redis

@app.get("/test")
async def test():
    return {"message": "API is working"}

@app.post("/send-mail")
async def post_to_queue(emailjob: UserEmailJob, redis = Depends(get_redis)):
    # the email should be pushed to queue
    # redis takes on strings so models are not allowed so need to serialise
    serialised_data = emailjob.model_dump_json()

    # need to add it to the queue
    try:
        result = await redis.lpush("email_queue", serialised_data)
    
    except aioredis.ConnectionError:        
        raise HTTPException(
            status_code=503,                 
            detail="Redis is unavailable, try again later"
    )

    return result

# async def do_fake_job(data: dict):
#     print(f"Processing email for: {data['userName']}")
#     await asyncio.sleep(10)
#     print(f"Done processing: {data['userName']}")

async def do_fake_job(data: dict):
    if data["retries"] < 3:
        raise Exception("simulated failure")  # force it to fail
    print(f"processed: {data['userName']}")
    await asyncio.sleep(2)

async def consume_queue():
    worker_redis = aioredis.Redis(username="blacktag", password="161718")

    while True:
        item = await worker_redis.blmove(
            "email_queue",
            "email_queue:processing",
            10,
            src="LEFT",
            dest="RIGHT",
        )

        if item is None:
            continue

        value = item
        data = json.loads(value)
        print(f"📥 picked up job for: {data['userName']}")
        try:
            await do_fake_job(data)
            await worker_redis.lrem("email_queue:processing", 1, value)
            print(f"✅ completed job for: {data['userName']}") 

        except Exception as e:
            print(f"❌ job failed for: {data['userName']} | reason: {e} | attempt: {data['retries']}")
            if data["retries"] < 3:
                data["retries"] += 1
                await worker_redis.lpush("email_queue", json.dumps(data)) 
                await worker_redis.lrem("email_queue:processing", 1, value)
                print(f"🔁 requeued: {data['userName']} | attempt: {data['retries']}")
            else:
                await worker_redis.blmove(
                    "email_queue:processing",
                    "email_queue:failed",
                    10,
                    src="RIGHT",
                    dest="LEFT",
                )
                print(f"💀 moved to failed queue: {data['userName']}")
            

@app.post("/start-worker")
async def start_workers(n: int = Query(3)):
    for i in range(n):
        asyncio.create_task(consume_queue())

@app.get("/queue-status")
async def queue_status(redis=Depends(get_redis)):
    waiting    = await redis.llen("email_queue")
    processing = await redis.llen("email_queue:processing")
    failed     = await redis.llen("email_queue:failed")

    return {
        "waiting": waiting,
        "processing": processing,
        "failed": failed
    }

    
    
