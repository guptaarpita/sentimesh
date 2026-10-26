import json, os, socket, time, uuid
import redis

r = redis.Redis.from_url(os.getenv("REDIS_URL","redis://localhost:6379/0"), decode_responses=True)
WORKER_ID = os.getenv("WORKER_ID", f"{socket.gethostname()}-{uuid.uuid4().hex[:5]}")
STREAM, GROUP = "jobs:stream", "sentinel-workers"

def event(kind, **fields):
    r.xadd("events:stream", {"kind":kind,"at":time.time(),**{k:str(v) for k,v in fields.items()}})

def heartbeat(active_job=""):
    r.hset(f"worker:{WORKER_ID}", mapping={"worker_id":WORKER_ID,"last_seen":time.time(),"active_job":active_job})

def ensure_group():
    try: r.xgroup_create(STREAM,GROUP,id="0",mkstream=True)
    except redis.ResponseError as e:
        if "BUSYGROUP" not in str(e): raise

def process(mid, fields):
    jid=fields.get("job_id")
    if not jid:
        r.xack(STREAM,GROUP,mid); return
    key=f"job:{jid}"; job=r.hgetall(key)
    if not job:
        r.xack(STREAM,GROUP,mid); return
    attempts=int(job.get("attempts",0))+1
    r.hset(key,mapping={"status":"processing","attempts":attempts,"worker_id":WORKER_ID,"error":""})
    heartbeat(jid); event("job_started",job_id=jid,worker_id=WORKER_ID,attempt=attempts)
    try:
        payload=json.loads(job.get("payload") or "{}")
        duration=max(0,min(10,float(payload.get("duration_seconds",2))))
        if payload.get("fail_once") and attempts == 1:
            time.sleep(min(duration,1)); raise RuntimeError("Simulated first-attempt failure")
        end=time.time()+duration
        while time.time()<end:
            time.sleep(min(.5,max(0,end-time.time()))); heartbeat(jid)
        r.hset(key,mapping={"status":"completed","result":f"Processed by {WORKER_ID} in {duration:.1f}s","completed_at":time.time()})
        event("job_completed",job_id=jid,worker_id=WORKER_ID)
    except Exception as e:
        r.hset(key,mapping={"status":"failed","error":str(e),"failed_at":time.time()})
        event("job_failed",job_id=jid,worker_id=WORKER_ID,error=str(e))
    finally:
        heartbeat("")
        r.xack(STREAM,GROUP,mid)

def main():
    ensure_group(); event("worker_started",worker_id=WORKER_ID)
    print(f"SentinelMesh worker {WORKER_ID} started",flush=True)
    while True:
        heartbeat()
        try:
            claimed=r.xautoclaim(STREAM,GROUP,WORKER_ID,min_idle_time=10000,start_id="0-0",count=1)
            pending=claimed[1] if claimed and len(claimed)>1 else []
            if pending:
                process(*pending[0]); continue
        except redis.ResponseError:
            pass
        rows=r.xreadgroup(GROUP,WORKER_ID,{STREAM:">"},count=1,block=1500)
        for _, messages in rows:
            for message in messages: process(*message)

if __name__ == "__main__": main()
