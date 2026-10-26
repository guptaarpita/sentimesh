import json, os, time, uuid
from pathlib import Path
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field
import redis

r = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=True)
app = FastAPI(title="SentinelMesh", version="0.1.0")

class JobRequest(BaseModel):
    task_type: str = Field(default="demo", max_length=40)
    payload: dict = Field(default_factory=dict)

@app.get("/health")
def health():
    try:
        r.ping()
        return {"api": "ok", "redis": "ok"}
    except redis.RedisError:
        raise HTTPException(503, "Redis unavailable")

@app.post("/jobs", status_code=202)
def submit_job(req: JobRequest):
    jid, now = str(uuid.uuid4()), time.time()
    r.hset(f"job:{jid}", mapping={"job_id":jid,"status":"queued","task_type":req.task_type,
        "payload":json.dumps(req.payload),"created_at":now,"attempts":0,"result":"","error":""})
    r.xadd("jobs:stream", {"job_id":jid})
    r.xadd("events:stream", {"kind":"job_submitted","job_id":jid,"at":now})
    return {"job_id":jid,"status":"queued"}

@app.get("/jobs")
def jobs():
    result = [r.hgetall(k) for k in r.scan_iter(match="job:*", count=200)]
    result.sort(key=lambda j: float(j.get("created_at",0)), reverse=True)
    return result[:100]

@app.get("/jobs/{jid}")
def job(jid: str):
    result = r.hgetall(f"job:{jid}")
    if not result: raise HTTPException(404, "Job not found")
    return result

@app.get("/workers")
def workers():
    now, out = time.time(), []
    for key in r.scan_iter(match="worker:*", count=100):
        w = r.hgetall(key)
        if w:
            w["age_seconds"] = round(now-float(w.get("last_seen",0)),1)
            w["status"] = "online" if now-float(w.get("last_seen",0)) < 8 else "suspected_down"
            out.append(w)
    return sorted(out, key=lambda x:x.get("worker_id",""))

@app.get("/events")
def events():
    return [{"id":i,**f} for i,f in r.xrevrange("events:stream",count=30)]

@app.get("/", response_class=HTMLResponse)
def dashboard():
    return Path("dashboard.html").read_text(encoding="utf-8")
