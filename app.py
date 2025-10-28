# app.py — controller heartbeat MVP

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Dict, Optional
from datetime import datetime, timezone, timedelta

app = FastAPI(title="distributed-swarm controller")

# In-memory registry of agents. Later you can swap this for Redis/Postgres.
class Heartbeat(BaseModel):
    agent_name: str = Field(..., description="Unique agent name, e.g., agent-1")
    ip: Optional[str] = Field(None, description="Agent container IP (optional)")
    cpu: Optional[float] = Field(None, description="CPU percent (optional)")
    mem: Optional[float] = Field(None, description="Memory percent (optional)")
    gpu: Optional[str] = Field(None, description="GPU brief summary (optional)")

class AgentView(BaseModel):
    agent_name: str
    last_seen: str
    seconds_since: float
    ip: Optional[str] = None
    cpu: Optional[float] = None
    mem: Optional[float] = None
    gpu: Optional[str] = None

REGISTRY: Dict[str, Dict] = {}

def utc_now():
    return datetime.now(timezone.utc)

@app.get("/healthz")
def healthz():
    return {"status": "ok", "time": utc_now().isoformat()}

@app.post("/heartbeat")
async def heartbeat(hb: Heartbeat, request: Request):
    # If agent didn't send IP, infer from request
    ip = hb.ip or request.client.host
    REGISTRY[hb.agent_name] = {
        "agent_name": hb.agent_name,
        "last_seen": utc_now(),
        "ip": ip,
        "cpu": hb.cpu,
        "mem": hb.mem,
        "gpu": hb.gpu,
    }
    return {"ok": True}

@app.get("/agents")
def list_agents(minutes:int = 10):
    cutoff = utc_now() - timedelta(minutes=minutes)
    out = []
    for row in REGISTRY.values():
        ts = row["last_seen"]
        out.append(AgentView(
            agent_name=row["agent_name"],
            last_seen=ts.isoformat(),
            seconds_since=(utc_now() - ts).total_seconds(),
            ip=row.get("ip"),
            cpu=row.get("cpu"),
            mem=row.get("mem"),
            gpu=row.get("gpu"),
        ))
    # Sort newest first
    out.sort(key=lambda x: x.seconds_since)
    return JSONResponse([o.dict() for o in out])

@app.get("/")
def root():
    return {"service": "controller", "routes": ["/healthz", "/heartbeat (POST)", "/agents"]}
