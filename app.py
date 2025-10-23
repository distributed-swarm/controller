from fastapi import FastAPI
app = FastAPI()

@app.get("/")
def root():
    return {"status": "ok", "service": "controller", "version": "0.1"}

@app.get("/healthz")
def healthz():
    return {"ok": True}
