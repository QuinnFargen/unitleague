# app/main.py
from fastapi import FastAPI

from routers import mart, bettor, syndicate, enhancement, txn

app = FastAPI()

app.include_router(mart.router)
app.include_router(bettor.router)
app.include_router(syndicate.router)
app.include_router(enhancement.router)
app.include_router(txn.router)
