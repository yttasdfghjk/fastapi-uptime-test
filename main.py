from fastapi import FastAPI, status
from apscheduler.schedulers.background import BackgroundScheduler
from contextlib import asynccontextmanager
import uvicorn
import pandas as pd
import numpy as np
import ccxt
import requests
from pydantic import BaseModel

from datetime import datetime, timezone

"""
counter = 1

def job_counter():
    global counter
    print('cron job: call you https requests here...')
    counter = counter + 1
"""
"""
@asynccontextmanager
async def lifespan(_: FastAPI):
    print('app started....')
    scheduler = BackgroundScheduler()
    scheduler.add_job(id="job1", func=job_counter, trigger='cron', second='*/2')
    scheduler.start()
    yield
    print('app stopped...')
    scheduler.shutdown(wait=False)
"""

ex = ccxt.binance()

tickers = ["ETH/USDT", "BTC/USDT","SOL/USDT","ICP/USDT","MATIC/USDT"] # config.file
limit = 1000

token = '1879523884:AAEWm9uh7JAHVblGq6_mbMCcl3A3yfF04n8' # os.env

def send_telegram_msg(msg):
    params = {"chat_id":"1842657441", "text":msg}
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    message = requests.post(url, params=params)
    message

def get_ohlc(ticker, timeframe, limit):     # default1000limit und typechecks?!
    ohlcv = ex.fetch_ohlcv(ticker, timeframe, limit)
    df = pd.DataFrame(ohlcv, columns = ['Time','Open', 'High', 'Low', 'Close', 'Volume'])
    df['Time'] = pd.to_datetime(df.Time, unit='ms')
    df.set_index("Time", inplace=True)
    df['datetimes'] = df.index
    return df

def populate_features(df):
    """
    returns a DataFrame with all potential signals included
    """
    add_volume_features(df)
    # add_candles(df)
    # add_ema_signals(df) #ema1,ema2,ema3?!
    return df

def add_volume_features(df):
    """
    returns a Dataframe with spikes/no spikes (1/0) as well as spike ratios
    """
    df['avgVolume'] = df['Volume'].rolling(window=20).mean()
    spikeMultiple = 2   #defualt value
    df["VolumeSpike"] = np.where(df['Volume'] > spikeMultiple*df["avgVolume"], 1, 0)
    df["spikeRatio"] = (df["Volume"]/df["avgVolume"])
    return df

def send_signals(ticker, df, timeframe):
    """
    todo: send only one message for all ohlc dataframe in a timeframe
    """
    local_now = datetime.now()
    local_time = local_now.strftime("%d:%m:%Y-%H:%M:%S")
    utc_now = local_now.astimezone(timezone.utc)
    utc_time = utc_now.strftime("%d:%m:%Y-%H:%M:%S")
    last_candle = df.iloc[-1]        

    if last_candle.VolumeSpike == 1:        # signal
        send_telegram_msg("Local Time: "+local_time+" | "+"UTC/BTC Time: "+utc_time+"\nSignal:"+" | "+ticker+" | "+timeframe+" | "+"Significant Volume Spike")
    elif last_candle.spikeRatio > 1:        # warning
        send_telegram_msg("Local Time: "+local_time+" | "+"UTC/BTC Time: "+utc_time+"\nWarning:"+" | "+ticker+" | "+timeframe+" | "+" | "+"Critical Spike Ratio")
    else:                                   # pass, what else?
        pass    

def signals_job(tickers, timeframe):
    """
    gets OHLC Dataframes for all tickers in a timeframe
    todo: only on init get all data, later append only, store data in database
    """
    # dfPriceList # tempList of DFs --> 1 signals message per timeframe
    for t in tickers:
        df = get_ohlc(t, timeframe, limit=1000)
        df = populate_features(df)
        send_signals(t, df, timeframe)
        # store_data(df, timeframe) #dfPriceList

def store_data(df):
    """
    stores Data in Excel, DuckDb, SQLite or Postgres
    todo: setup storage, test storage, adapt signals() 
    """    


@asynccontextmanager
async def lifespan(_: FastAPI):
    print('Screener app started...')
    scheduler = BackgroundScheduler()
    scheduler.add_job(id="1d", func=signals_job,args=[tickers,"1d"], trigger='cron', day='*')
    #scheduler.add_job(id="4h", func=signals, trigger='cron', second='*/2')
    scheduler.add_job(id="1h", func=signals_job, args=[tickers,"1h"], trigger='cron', hour='*', jitter=30)
    #scheduler.add_job(id="15m", func=signals, trigger='cron', minute='0,15,30,45', jitter=10)
    scheduler.add_job(id="15m", func=signals_job, args=[tickers,"15m"], trigger='cron', minute='*/15', jitter=10)
    #scheduler.add_job(id="5m", func=signals, trigger='cron', minute='0,5,10,15,20,25,30,35,40,45,50,55')
    scheduler.add_job(id="5m", func=signals_job, args=[tickers,"5m"], trigger='cron', minute='*/5')
    scheduler.start()
    yield
    print('Screener app stopped...')
    scheduler.shutdown(wait=False)


app = FastAPI(lifespan=lifespan)

@app.get("/")
async def index():
   return {"message": "Hello World"}

@app.get("/home")
async def root():
   return {"message": "Hello Home"}
    
class HealthCheck(BaseModel):
    """Response model to validate and return when performing a health check."""
    
    status: str = "OK"


@app.get(
    "/health",
    tags=["healthcheck"],
    summary="Perform a Health Check",
    response_description="Return HTTP Status Code 200 (OK)",
    status_code=status.HTTP_200_OK,
    response_model=HealthCheck,
)
def get_health() -> HealthCheck:
    """
    ## Perform a Health Check
    Endpoint to perform a healthcheck on. This endpoint can primarily be used Docker
    to ensure a robust container orchestration and management is in place. Other
    services which rely on proper functioning of the API service will not deploy if this
    endpoint returns any other HTTP status code except 200 (OK).
    Returns:
        HealthCheck: Returns a JSON response with the health status
    """
    return HealthCheck(status="OK")

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000)
