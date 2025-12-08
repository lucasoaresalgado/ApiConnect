from fastapi import FastAPI
import uvicorn
from extractScript import run_extraction_thread, api_local_key 
from Catalog import extractions
import asyncio

app = FastAPI()

@app.get("/catalog")
def showCatalog():
    return [extractions]

@app.get("/data/{name}")
def showTable(name):
    if name in extractions:
       return [extractions[name]]
    else:
        return {"status": f"data {name} not found."}
    
@app.get("/schedule/{schedule}")
def showSchedule(schedule: str):
    results = [
        {key: value}
        for key, value in extractions.items()
        if value.get("schedule") == schedule
    ]
    if results:
        return results
    else:
        return {"status": f"schedule {schedule} not found."}


@app.post("/extract/all")
async def exec_call():
    await asyncio.gather(*(run_extraction_thread(key) for key in extractions.keys()))       
    return {"status": "Done."}

@app.post("/extract/{name}")
async def exec_call_name(name: str):
    if name in extractions:
        await run_extraction_thread(name)
        return {"status": f"Extraction {name} completed."}
    else:
        return {"status": f"Extraction {name} not found."}

@app.post("/extract/schedule/{schedule}")
async def exec_call_schedule(schedule: str):
    tasks = []
    for key, value in extractions.items():
        if value.get("schedule") == schedule:
            tasks.append(run_extraction_thread(key))
    if tasks:
        await asyncio.gather(*tasks)
        return {"status": f"Extractions with schedule {schedule} completed."}
    else:
        return {"status": f"No extractions found with schedule {schedule}."}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
