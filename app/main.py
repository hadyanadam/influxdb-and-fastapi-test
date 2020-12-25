from fastapi import FastAPI, Depends, HTTPException, status
from datetime import datetime
from pydantic import BaseModel, Field
from typing import List
from influxdb import InfluxDBClient, SeriesHelper
from influxdb_client import InfluxDBClient as FluxClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS

app = FastAPI()

def get_session_db():
    client = InfluxDBClient(host='localhost',port=8086)
    client.switch_database('influxdb-test')
    try:
        yield client
    finally:
        client.close()

def get_flux_session():
    client = FluxClient(url="http://localhost:8086", token="h0zgWrmVbUIqjIyI4A53B0sAY7l5S2YzvmCPskQKFIUcZjNUQxSfA0FO8ejx8wLnpqf8gAtPBJrDNmxXbQay7A==", org="sundaya", debug=True)
    try:
        yield client
    finally:
        client.__del__()

class SuhuTags(BaseModel):
    user: str
    suhuId: int

class SuhuFields(BaseModel):
    value: float


class SuhuBase(BaseModel):
    measurement: str
    tags: SuhuTags
    time: datetime
    fields_: SuhuFields = Field(...,alias='fields')

    # class Config:
    #     orm_mode=True

class SuhuSeriesHelper(SeriesHelper):
    class Meta:
        client=next(get_session_db())
        series_name= 'suhu'
        fields = ['time', 'value']
        tags = ['user', 'suhuId']
        bulk_size=5
        autocommit= True

@app.get('/')
def index():
    return "test influxdb"

@app.get('/influxql/suhu')
def view_suhu(client: InfluxDBClient=Depends(get_session_db)):
    result = client.query('SELECT "value" FROM "influxdb-test"."autogen"."suhu" GROUP BY "user"')
    print(result)
    return result.raw

@app.post('/influxql/suhu', status_code=status.HTTP_201_CREATED)
def create_suhu(data:List[SuhuBase], client: InfluxDBClient=Depends(get_session_db)):
    data_list = [d.dict(by_alias=True) for d in data]
    for data in data_list:
        tags = data.get('tags')
        fields = data.get('fields')
        SuhuSeriesHelper(time=data.get('time'), **tags, **fields)
    response = SuhuSeriesHelper._json_body_()
    if SuhuSeriesHelper.commit():
        return {"status":"success", "details": f"{len(response)} points created"}
    else:
        raise HTTPException(status=400, detail='Data not pushed')

@app.post('/flux/suhu', status_code=status.HTTP_201_CREATED)
def flux_create_suhu(data: List[SuhuBase], client: FluxClient= Depends(get_flux_session)):
    write_api = client.write_api(write_options=SYNCHRONOUS)
    data_list = [d.dict(by_alias=True) for d in data]
    points = [
            Point(data.get('measurement')) \
                .tag('user', data.get('tags').get('user')) \
                .tag('suhuId', data.get('tags').get('suhuId')) \
                .field('value', data.get('fields').get('value'))
                # .time(data.get('time'))
            for data in data_list
        ]
    print(points)
    # for point in points:
    #     print(point.__dict__)
    write_api.write(bucket="test",record=points)
    write_api.__del__()
    return {"details" : f"{len(points)} data created."}

@app.get('/flux/suhu')
def flux_view_suhu(client : FluxClient = Depends(get_flux_session)):
    query_api = client.query_api()
    tables = query_api.query('from(bucket:"test") |> range(start: -50m)')
    response = []
    for table in tables:
        print(len(table.records))
        for record in table.records:
            response.append(record.values)
    return response
