import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output,State
from dash.exceptions import PreventUpdate
import plotly.express as px
import os
from sqlalchemy import create_engine
import pandas as pd

import datetime
import time
import solarirradiance

host='ec2-52-11-165-246.us-west-2.compute.amazonaws.com'#'10.0.0.8'
user='ubuntu'
passwd=os.environ["MYSQL_SECRET_KEY"]
database = 'zipcloud'
engine = create_engine("mysql://"+user+':'+passwd+'@'+host+'/'+database)

zipcodedf = pd.read_sql('select * from solarPanels',con=engine)
zipcodedf.columns = ['ZipCode','LandSq','Lat','Long','Size','Efficiency']
joindf = None
saveDate = None
earthtilt = 23.45
del zipcodedf['Lat']
del zipcodedf['Long']
del zipcodedf['LandSq']

zipcodedf2 = pd.read_csv('zcta2010.csv')
a = zipcodedf2.columns
a = ['ZipCode']+ list(a)[1:]
zipcodedf2.columns = a
zipcodedf = zipcodedf.join(zipcodedf2.set_index('ZipCode'), on='ZipCode', how='right').fillna(0)
print(zipcodedf)
fig = px.scatter_mapbox(zipcodedf, lat="Latitude", lon="Longitude", hover_name="ZipCode", hover_data=[],color_discrete_sequence=["blue"], zoom=3, height=500)
fig.update_layout(mapbox_style="open-street-map")
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})

def timeslicequery(date1, date2):
    return "(select * from cloudcoverage where timestamp > '"+date1.strftime("%Y-%m-%d %H:%M:%S")+"' and timestamp < '"+date2.strftime("%Y-%m-%d %H:%M:%S")+"')"

def timezipcodequerry(date):
    date +=' 00:00:00'
    postdate =datetime.datetime.strptime(date,"%Y-%m-%d %H:%M:%S")
    
    predate = postdate - datetime.timedelta(days=10)
    timewindowquery = timeslicequery(predate,postdate)
    return "select a.zipcode,a.date, c.cloudcoverage from (select zipcode,max(timestamp) date from " + timewindowquery + " ex group by zipcode) a join " + timewindowquery +" c on c.zipcode=a.zipcode and a.date = c.timestamp;"

app = dash.Dash()

colors = {'background':'#111111','text':'#7FDBFF'}

app.layout = html.Div(style={'backgroundColor': colors['background']}, children=[
    html.H1(
        children='Solar Insight',
        style={
            'textAlign': 'center',
            'color': colors['text']
        }
    ),
    html.Div(children='Analyse Solar Electricity Production', style={
        'textAlign': 'center',
        'color': colors['text']
    }),
    dcc.Graph(
        id='zipCodeMap',
        figure = fig
        
    ),
    html.Div(style={'backgroundColor': colors['background']},
        children=[dcc.RadioItems(id='CCEP',
    options=[
        {'label': 'Cloud Coverage', 'value': 'CC'},
        {'label': 'Electricity Production', 'value': 'EP'}
    ],
        style={
            'textAlign': 'center',
            'color': colors['text']
        },
    value='CC'
    )]),
    html.Div(style={'backgroundColor': colors['background']},
        children=[ dcc.DatePickerSingle(
        id='date-picker-single',
        date='2020-01-16'
        )])
    ,html.Div(style={'backgroundColor': colors['background']},
        children=[ dcc.Input(id='zipcodeInput',value='94025',type='text'),dcc.Graph(id='zipcodeGraph'),dcc.Graph(id='zipcodeElecGraph')])
        ]
)


@app.callback(
    Output(component_id='zipCodeMap', component_property='figure'),
    [Input(component_id='date-picker-single', component_property='date'),
    Input(component_id='CCEP',component_property='value')]
)
def updatemap(date,value):
    global saveDate
    global joindf
    if date != saveDate:
        try:    
            query = timezipcodequerry(date)
        except ValueError:
            print(date)
            raise PreventUpdate
        saveDate = date
        timeslicedf = pd.read_sql(query,con=engine)
        timeslicedf['zipcode']=pd.to_numeric(timeslicedf['zipcode'])
    
        dtdate =datetime.datetime.strptime(date,"%Y-%m-%d")
        dtdate = datetime.date(dtdate.year,dtdate.month,dtdate.day)
        joindf = zipcodedf.join(timeslicedf.set_index('zipcode'), on='ZipCode', how='inner')
        joindf['solarElectricity']=  joindf.apply(lambda row: (1-row['cloudcoverage'])*row['Efficiency']*row['Size']*solarirradiance.currentRadiation(dtdate,  row['Latitude'],  12, (row['Latitude']*.76+3.1)), axis=1)
    
    if value == 'CC':
        shadecolors =  joindf['cloudcoverage']
    elif value == 'EP':
        shadecolors =  joindf['solarElectricity']
    else:
        shadecolors= ['blue']
    fig = px.scatter_mapbox(joindf, lat="Latitude", lon="Longitude", hover_name="ZipCode", hover_data=[],color=shadecolors, zoom=3, height=500)
    fig.update_layout(mapbox_style="open-street-map")
    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    return fig

@app.callback(
    Output(component_id='zipcodeInput', component_property='value'),
    [Input(component_id='zipCodeMap', component_property='clickData')]
)
def updateZipcode(clickData):
    try:
        click = clickData['points'][0]
    except TypeError:
        raise PreventUpdate
    return str(click['hovertext'])

def RepresentsInt(s):
    try: 
        int(s)
        return True
    except ValueError:
        return False

@app.callback(
    Output(component_id='zipcodeGraph', component_property='figure'),
    [Input(component_id='zipcodeInput', component_property='value')]
)
def updateZipcodeGraph(input_value):
    if not(len(input_value)==5 and RepresentsInt(input_value)):
        raise PreventUpdate
    
    query= "select timestamp,cloudcoverage from cloudcoverage where zipcode = '%s' order by timestamp"
    param = (input_value,)
    sqldf = pd.read_sql(query,con=engine,params=param)
    #sqldf['cloudcoverage'] = sqldf['cloudcoverage'].apply(lambda x:((x-.07)/.75)**(1/3))
    fig = px.line(sqldf, x="timestamp", y="cloudcoverage", title='Cloud coverage for '+input_value)
    
    return fig

def numpydatetimetodate(a):
    st = a.__str__()
    year = int(st[:4])
    month = int(st[5:7])
    day = int(st[8:10])
    return datetime.date(year,month,day)

@app.callback(
    Output(component_id='zipcodeElecGraph', component_property='figure'),
    [Input(component_id='zipcodeInput', component_property='value')]
)
def updateZipcodeElectGraph(input_value):
    if not(len(input_value)==5 and RepresentsInt(input_value)):
        raise PreventUpdate
    
    query= "select timestamp,cloudcoverage from cloudcoverage where zipcode = '%s' order by timestamp"
    param = (input_value,)
    sqldf = pd.read_sql(query,con=engine,params=param)
    ziprow = zipcodedf[zipcodedf['ZipCode']==int(input_value)]
    eff = ziprow['Efficiency'].values[0]
    size = ziprow['Size'].values[0]
    lat = ziprow['Latitude'].values[0]
    print(sqldf.dtypes)    
    sqldf['solarElectricity']=  sqldf.apply(lambda row: (1-row['cloudcoverage'])*eff*size*solarirradiance.currentRadiation(numpydatetimetodate(row['timestamp']),  lat,  12, (lat*.76+3.1)), axis=1)
    #sqldf['cloudcoverage'] = sqldf['cloudcoverage'].apply(lambda x:((x-.07)/.75)**(1/3))
    fig = px.line(sqldf, x="timestamp", y='solarElectricity', title='Electricity production for '+input_value)
    
    return fig
if __name__=='__main__':
    host = os.environ["DASH_HOST"]
    app.run_server(host=host, port=80)
