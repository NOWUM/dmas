import os
os.chdir(os.path.dirname(os.path.dirname(__file__)))
import configparser
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
pio.renderers.default = "json"
from plotly.colors import label_rgb, find_intermediate_color
import numpy as np
from interfaces.interface_Influx import InfluxInterface
import json
import plotly
import os
import time as tme

class GridView:

    def __init__(self):
        config = configparser.ConfigParser()
        config.read(r'./app.cfg')
        database = config['Results']['Database']
        influx_host = config['InfluxDB']['Host']
        self.influx_con=InfluxInterface(host=influx_host, database=database)
        self.buses = pd.read_csv(r'./data/Grid_Buses.csv', sep=';', decimal=',', index_col=0)
        self.lines = pd.read_csv(r'./data/Grid_Lines.csv', sep=';', decimal=',', index_col=0)
        self.token = 'pk.eyJ1Ijoicmlla2VjaCIsImEiOiJjazRiYTdndXkwYnN3M2xteGN2MHhtZjB0In0.33tSDK45TXF3lb3-G147jw'


    def __color_map(self, i):
        color = (label_rgb([int(n) for n in find_intermediate_color([0., 0., 255.], [255., 0., 0.], i)]))
        code = eval(color.split('rgb')[-1])
        code = (max(0, code[0]), max(0, code[1]), max(0, code[2]))
        rgbString = 'rgb' + str(code)
        return rgbString

    def get_plot(self, date, hour):

        time_start = tme.time()
        fig = go.Figure()
        print("Figure:",time_start - tme.time())
        power_color_value = []
        lines_green_lat = []
        lines_green_lon = []
        lines_red_lat = []
        lines_red_lon = []
        for item in range(len(self.lines)):
            print("Item {}:".format(item), time_start - tme.time())
            bus0 = self.lines.iloc[item, 1]
            bus1 = self.lines.iloc[item, 2]

            name = self.lines.iloc[item, 0].split('_')[0] + '_' + str(item)
            power_flow, s_nom = self.influx_con.get_line_data(date=date, line=name)
            power_color_value = power_flow[hour] / s_nom[hour]

            #color = self.__color_map(np.random.uniform(low=0, high=1))

            bus0_x = self.buses.loc[self.buses['name'] == bus0, 'x'].to_numpy()[0]
            bus1_x = self.buses.loc[self.buses['name'] == bus1, 'x'].to_numpy()[0]

            bus0_y = self.buses.loc[self.buses['name'] == bus0, 'y'].to_numpy()[0]
            bus1_y = self.buses.loc[self.buses['name'] == bus1, 'y'].to_numpy()[0]

            if power_color_value > 0.1:
                #color = 'Red'
                lines_red_lat = lines_red_lat + [bus0_y, bus1_y, None]
                lines_red_lon = lines_red_lon + [bus0_x, bus1_x, None]
            else:
                #color = 'Green'
                lines_green_lat = lines_green_lat + [bus0_y, bus1_y, None]
                lines_green_lon = lines_green_lon + [bus0_x, bus1_x, None]

        print("Trace Green:", time_start - tme.time())
        fig.add_trace(
            go.Scattermapbox(
                name='green',
                showlegend=False,
                lon=lines_green_lon,
                lat=lines_green_lat,
                mode='lines',
                line=dict(width=2, color='Green'),
            )
        )
        print("Trace Red:", time_start - tme.time())
        fig.add_trace(
            go.Scattermapbox(
                name='red',
                showlegend=False,
                lon=lines_red_lon,
                lat=lines_red_lat,
                mode='lines',
                line=dict(width=2, color='Red'),
            )
        )
        print("Trace Nodes:", time_start - tme.time())
        fig.add_trace(go.Scattermapbox(
            name='Nodes',
            lon=self.buses['x'],
            lat=self.buses['y'],
            mode='markers',
            marker={'allowoverlap': False,
                    'color': 'blue',
                    'size': 10}
        ))

        print("Layout:", time_start - tme.time())
        fig.update_layout(
            mapbox=dict(
                accesstoken=self.token,
                bearing=0,
                center=go.layout.mapbox.Center(
                    lat=50.977433,
                    lon=10.313721
                ),
                pitch=0,
                zoom=4
            )
        )

        graph_json = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
        print("End:", time_start - tme.time())
        return graph_json


if __name__ == "__main__":
    pass

