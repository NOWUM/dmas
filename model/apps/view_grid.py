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
        #print("Figure:", time_start - tme.time())

        lines_green_lat = []
        lines_green_lon = []
        lines_red_lat = []
        lines_red_lon = []
        lines_middle_lat = []
        lines_middle_lon = []
        lines_name = []
        lines_powerflow = []
        lines_s_nom = []
        lines_load_factor = []

        line_data = self.influx_con.get_lines_data(date=date + pd.DateOffset(hours=hour))
        #print("Influx:", time_start - tme.time())
        # print(line_data)

        for key, values in line_data.items():
            power_flow, s_nom = values['power'][0], values['s_nom'][0]

            power_color_value = power_flow / s_nom #percentage of line load

            bus0 = self.lines.loc[self.lines['name'] == key, 'bus0'].to_numpy()[0]
            bus1 = self.lines.loc[self.lines['name'] == key, 'bus1'].to_numpy()[0]

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

            lines_middle_lat += [(bus0_y + bus1_y) / 2, None]
            lines_middle_lon += [(bus0_x + bus1_x) / 2, None]
            lines_name += [key, None]
            lines_powerflow += [power_flow, None]
            lines_s_nom += [s_nom, None]
            lines_load_factor += [power_color_value, None]

        #lines_name = list(line_data.keys())'#falsches Format
        #lines_custom_data = [lines_name] + [lines_powerflow] + [lines_s_nom] + [lines_load_factor]#falsches Fortmat
        lines_custom_data = np.stack((lines_name, lines_powerflow, lines_s_nom, lines_load_factor), axis=-1)

        #print("Trace Green:", time_start - tme.time())
        fig.add_trace(
            go.Scattermapbox(
                name='green',
                lon=lines_green_lon,
                lat=lines_green_lat,
                mode='lines',
                line=dict(width=2, color='Green'),
            )
        )
        #print("Trace Red:", time_start - tme.time())
        fig.add_trace(
            go.Scattermapbox(
                name='red',
                lon=lines_red_lon,
                lat=lines_red_lat,
                mode='lines',
                line=dict(width=2, color='Red'),
                hovertext='name'    #TODO: hoverinfo / hovertext possible for lines? --> That's not currently not possible (https://stackoverflow.com/questions/46037897/line-hover-text-in-plotly)
            )
        )
        #print("Trace Nodes:", time_start - tme.time())
        fig.add_trace(go.Scattermapbox(
            name='Node',
            lon=self.buses['x'],
            lat=self.buses['y'],
            mode='markers',
            marker={'allowoverlap': False,
                    'color': 'blue',
                    'size': 10},
            text=self.buses['name'],
            hoverinfo='name+text'
        ))

        #new_customdata = np.stack((lines, 100*np.random.rand(7)), axis=-1)
        # invisible nodes on top of lines (hovertext hack)
        fig.add_trace(go.Scattermapbox(
            name='Line',
            lon=lines_middle_lon,
            lat=lines_middle_lat,
            mode='markers',
            marker={'allowoverlap': False,
                    'color': 'red',
                    'size': 10,
                    'opacity': 0},
            text=lines_name,
            customdata=lines_custom_data, # lines_custom_data = np.stack((lines_name, lines_powerflow, lines_s_nom, lines_load_factor), axis=-1)
            hovertemplate = "<b>%{text}</b><br>" + \
                            #"Point properties:<br>" + \
                            #"Lat: %{lat}<br>" + \
                            #"Lon: %{lon}<br>" + \
                            #"Name: %{customdata[0]}<br>" + \
                            "Powerflow: %{customdata[1]: .2f}<br>" + \
                            "S_nom: %{customdata[2]: .2f}<br>" + \
                            "Load Factor: %{customdata[3]: .5f}",
        ))

        #print("Layout:", time_start - tme.time())
        fig.update_layout(width=700,
            height=800,
            mapbox=dict(
                accesstoken=self.token,
                bearing=0,
                center=go.layout.mapbox.Center(
                    lat=50.977433,
                    lon=10.313721
                ),
                pitch=0,
                zoom=5
            )
        )

        graph_json = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
        #print("End:", time_start - tme.time())
        return graph_json


if __name__ == "__main__":
    pass

