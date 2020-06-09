import os
os.chdir(os.path.dirname(os.path.dirname(__file__)))
import pandas as pd
import numpy as np
import pypsa
from interfaces.interface_Influx import influxInterface
from plotly.colors import label_rgb, find_intermediate_color
import json
import plotly
import os
os.environ['NUMEXPR_MAX_THREADS'] = '64'
os.environ['NUMEXPR_NUM_THREADS'] = '32'


class gridModel:

    def __init__(self, influx='149.201.88.150', dbName='MAS_2020'):

        self.ConnectionInflux = influxInterface(database=dbName, host=influx)

        self.network = pypsa.Network()
        buses = pd.read_excel(r'./data/Grid_Buses.xlsx', index_col=0)
        lines = pd.read_excel(r'./data/Grid_Lines.xlsx', index_col=0)
        transformers = pd.read_excel(r'./data/Grid_Trafos.xlsx', index_col=0)

        for i in range(len(buses)):
            try:
                values = buses.iloc[i, :]
                self.network.add("Bus", name=values['name'], v_nom=values['v_nom'], x=values['x'], y=values['y'])
            except:
                print('cant add Bus %s to network' % values['name'])

        for i in range(len(lines)):
            try:
                values = lines.iloc[i, :]
                self.network.add("Line", name=values['name'].split('_')[0] + '_' + str(i), bus0=values['bus0'], bus1=values['bus1'], x=values['x'], r=values['r'], s_nom=values['s_nom'])
            except Exception as e:
                print(e)
                print('cant add Line %s to network' % values['name'])

        for i in range(len(transformers)):
            try:
                values = transformers.iloc[i, :]
                self.network.add("Transformer", name=values['name'], bus0=values['bus0'], bus1=values['bus1'], s_nom=2000., x=22.9947/2000., r=0.3613/2000., model='t')
            except:
                print('cant add Transformator %s to network' % values['name'])

        for i in range(1, 100):
            if '%s_380' % i in buses.to_numpy() or '%s_220' % i in buses.to_numpy() and i not in [5, 11, 20, 43, 60, 62, 70, 80]:
                if '%s_380' % i in buses.to_numpy():
                    self.network.add("Load", 'Load_%s' % i, p_set=0, bus='%s_380' % i)
                    self.network.add("Generator", 'Gen_%s' % i, p_set=0, control='PV', bus='%s_380' % i)
                else:
                    self.network.add("Load", 'Load_%s' % i, p_set=0, bus='%s_220' % i)
                    self.network.add("Generator", 'Gen_%s' % i, p_set=0, control='PV', bus='%s_220' % i)

        print('Stop Building Grid')

    def colormap(self, i):
        color = (label_rgb([int(n) for n in find_intermediate_color([0., 0., 255.], [255., 0., 0.], i)]))
        code = eval(color.split('rgb')[-1])
        code = (max(0, code[0]), max(0, code[1]), max(0, code[2]))
        rgbString = 'rgb' + str(code)
        return rgbString


    def powerFlow(self, date, hour):
        total = [self.ConnectionInflux.getPowerArea(date=date, area=i) for i in range(1, 100)]

        index = 1
        for row in np.asarray(total):
            if index not in [5, 11, 20, 43, 60, 62, 70, 80]:
                load = [val if val >= 0 else 0 for val in row]
                gen = [-1 * val if val < 0 else 0 for val in row]
                self.network.loads.loc['Load_%s' % index, 'p_set'] = load[hour]
                self.network.generators.loc['Gen_%s' % index, 'p_set'] = gen[hour]
            index += 1

        self.network.pf(distribute_slack=True)

    def getPlot(self, height=600, hour=0):

        loading = self.network.lines_t.p0.loc['now'] / self.network.lines.s_nom
        factor = 750/850
        size = height
        fig = self.network.iplot(title='Lastfluss zur Stunde ' + str(hour),
                                 line_colors=abs(loading).map(self.colormap),
                                 bus_sizes=5,
                                 size=(size*factor, size),
                                 line_text='Line ' + self.network.lines.index + ' has loading ' + abs(100 * loading).round(1).astype(str) + '%',
                                 iplot=False)
        fig['layout'].update(dict(plot_bgcolor='rgb(255,255,255)'))
        graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

        return graphJSON

if __name__ == "__main__":

    test = gridModel()

    json_body = []
    date = pd.to_datetime('2019-05-05')
    lines = test.network.lines

    for i in range(24):
        time = date + pd.DateOffset(hours=i)
        test.powerFlow(pd.to_datetime('2019-05-05'), i)
        data = test.network.lines_t
        power = data.p0.loc['now']

        for element in power.items():
            json_body.append(
                {
                    "measurement": 'Grid',
                    "tags": dict(area1=int(element[0].split('_')[0].split('t')[0].replace('f','')),
                                 area2=int(element[0].split('_')[0].split('t')[-1].split('V')[0].replace('t','')),
                                 voltage=int(element[0].split('_')[0][-3:])),
                    "time": time.isoformat() + 'Z',
                    "fields": dict(power=element[1],
                                   powerMax=lines.loc[element[0],'s_nom'],
                                   load=(np.abs(element[1])/lines.loc[element[0],'s_nom'])*100)
                }
            )