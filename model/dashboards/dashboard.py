from dashboards.styles import Classic
from dashboards.simulation_control import SimulationControl
from dashboards.meta_information import MetaInformation
from datetime import date
# third party modules
from dash import dcc
from dash import html


class Dashboard:

    def __init__(self):

        self.style = Classic()
        self.layout = html.Div([self.header(), self.tab_menu()])

    def header(self):
        content = html.Div(children=[
            html.H3('Distributed Agent-Based Simulation of the German Energy Market'),
            html.B('FH Aachen University of Applied Sciences', style={'color': '#00adb0'}),
        ], style=self.style.main, id='header')

        return content

    def tab_menu(self):
        content = html.Div([
            dcc.Tabs(id="tab_menu", value='simulation', children=[
                dcc.Tab(label='Simulation Control', value='simulation',
                        style=self.style.tab_menu['style'], selected_style=self.style.tab_menu['select']),
                dcc.Tab(label='Meta Information', value='meta',
                        style=self.style.tab_menu['style'], selected_style=self.style.tab_menu['select']),
                dcc.Tab(label='Agent Information', value='agent',
                        style=self.style.tab_menu['style'], selected_style=self.style.tab_menu['select']),
                dcc.Tab(label='Grid Information', value='grid',
                        style=self.style.tab_menu['style'], selected_style=self.style.tab_menu['select']),
            ], style=self.style.tab_menu['height']),
            html.Div(id='information')
        ])

        return content

    def simulation_control(self, status):
        return SimulationControl(status).container

    def meta_information(self, capacities):
        return MetaInformation(capacities).container


if __name__ == "__main__":

    import dash
    from dash.dependencies import Input, Output, State
    import pandas as pd
    dashboard = Dashboard()
    app = dash.Dash('test')

    @app.callback(Output('information', 'children'), Input('tab_menu', 'value'))
    def render_information(tab):
        if tab == 'simulation':
            return dashboard.simulation_control(status=False)
        if tab == 'meta':
            print('x')
            capacities = pd.DataFrame.from_dict(dict(solar=4, wind=7), orient='index')
            return dashboard.meta_information(capacities)

    @app.callback(Output('simulation_status', 'children'),
                  Input('trigger_simulation', 'on'),
                  State('date_range', 'start_date'),
                  State('date_range', 'end_date'))
    def update_output(on, start, end):
        if on:
            print(start, end)
        else:
            print('simulation not running')
        return 'Simulation is running: {}.'.format(on)

    app.layout = dashboard.layout
    app.run_server(debug=False, port=8000)

