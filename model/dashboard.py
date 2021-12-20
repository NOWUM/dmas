# third party modules
import time
import pandas as pd
from flask import Flask, request, redirect
import dash
from dash import dcc
from dash import html
import plotly.express as px
from dash.dependencies import Input, Output

app = dash.Dash('dMAS_controller')


class Dashboard:

    def __init__(self, simulation_running):

        self.simulation_running = simulation_running
        self.content = html.Div()

        app.layout = html.Div(children=[
            html.H1('Docker Agent-based Simulation'),
            html.P(f'simulation is running: {self.simulation_running}'),
            self.content
            ], style={'width': '80%', 'margin': 'auto', 'height': '80%',
                      'background-color': '#262626'})

        if not self.simulation_running:
            content = [
                html.Form(children=[
                    html.Div(children=[
                        html.Label('Starting Date:', htmlFor='start_date',
                                   style={'margin-right': '10px'}),
                        dcc.Input(type="date", id="start_date", name="start_date",
                                  value="1995-01-01", style={'display': 'flex'})
                    ]),
                    html.Div(children=[
                        html.Label('Ending Date:', htmlFor='end_date',
                                   style={'margin-right': '10px'}),
                        dcc.Input(type="date", id="end_date", name="end_date",
                                  value="1995-02-01", style={'display': 'flex'})
                    ]),
                    dcc.Input(type="submit", value="run simulation", id='run_simulation')
                ], method='POST', action='/start')
            ]
        else:
            content = [
                html.Form(children=[
                    html.Div(children=[
                        dcc.Input(type="submit", value="stop simulation", id="stop_simulation")
                    ], method='POST', action ='/stop')
                ])
            ]

        self.content.children.append(content)

    app.run_server(debug=False, port=5000, host='0.0.0.0')


if __name__ == "__main__":
    mydash = Dashboard(simulation_running=False)