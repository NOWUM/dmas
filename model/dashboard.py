# third party modules
import time
import pandas as pd
from flask import Flask, request, redirect
from dash import dcc
from dash import html
import plotly.express as px
from dash.dependencies import Input, Output


class Dashboard:

    def __init__(self, simulation_running):

        self.simulation_running = simulation_running

        content = html.Div(children=[
            html.H1('Docker Agent-based Simulation'),
            html.P(f'simulation is running: {self.simulation_running}'),
            ], style={'width': '95%',
                      'padding': '20px',
                      'height': '95%',
                      'background-color': '#262626',
                      'color': 'white'
                      })

        content.children.append(self.initial_page())

        self.layout = content

    def initial_page(self):

        if not self.simulation_running:
            content = [
                html.Form(children=[
                    html.Div(children=[
                        html.Label('Starting Date:', htmlFor='start_date',
                                   style={'margin-right': '10px', 'margin-bottom': '10px'}),
                        dcc.Input(type="date", id="start_date", name="start_date",
                                  value="1995-01-01", style={'display': 'flex'})
                    ]),
                    html.Div(children=[
                        html.Label('Ending Date:', htmlFor='end_date',
                                   style={'margin-right': '10px', 'margin-bottom': '10px'}),
                        dcc.Input(type="date", id="end_date", name="end_date",
                                  value="1995-02-01", style={'display': 'flex'})
                    ]),
                    dcc.Input(type="submit", value="run simulation", id='run_simulation',
                              style={'margin-top': '10px'})
                ], method='POST', action='/start')
            ]
        else:
            content = [
                html.Form(children=[
                    html.Div(children=[
                        dcc.Input(type="submit", value="stop simulation", id="stop_simulation")
                    ], method='POST', action='/stop')
                ])
            ]

        return html.Div(children=content, style={'padding': '10px'})
