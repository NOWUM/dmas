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

        self.tab_menue = html.Div([
            dcc.Tabs(id="tab_menue", value='pwp_Agent', children=[
                dcc.Tab(label='Fossil Fuel Agents', value='pwp_Agent'),
                dcc.Tab(label='Renewable Energy Agents', value='res_Agent'),
                dcc.Tab(label='Demand Agents', value='dem_Agent'),
                dcc.Tab(label='Storage Agents', value='str_Agent'),
            ]),
            html.Div(id='information')
        ])

        self.agent_dropdown = html.Div([
            dcc.Dropdown(id='agent_dropdown',
                         options=[],
                         ),
            html.Div(id='plots')
        ])

        content.children.append(self.initial_page())
        content.children.append(self.tab_menue)

        self.layout = content

    def plot_data(self, generation):
        content = html.Div(children=[
                    html.B('Generation:'),
                    html.Br(),
                    dcc.Graph(figure=px.line(generation))
                ], style={'margin-top': '30px'})

        return content

    def information(self, agents, agent_type):
        for agent in agents:
            if agent_type in agent:
                self.agent_dropdown.children[0].options.append({'label': agent, 'value': agent})
        content = html.Div(children=[
            html.H6(f'{agent_type.upper()} Agents:'),
            self.agent_dropdown
        ])
        return content

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
