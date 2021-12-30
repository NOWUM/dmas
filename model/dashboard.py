# third party modules
import time
import pandas as pd
from flask import Flask, request, redirect
from dash import dcc
from dash import html
import plotly.express as px
from dash.dependencies import Input, Output


class Dashboard:

    def __init__(self):

        self.tab_menu_style = {
            'height': {'height': '44px'},
            'style': {'borderBottom': '1px solid #d6d6d6',
                      'padding': '6px',
                      'fontWeight': 'bold',
                      'backgroundColor': '#363636',
                      'color': 'white'},
            'select': {'borderTop': '1px solid #d6d6d6',
                       'borderBottom': '1px solid #d6d6d6',
                       'backgroundColor': '#00adb0',
                       'color': 'white',
                       'fontWeight': 'bold',
                       'padding': '6px'}
        }

        self.main_style = {'padding': '20px',
                           'height': '95%',
                           'background-color': '#262626',
                           'color': 'white'}

        self.header = html.Div(children=[
            html.H3('Distributed Agent-Based Simulation of the German Energy Market'),
            html.B('Fachhochschule Aachen', style={'color': '#00adb0'}),
        ], style=self.main_style, id='header')

        self.tab_menu = self.get_tab_menu()

        self.layout = html.Div([self.header, self.tab_menu])

    def get_tab_menu(self):
        content = html.Div([
            dcc.Tabs(id="tab_menu", value='simulation', children=[
                dcc.Tab(label='Simulation Control', value='simulation',
                        style=self.tab_menu_style['style'], selected_style=self.tab_menu_style['select']),
                dcc.Tab(label='PWP Agents', value='pwp_Agent',
                        style=self.tab_menu_style['style'], selected_style=self.tab_menu_style['select']),
                dcc.Tab(label='RES Agents', value='res_Agent',
                        style=self.tab_menu_style['style'], selected_style=self.tab_menu_style['select']),
                dcc.Tab(label='DEM Agents', value='dem_Agent',
                        style=self.tab_menu_style['style'], selected_style=self.tab_menu_style['select']),
                dcc.Tab(label='STR Agents', value='str_Agent',
                        style=self.tab_menu_style['style'], selected_style=self.tab_menu_style['select']),
                dcc.Tab(label='MRK Agent', value='mrk_Agent',
                        style=self.tab_menu_style['style'], selected_style=self.tab_menu_style['select']),
                dcc.Tab(label='TSO Agent', value='tso_Agent',
                        style=self.tab_menu_style['style'], selected_style=self.tab_menu_style['select']),
            ], style=self.tab_menu_style['height']),
            html.Div(id='information')
        ])

        return content

    def get_simulation_info(self, agents, running, date=None):
        content = [html.P(f'simulation is running: {running}'),
                   html.Br(),
                   html.P(f'{len(agents)} Agents are running')]

        if not running:
            content.append(
                html.Form(children=[
                    html.Div(children=[
                        html.Label('Starting Date:', htmlFor='start_date'),
                        dcc.Input(type="date", id="start_date", name="start_date", value="1995-01-01"),
                        html.Label('Ending Date:', htmlFor='end_date'),
                        dcc.Input(type="date", id="end_date", name="end_date", value="1995-02-01")
                    ]),
                    dcc.Input(type="submit", value="run simulation", id='run_simulation',
                              style={'margin-top': '10px'})
                ], method='POST', action='/start')
            )
        else:
            content.append(html.B(f'current date: {date}'))
            content.append(
                html.Form(children=[
                    html.Div(children=[
                        dcc.Input(type="submit", value="stop simulation", id="stop_simulation")
                    ])
                ], method='POST', action='/stop')
            )
        return html.Div(children=content, style={'margin-top': '20px'})

    def get_agent_info(self, agents, agent_type, capacities=None, generation=None, demand=None):

        content = html.Div(children=[
            html.H6(f'{agent_type.upper()} Agents:'),
            dcc.Dropdown(id='agent_dropdown', options=[],),
            html.Div(id='plots')
        ])

        for agent in agents:
            if agent_type in agent:
                content.children[1].options.append({'label': agent, 'value': agent})

        return content

    def plot_data(self, generation):
        content = html.Div(children=[
            html.B('Generation:'),
            html.Br(),
            dcc.Graph(figure=px.line(generation))
        ], style={'margin-top': '30px'})

        return content