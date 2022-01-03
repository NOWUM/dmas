from datetime import date
from dash import dcc
from dash import html
from dash_daq import PowerButton, GraduatedBar
import dash_bootstrap_components as dbc


class Page:

    def __init__(self):
        self.container = dbc.Container(children=[])
