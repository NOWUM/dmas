from dashboards.page import Page

from datetime import date
from dash import dcc
from dash import html
from dash_daq import PowerButton, GraduatedBar
import dash_bootstrap_components as dbc


class SimulationControl(Page):

    def __init__(self, status):
        super().__init__()
        self.container.children.append(
            html.Div([
                dbc.Row([
                    dbc.Col(
                        dcc.DatePickerRange(
                            id='date_range',
                            min_date_allowed=date(1995, 1, 1),
                            max_date_allowed=date(2030, 1, 1),
                            initial_visible_month=date(1995, 1, 1),
                            start_date=date(1995, 1, 1),
                            end_date=date(1995, 2, 1)),
                        width=9),
                    dbc.Col(
                        PowerButton(id='trigger_simulation', on=status),
                        width=3
                    )
                ]),
                dbc.Row([
                    dbc.Col(
                        GraduatedBar(id='loading_bar', value=0)
                    ),
                    dbc.Col(
                        html.Div(id='simulation_status')
                    )
                ])
            ], style={'margin-left': '10px', 'margin-top': '20px'})
        )
