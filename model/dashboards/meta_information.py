from dashboards.page import Page

from datetime import date
from dash import dcc
from dash import html
import plotly.express as px


class MetaInformation(Page):

    def __init__(self, capacities):
        super().__init__()

        capacities = px.bar(capacities)

        self.container.children.append(
            html.Div([
                dcc.Graph(id='installed capacities', figure=capacities)
            ])
        )
