import plotly.express as px
import json


def plot_energy_balance(start, end, typ, i_con):

    with open(r'./data/germany.geojson') as file:
        areas = json.load(file)

    for feature in areas['features']:
        feature.update({'id': feature['properties']['plz']})

    df = i_con.get_typ_generation(start, end, typ)

    colors = { 'PWP': "greys",
               'RES': "greens",
               'DEM': "reds",
               'STR': "jet"}

    fig = px.choropleth_mapbox(df, geojson=areas, color='power', locations='plz',
                               color_continuous_scale=colors[typ],
                               range_color=(df['power'].min(), df['power'].max()),
                               mapbox_style="carto-positron",
                               zoom=5, center = {"lat": 51.3, "lon": 10.2},
                               opacity=0.5,
                               labels={'power': 'E [GWh]'}
                               )
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})

    return fig


def plot_energy_range(start, end, typ, i_con):

    with open(r'./data/germany.geojson') as file:
        areas = json.load(file)

    for feature in areas['features']:
        feature.update({'id': feature['properties']['plz']})

    df = i_con.get_range_generation(start, end, typ)

    fig = px.choropleth_mapbox(df, geojson=areas, color='power', locations='plz', animation_frame="date",
                               color_continuous_scale='greys',
                               range_color=(df['power'].min(), df['power'].max()),
                               mapbox_style="carto-positron",
                               zoom=4.5, center = {"lat": 51.3, "lon": 10.2},
                               opacity=0.5,
                               labels={'power': 'E [GWh]'})

    return fig

if __name__ == "__main__":
    pass
