import pandas as pd
import plotly.express as px
from pathlib import Path



def bar_sorted(df, x, y, title):
    fig = px.bar(df.sort_values(by=x, ascending=False), x=x, y=y, title=title)
    fig.update_layout(
        font_size=14,
        title_font_size=18,
        plot_bgcolor='white',
    )
    fig.update_xaxes(
        title_text=x,
        showgrid=True,
    )
    fig.update_yaxes(
        title_text=y,
        showgrid=True,
    )
    return fig



def time_line(df, x, y, title):
    fig = px.line(df,x=x,y=y,title=title)
    fig.update_layout(
        font_size=14,
        title_font_size=18,
        plot_bgcolor='white',
    )
    fig.update_xaxes(
        title_text=x,
        showgrid=False,
    )
    fig.update_yaxes(
        title_text=y,
        showgrid=False,
    )
    return fig




def histogram_chart(df, x, nbins, title):
    fig = px.histogram(df, x=x, nbins=nbins, title=title)
    fig.update_layout(
        font_size=14,
        title_font_size=18,
        plot_bgcolor='white',
    )
    fig.update_xaxes(
        title_text=x,
        showgrid=True,
    )
    return fig


def save_fig(fig, path, scale=2):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.write_image(str(path), scale=scale)