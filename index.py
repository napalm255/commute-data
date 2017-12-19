#!/usr/bin/env python
"""Commute Traffic Data."""

from plotly import __version__
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
from plotly.graph_objs import Scatter, Figure, Layout
import pymysql



def handler(event, context):
    """Lambda handler."""
    # pylint: disable=unused-argument

    plot([Scatter(x=[1, 2, 3], y=[3, 1, 6])], auto_open=False)

    with open('temp-plot.html') as html_file:
        html_contents = html_file.read()

    return {'statusCode': 200,
            'body': html_contents,
            'headers': {'Content-Type': 'text/html'}}


if __name__ == '__main__':
    print(handler({}, None))
