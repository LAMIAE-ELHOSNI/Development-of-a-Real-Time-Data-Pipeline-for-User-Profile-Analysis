import time
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
from pymongo import MongoClient
import pandas as pd
import tldextract
import dash_bootstrap_components as dbc
from dash.exceptions import PreventUpdate

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["MyAppDatabase"]
collection = db["users"]

# Create a Dash app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Define a layout for the app
app.layout = html.Div(
    children=[
        dbc.Card(
            dbc.CardBody([
                html.H1("User Insights (Total Users: 0)", className="display-4", id="total-users"),
                html.P("Explore user data and insights.", className="lead"),
            ]),
            style={"margin-bottom": "20px"},
        ),
        dbc.Row(
            [
                dbc.Col(
                    dcc.Graph(
                        id='age-distribution',
                        figure=px.line(
                            pd.DataFrame({'Age': [], 'Count': []}),
                            x='Age',
                            y='Count',
                            title='Age Distribution'
                        ),
                    ),
                    width={"size": 6, "offset": 0},
                ),
                dbc.Col(
                    [
                        dbc.Row(
                            [
                                dcc.Graph(
                                    id='gender-distribution',
                                    figure=px.pie(
                                        names=[],
                                        values=[],
                                        title='Gender Distribution'
                                    ),
                                ),
                            ],
                            style={"margin-bottom": "20px"},
                        ),
                        dbc.Row(
                            [
                                dcc.Graph(
                                    id='most-common-email-domains',
                                    figure=px.pie(
                                        names=[],
                                        values=[],
                                        title='Most Common Email Domains'
                                    ),
                                ),
                            ],
                        ),
                    ],
                ),
            ],
            style={"margin-top": "20px"},
        ),
        dcc.Interval(
            id='interval-component',
            interval=10 * 1000,  # Update every 10 seconds (specified in milliseconds)
            n_intervals=0
        )
    ],
    style={"padding": "20px"},
)

@app.callback(
    [Output('age-distribution', 'figure'),
     Output('gender-distribution', 'figure'),
     Output('most-common-email-domains', 'figure'),
     Output('total-users', 'children')],
    Input('interval-component', 'n_intervals')
)
def update_data(n_intervals):
    # Fetch updated data from MongoDB
    data = list(collection.find())
    df = pd.DataFrame(data)

    # Recalculate the data for your charts
    gender_distribution = df.groupby('gender').size().reset_index(name='count')
    email_domains = [tldextract.extract(email).domain for email in df['email']]
    email_counter = pd.Series(email_domains).value_counts().head(5)
    total_users = len(df)

    # Update the figures for your charts
    age_distribution_fig = px.line(
        pd.DataFrame({'Age': df['age'].sort_values(), 'Count': range(1, len(df) + 1)}),
        x='Age',
        y='Count',
        title='Age Distribution'
    )
    gender_distribution_fig = px.pie(
        names=gender_distribution['gender'],
        values=gender_distribution['count'],
        title='Gender Distribution'
    )
    email_domains_fig = px.pie(
        names=email_counter.index,
        values=email_counter.values,
        title='Most Common Email Domains'
    )

    return age_distribution_fig, gender_distribution_fig, email_domains_fig, f"User Insights (Total Users: {total_users})"

if __name__ == '__main__':
    app.run_server(debug=True)
