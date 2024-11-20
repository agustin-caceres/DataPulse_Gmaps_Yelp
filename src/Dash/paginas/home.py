from dash import dcc, html
import dash_bootstrap_components as dbc

# Layout de la página Home
layout = html.Div([
    html.H1("Bienvenido al Recomendador de Restaurantes", className="app-title"),

    # Presentación
    dbc.Container([
        html.P("Esta aplicación te ayudará a encontrar los mejores restaurantes según tus preferencias. ¡Comienza ahora!"),
        dbc.Row([
            dbc.Col(
                dbc.Card(
                    dbc.CardBody([
                        html.H5("Filtra por Distancia", className="card-title"),
                        html.P("Selecciona la distancia en kilómetros para encontrar opciones cercanas.")
                    ]),
                    className="mb-3"
                ),
                width=4
            ),
            dbc.Col(
                dbc.Card(
                    dbc.CardBody([
                        html.H5("Personaliza tu Búsqueda", className="card-title"),
                        html.P("Usa filtros como estado, ciudad, características y categorías.")
                    ]),
                    className="mb-3"
                ),
                width=4
            ),
            dbc.Col(
                dbc.Card(
                    dbc.CardBody([
                        html.H5("Obtén Recomendaciones", className="card-title"),
                        html.P("Recibe recomendaciones personalizadas con horarios y más.")
                    ]),
                    className="mb-3"
                ),
                width=4
            )
        ]),
        dbc.Row([
            dbc.Col(
                dbc.Button(
                    "Comenzar",
                    href="/page-1",  # Asegúrate de que esta ruta coincida con la página siguiente
                    color="primary",
                    className="mt-3"
                ),
                width=12,
                style={"textAlign": "center"}
            )
        ])
    ], className="home-container mt-4")
])
