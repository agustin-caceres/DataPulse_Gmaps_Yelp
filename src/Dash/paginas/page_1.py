from dash import dcc, html
import dash_bootstrap_components as dbc
from functions.config import categorias, caracteristicas, kms, estados, ciudades_por_estado, usuarios

# Layout con barra lateral
layout = html.Div([
    # Contenedor general
    html.Div([
        # Barra lateral
        html.Div([
            html.H4("Filtros", className="sidebar-title"),

            # Usuario
            dbc.Button(
                "Usuario", id="collapse-usuario-btn", className="filter-btn mb-2"
            ),
            dbc.Collapse([
                html.Label("Selecciona el usuario:", className="label"),
                dcc.Dropdown(
                    id="input-usuario",
                    options=[{"label": usuario, "value": usuario} for usuario in usuarios],
                    placeholder="Selecciona el usuario",
                    className="dropdown"
                )
            ], id="collapse-usuario", is_open=False),

            # Distancia y Ubicación
            dbc.Button(
                "Distancia y Ubicación", id="collapse-distancia-btn", className="filter-btn mb-2"
            ),
            dbc.Collapse([
                html.Label("Distancia (km):", className="label"),
                dcc.Dropdown(
                    id="input-km",
                    options=[{"label": f"{km} km", "value": km} for km in kms],
                    placeholder="Selecciona la distancia",
                    className="dropdown"
                ),
                html.Label("Estado:", className="label"),
                dcc.Dropdown(
                    id="input-estado",
                    options=[{"label": estado, "value": estado} for estado in estados],
                    placeholder="Selecciona el estado",
                    className="dropdown"
                ),
                html.Label("Ciudad:", className="label"),
                dcc.Dropdown(
                    id="input-ciudad",
                    placeholder="Selecciona la ciudad",
                    className="dropdown"
                )
            ], id="collapse-distancia", is_open=False),

            # Preferencias
            dbc.Button(
                "Preferencias", id="collapse-preferencias-btn", className="filter-btn mb-2"
            ),
            dbc.Collapse([
                html.Label("Características:", className="label"),
                dbc.Button(
                    "Selecciona características", 
                    id="open-characteristics-modal", 
                    color="info", 
                    className="dropdown"
                ),

                html.Label("Categorías:", className="label"),
                dbc.Button(
                    "Selecciona categorías", 
                    id="open-categories-modal", 
                    color="info", 
                    className="dropdown"
                ),

                html.Label("Top N recomendaciones:", className="label"),
                dcc.Dropdown(
                    id="input-top-n",
                    options=[{"label": str(n), "value": n} for n in range(1, 6)],
                    value=5,
                    placeholder="Selecciona el número de recomendaciones",
                    className="dropdown"
                )
            ], id="collapse-preferencias", is_open=False),

            # Botón para enviar
            dbc.Button(
                "Obtener Recomendaciones", id="btn-submit", color="success", className="submit-btn mt-3"
            ),

            # Logo al final de la barra lateral
            html.Div(
                className="image-container"  # estilo css
            )

        ], className="sidebar"),

        # Contenido principal
        html.Div([
            html.H4("", className="results-title"),
            html.Div(id="resultados", className="results-container")
        ], className="main-content")
    ], className="layout-container"),

    # Modal para seleccionar características
    dbc.Modal(
        [
            dbc.ModalHeader("Selecciona las características"),
            dbc.ModalBody(
                dcc.Checklist(
                    id="input-caracteristicas-modal",
                    options=[{"label": carac, "value": carac} for carac in caracteristicas],
                ),
            ),
            dbc.ModalFooter(
                dbc.Button("Cerrar", id="close-characteristics-modal", className="filter-btn ml-auto")
            ),
        ],
        id="characteristics-modal",
        is_open=False,
    ),

    # Modal para seleccionar categorías
    dbc.Modal(
        [
            dbc.ModalHeader("Selecciona las categorías"),
            dbc.ModalBody(
                dcc.Checklist(
                    id="input-categorias-modal",
                    options=[{"label": cat, "value": cat} for cat in categorias],
                ),
            ),
            dbc.ModalFooter(
                dbc.Button("Cerrar", id="close-categories-modal", className="filter-btn ml-auto")
            ),
        ],
        id="categories-modal",
        is_open=False,
    ),
])
