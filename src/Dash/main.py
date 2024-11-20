import dash
from dash import Input, Output, State, dcc, html
import dash_bootstrap_components as dbc
from functions.api_request import get_recommendations
from paginas.page_1 import layout  
from functions.config import ciudades_por_estado
import pandas as pd

# Inicializar la app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], suppress_callback_exceptions=True)
app.layout = layout

# Función para generar tarjetas
def generate_recommendation_card(recommendation):
    horarios = [
        html.Tr([html.Td("Lunes"), html.Td(recommendation["lunes"])]),
        html.Tr([html.Td("Martes"), html.Td(recommendation["martes"])]),
        html.Tr([html.Td("Miércoles"), html.Td(recommendation["miercoles"])]),
        html.Tr([html.Td("Jueves"), html.Td(recommendation["jueves"])]),
        html.Tr([html.Td("Viernes"), html.Td(recommendation["viernes"])]),
        html.Tr([html.Td("Sábado"), html.Td(recommendation["sabado"])]),
        html.Tr([html.Td("Domingo"), html.Td(recommendation["domingo"])]),
    ]

    # Generar enlace dinámico a Google Maps
    google_maps_url = f"https://www.google.com/maps/search/?api=1&query={recommendation['latitud']},{recommendation['longitud']}"

    return dbc.Card(
        dbc.CardBody([ 
            html.H4(recommendation["negocio"], className="card-title"),
            html.P(f"Dirección: {recommendation['direccion']}, {recommendation['ciudad']}, {recommendation['estado']}"),
            html.P(f"Distancia: {recommendation['distancia']} km"),
            html.Hr(),
            html.Table(horarios, className="table table-striped"),
            html.A(
                "Ver en Google Maps", 
                href=google_maps_url, 
                target="_blank",  # Abrir en nueva pestaña
                className="btn-google-maps"  # Estilo del botón
            )
        ]),
        className="mb-3"
    )

# Callback para actualizar ciudades según el estado seleccionado
@app.callback(
    Output("input-ciudad", "options"),
    Input("input-estado", "value")
)
def update_cities(estado):
    if not estado:
        return []
    if estado not in ciudades_por_estado:
        return [{"label": "No hay ciudades disponibles", "value": ""}]
    return [{"label": ciudad, "value": ciudad} for ciudad in ciudades_por_estado[estado]]

# Callback para obtener recomendaciones
@app.callback(
    Output("resultados", "children"),
    Input("btn-submit", "n_clicks"),
    State("input-km", "value"),
    State("input-estado", "value"),
    State("input-ciudad", "value"),
    State("input-usuario", "value"),
    State("input-caracteristicas-modal", "value"),
    State("input-categorias-modal", "value"),
    State("input-top-n", "value")
)
def update_recommendations(n_clicks, km, estado, ciudad, usuario, caracteristicas, categorias, top_n):
    if not n_clicks:
        return "Introduce los parámetros y haz clic en el botón para obtener sus recomendaciones."

    try:
        # Llamar al backend
        response = get_recommendations(km, estado, ciudad, usuario, caracteristicas or [], categorias or [])

        if not response or "recomendations" not in response:
            raise ValueError("No se encontraron recomendaciones para los filtros seleccionados. Por favor, prueba con otros filtros.")

        # Limitar el número de recomendaciones mostradas
        recomendaciones = response["recomendations"][:top_n]
        # Generar las tarjetas
        cards = [generate_recommendation_card(rec) for rec in recomendaciones]

        return dbc.Container(cards, className="results-container")
    except Exception as e:
        return dbc.Alert(
            f"Error al obtener recomendaciones: {str(e)}",
            color="danger",
            dismissable=True
        )

# Callback único para manejar secciones colapsables
@app.callback(
    [Output("collapse-usuario", "is_open"),
     Output("collapse-distancia", "is_open"),
     Output("collapse-preferencias", "is_open")],
    [Input("collapse-usuario-btn", "n_clicks"),
     Input("collapse-distancia-btn", "n_clicks"),
     Input("collapse-preferencias-btn", "n_clicks")],
    [State("collapse-usuario", "is_open"),
     State("collapse-distancia", "is_open"),
     State("collapse-preferencias", "is_open")]
)
def toggle_sections(usuario_click, distancia_click, preferencias_click, usuario_open, distancia_open, preferencias_open):
    ctx = dash.callback_context

    if not ctx.triggered:
        return usuario_open, distancia_open, preferencias_open

    button_id = ctx.triggered[0]["prop_id"].split(".")[0]

    if button_id == "collapse-usuario-btn":
        return not usuario_open, False, False
    elif button_id == "collapse-distancia-btn":
        return False, not distancia_open, False
    elif button_id == "collapse-preferencias-btn":
        return False, False, not preferencias_open

    return usuario_open, distancia_open, preferencias_open

# Callback único para manejar la apertura y cierre de modales
@app.callback(
    [Output("characteristics-modal", "is_open"),
     Output("categories-modal", "is_open")],
    [Input("open-characteristics-modal", "n_clicks"),
     Input("open-categories-modal", "n_clicks"),
     Input("close-characteristics-modal", "n_clicks"),
     Input("close-categories-modal", "n_clicks")],
    [State("characteristics-modal", "is_open"),
     State("categories-modal", "is_open")]
)
def toggle_modals(open_characteristics_click, open_categories_click, close_characteristics_click, close_categories_click,
                  characteristics_modal_open, categories_modal_open):
    ctx = dash.callback_context

    if ctx.triggered:
        button_id = ctx.triggered[0]["prop_id"].split(".")[0]

        if button_id == "open-characteristics-modal":
            return True, categories_modal_open
        elif button_id == "close-characteristics-modal":
            return False, categories_modal_open

        if button_id == "open-categories-modal":
            return characteristics_modal_open, True
        elif button_id == "close-categories-modal":
            return characteristics_modal_open, False

    return characteristics_modal_open, categories_modal_open

# Ejecutar la app
if __name__ == "__main__":
    app.run_server(debug=True)
