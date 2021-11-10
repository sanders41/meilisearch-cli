from rich.console import Console
from rich.theme import Theme
from typer import Option

MASTER_KEY_OPTION = Option(
    None, envvar="MEILI_MASTER_KEY", help="The master key for the MeiliSearch instance"
)
PANEL_BORDER_COLOR = "sky_blue2"
RAW_OPTION = Option(
    False, help="If this flag is set the raw JSON will be displayed instead of the formatted output"
)
SECONDARY_BORDER_COLOR = "dodger_blue1"
URL_OPTION = Option(None, envvar="MEILI_HTTP_ADDR", help="The url to the MeiliSearch instance")
WAIT_OPTION = Option(
    False,
    "--wait",
    "-w",
    help="If this flag is set the function will wait for MeiliSearch to finish processing the data and return the results. Otherwise the update ID will be returned immediately",
)


custom_theme = Theme(
    {
        "error": "red",
        "error_highlight": "yellow bold",
    }
)
console = Console(theme=custom_theme)
