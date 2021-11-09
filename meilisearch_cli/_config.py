from rich.console import Console
from rich.theme import Theme

URL_HELP_MESSAGE = "The url to the MeiliSearch instance"
MASTER_KEY_HELP_MESSAGE = "The master key for the MeiliSearch instance"
WAIT_MESSAGE = "If this flag is set the function will wait for MeiliSearch to finish processing the data and return the results. Otherwise the update ID will be returned immediately"
PANEL_BORDER_COLOR = "sky_blue2"
SECONDARY_BORDER_COLOR = "dodger_blue1"
RAW_MESSAGE = "If this flag is set the raw JSON will be displayed instead of the formatted output"

custom_theme = Theme(
    {
        "error": "red",
        "error_highlight": "yellow bold",
    }
)
console = Console(theme=custom_theme)
