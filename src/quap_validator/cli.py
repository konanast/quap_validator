"""Console script for quap_validator."""
import quap_validator

import typer
from rich.console import Console

app = typer.Typer()
console = Console()


@app.command()
def main():
    """Console script for quap_validator."""
    console.print("To be updated ... "
               "quap_validator.cli.main")
    console.print("See documentation at https://quap-validator.readthedocs.io.")



if __name__ == "__main__":
    app()
