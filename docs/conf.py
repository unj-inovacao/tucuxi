"""Sphinx configuration."""
from datetime import datetime


project = "Tucuxi"
author = "Luccas Quadros"
copyright = f"{datetime.now().year}, {author}"
extensions = ["sphinx.ext.autodoc", "sphinx.ext.napoleon"]
autodoc_typehints = "description"
