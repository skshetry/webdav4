"""Configuration file for the Sphinx documentation builder."""

# pylint: disable=invalid-name

# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
import sys
from pathlib import Path

path = Path(__file__)
root = path.parents[1]
module_dir = (root / "src").absolute()
sys.path.insert(0, str(module_dir))

project = "webdav4"
copyright = "2021, Saugat Pachhai"  # pylint: disable=redefined-builtin
author = "Saugat Pachhai"

# -- General configuration ---------------------------------------------------

extensions = [
    "myst_parser",
    "sphinx.ext.intersphinx",
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.todo",
    "sphinx.ext.viewcode",
    "sphinx.ext.autosectionlabel",
    # external
    "sphinx_copybutton",
]

templates_path = ["_templates"]
todo_include_todos = True

# -- Options for HTML output -------------------------------------------------

html_theme = "furo"
intersphinx_mapping = {
    "python": ("https://docs.python.org/", None),
    "fsspec": ("https://filesystem-spec.readthedocs.io/en/stable", None),
}

html_title = "webdav4"
html_static_path = ["_static"]

copybutton_prompt_text = (
    r">>> |\.\.\. |\$ |In \[\d*\]: | {2,5}\.\.\.: | {5,8}: "
)
copybutton_prompt_is_regexp = True
