"""WSGI entrypoint: `waitress-serve --call wsgi:get_application` from examples/."""
from runtime import create_http_app

def get_application():
    return create_http_app()
