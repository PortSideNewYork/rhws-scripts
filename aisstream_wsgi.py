#!python

from aisstream_service import create_app

application = create_app()

if __name__ == "__main__":
    # Errors with multiple instances created if debug=True or use_reloader=True
    application.run(debug=False, use_reloader=False)
