import os

from poe_api_wrapper.service import start_server


def main():
    host = os.environ.get("POE_GATEWAY_HOST", "127.0.0.1")
    port = os.environ.get("POE_GATEWAY_PORT", "8000")
    start_server(address=host, port=port)


if __name__ == "__main__":
    main()
