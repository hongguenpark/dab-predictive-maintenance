"""DAB 기능별 배포 테스트 결과 보고서 - Static HTML 서빙"""

from http.server import HTTPServer, SimpleHTTPRequestHandler
import os

PORT = 8000


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=os.path.dirname(__file__), **kwargs)


if __name__ == "__main__":
    server = HTTPServer(("0.0.0.0", PORT), Handler)
    print(f"Serving on port {PORT}")
    server.serve_forever()
