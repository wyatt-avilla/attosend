"""Local-only VM fixture. Never sends email."""
import http.server
import json
import pathlib

state = pathlib.Path('/var/lib/mock-resend')


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(503 if (state / 'unready').exists() else 200)
        self.end_headers()

    def do_POST(self):
        payload = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        if (state / 'reject').exists():
            self.send_response(403)
            self.end_headers()
            return
        assert self.headers['Authorization'] == 'Bearer test-only-key'
        with (state / 'messages').open('a') as output:
            output.write(json.dumps({'key': self.headers['Idempotency-Key'], 'payload': payload}) + '\n')
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'{"id":"mock-id"}')


http.server.ThreadingHTTPServer(('127.0.0.1', 9088), Handler).serve_forever()
