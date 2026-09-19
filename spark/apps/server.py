import http.server
import subprocess
import json
import sys

class SparkJobHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'status': 'healthy'}).encode('utf-8'))
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        if self.path == '/batch':
            print('[SparkJobHandler] Received request to trigger batch_pipeline.py...', flush=True)
            res = subprocess.run(
                ['python3', '/app/apps/batch_pipeline.py'],
                capture_output=True,
                text=True
            )
            status_code = 200 if res.returncode == 0 else 500
            self.send_response(status_code)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            response_payload = {
                'returncode': res.returncode,
                'stdout': res.stdout,
                'stderr': res.stderr
            }
            self.wfile.write(json.dumps(response_payload).encode('utf-8'))
            print(f'[SparkJobHandler] Completed batch execution with returncode={res.returncode}', flush=True)
        else:
            self.send_response(404)
            self.end_headers()

def run_server(port=8088):
    server_address = ('0.0.0.0', port)
    httpd = http.server.HTTPServer(server_address, SparkJobHandler)
    print(f'Spark Job Server listening on port {port}...', flush=True)
    httpd.serve_forever()

if __name__ == '__main__':
    run_server()
