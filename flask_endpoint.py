from flask import Flask, request, jsonify, send_from_directory
import subprocess
import threading
import uuid
import os
from werkzeug.utils import secure_filename
from threading import Lock
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

app = Flask(__name__)

API_KEY = os.getenv('API_KEY')  # Fetch the API key from environment variables

def check_api_key(func):
    """Decorator to check the API key in request headers."""
    def wrapper(*args, **kwargs):
        api_key = request.headers.get('API-Key')
        if api_key != API_KEY:
            return jsonify({'error': 'Unauthorized'}), 401
        return func(*args, **kwargs)
    wrapper.__name__ = func.__name__
    return wrapper

tasks = {}
uploads_dir = "pdf_uploads"
tasks_lock = Lock()

@app.route('/convert', methods=['POST'])
@check_api_key
def convert_pdf_to_md():
    """Endpoint to convert PDF to Markdown."""
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400
    if not file or not file.filename.endswith('.pdf'):
        return jsonify({'error': 'Invalid file type'}), 400

    os.makedirs(uploads_dir, exist_ok=True)
    task_id = str(uuid.uuid4())
    input_path = os.path.join(uploads_dir, secure_filename(file.filename))
    output_path = os.path.join(uploads_dir, secure_filename(file.filename.rsplit('.', 1)[0] + '.md'))

    file.save(input_path)
    with tasks_lock:
        tasks[task_id] = {'status': 'Pending', 'output_path': output_path}

    thread = threading.Thread(target=run_conversion, args=(task_id, input_path, output_path))
    thread.start()
    return jsonify({'status': 'Pending', 'message': 'Conversion started', 'task_id': task_id}), 202

@app.route('/status/<task_id>', methods=['GET'])
@check_api_key
def get_status(task_id):
    """Endpoint to check the status of a conversion task."""
    task_info = tasks.get(task_id)
    if task_info is None:
        return jsonify({'error': 'Task ID not found'}), 404
    return jsonify({'task_id': task_id, 'status': task_info['status']}), 200

@app.route('/download/<task_id>', methods=['GET'])
@check_api_key
def download_file(task_id):
    """Endpoint to download the converted file."""
    task_info = tasks.get(task_id)
    if task_info is None:
        return jsonify({'error': 'Task ID not found'}), 404
    if task_info['status'] == 'Completed':
        return send_from_directory(uploads_dir, os.path.basename(task_info['output_path']), as_attachment=True)
    return jsonify({'error': 'File not ready for download'}), 400

def run_conversion(task_id, input_path, output_path):
    """Thread function to run the conversion process."""
    tasks[task_id]['status'] = 'In Progress'
    command = ['poetry', 'run', 'python', 'convert_single.py', input_path, output_path, "--parallel_factor", "4"]
    print(str(command))
    try:
        subprocess.run(command, check=True)
        logging.info(f"Conversion completed for {input_path}")
        tasks[task_id]['status'] = 'Completed'
    except subprocess.CalledProcessError as e:
        tasks[task_id]['status'] = 'Failed'
        logging.error(f"An error occurred during conversion: {e}")

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5001, debug=True, threaded=True)
