from flask import Flask, send_file
from .proofpoint_parser import ProofPointParser
import io
app = Flask(__name__)

@app.route('/')
def return_csv():
    file = open('sample_file.csv', 'r')
    parser = ProofPointParser(file)
    output_file = parser.get_csv()
    mem = io.BytesIO()
    mem.write(output_file.getvalue().encode('utf-8'))
    mem.seek(0)
    return send_file(mem, as_attachment=True, attachment_filename='output.csv', mimetype='text/csv')
