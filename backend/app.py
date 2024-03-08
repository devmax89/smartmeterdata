from flask import Flask, request, redirect, url_for, flash, render_template, send_file
import pandas as pd
from sqlalchemy import create_engine, inspect
import os
import tempfile
import atexit
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.sql import select
import io

app = Flask(__name__)
app.secret_key = 'smartmetering'  # Sostituisci con la tua chiave segreta

# Crea un motore SQLAlchemy per connettersi al database PostgreSQL
engine = create_engine('postgresql://admin:admin@172.17.0.2:5432/smartmetering')

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/read_excel')
def read_excel():
    return render_template('read_excel.html')

@app.route('/storico_dati')
def storico_dati():
    inspector = inspect(engine)
    table_names = inspector.get_table_names()
    return render_template('storico_dati.html', tables=table_names)

Session = sessionmaker(bind=engine)

@app.route('/template')
def template():
    inspector = inspect(engine)
    table_names = inspector.get_table_names()
    template_names = ['Produzione']  # Aggiungi qui i nomi dei tuoi template
    return render_template('template.html', tables=table_names, templates=template_names)

@app.route('/delete/<table_name>')
def delete_table(table_name):
    session = Session()
    try:
        session.execute(text(f'DROP TABLE IF EXISTS public."{table_name}"'))
        session.commit()
        flash('Table successfully deleted')
    except Exception as e:
        session.rollback()
        flash(str(e))
    finally:
        session.close()
    return redirect(url_for('storico_dati'))

@app.route('/download/<table_name>')
def download(table_name):
    df = pd.read_sql_table(table_name, engine)
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as temp:
        df.to_excel(temp.name, index=False)
        atexit.register(os.remove, temp.name)
        return send_file(temp.name, as_attachment=True, download_name=f'{table_name}.xlsx')

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        flash('No file part')
        return redirect(url_for('read_excel'))  
    file = request.files['file']
    if file.filename == '':
        flash('No selected file')
        return redirect(url_for('read_excel'))  
    if file:
        try:
            df = pd.read_excel(file, engine='openpyxl')
        except ValueError as e:
            if "Value must be either numerical or a string containing a wildcard" in str(e):
                flash('Rimuovere tutti i filtri applicati e poi effettuare il caricamento')
                return redirect(url_for('read_excel'))  
        # Pulisci il nome del file per utilizzarlo come nome della tabella
        table_name = file.filename.rsplit('.', 1)[0].replace(' ', '_')
        # Scrivi il DataFrame nel database PostgreSQL con il nome della tabella
        df.to_sql(table_name, engine, if_exists='replace')
        flash('File successfully uploaded')
        return redirect(url_for('read_excel'))

import tempfile

@app.route('/query/<table_name>/<template_name>')
def query(table_name, template_name):
    queries = {
        'Produzione': "SELECT * FROM {} WHERE \"TIPO LINEA\" = 'Produzione' AND \"Avviso\" IS NOT null AND \"Avviso\" != ''"
    }

    query_template = queries.get(template_name)
    if query_template is None:
        return 'Template non trovato', 404

    if not table_name.isidentifier():
        return 'Nome della tabella non valido', 400

    session = Session()
    try:
        result = session.execute(text(query_template.format(table_name)))
        data = [row._mapping for row in result]
        
        df = pd.DataFrame(data)
        with tempfile.NamedTemporaryFile(suffix='.xlsx') as temp:
            df.to_excel(temp.name, engine='xlsxwriter')
            return send_file(temp.name, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', as_attachment=True)

    except Exception as e:
        session.rollback()
        return str(e)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')