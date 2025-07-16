# %%

screenCood = '''
User clicked the checkbox!
hcaptcha.html:14 Viewport coordinates (clientX, clientY): (29, 41)
hcaptcha.html:14 Screen coordinates (screenX, screenY): (605, 405)
hcaptcha.html:14 Coordinates relative to element (offsetX, offsetY): (13, 18)
hcaptcha.html:14 Checkbox checked state: undefined
'''

import subprocess
from flask import Flask, request
from bs4 import BeautifulSoup
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

@app.get("/")
def clickCaptcha():
    subprocess.run(["xdotool", "mousemove", "605", "405", "click", "1"]) # ajustar coordenadas cada vez
    print("clicked")
    return "Clicked!"

@app.post("/")
def saveEnterpriseOpening():
    """
    Receives a string via a POST request and saves it to a plain text file.
    The string is expected to be in the request body.
    """
    try:
        print(request)

        data = request.get_json()
        data = dict(data)
        if not data:
            return "Error: No JSON data received in the request body.", 400

        # Validate that 'id' and 'string_text' keys exist in the received JSON
        if 'id' not in data or 'htmlContent' not in data:
            return "Error: Missing 'id' or 'string_text' in JSON payload.", 400

        # Extract the id and string_text
        record_id = data['id']
        string_text = data['htmlContent']
        #get the enterprise opening
        opening_date = extract_opening_date(string_text)

        # Define the filename for the document
        # You can make this dynamic if needed, e.g., using a timestamp

        filename = "enterprise_opening_data.txt"

        print(opening_date)

        # Save the string to the file
        with open(filename, "a") as f: # Use "a" for append mode to add to existing file
            f.write(f"{{id:{record_id},opening_date:{string_text}}}" + "\n") # Add a newline for each entry

        return f"Successfully saved data to {filename}!", 200

    except Exception as e:
        # Basic error handling
        return f"An error occurred: {str(e)}", 500


def extract_opening_date(html_content):
    """
    Extrai a data de abertura da empresa de um conteúdo HTML.

    Args:
        html_content (str): O conteúdo HTML contendo as informações.

    Returns:
        str: A data de abertura no formato DD/MM/AAAA, ou None se não encontrada.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Encontra a tabela que contém a "DATA DE ABERTURA"
    # A estrutura HTML sugere que a data está em uma <td> dentro de uma <table>
    # com um texto específico antes do valor.
    
    date_label_tag = soup.find('font', string='DATA DE ABERTURA')
    
    if date_label_tag:
        # O valor da data está no próximo <b> após o <br> que segue a tag <font>
        # Navega para o elemento pai (<td>) e depois encontra o <b> com a data.
        parent_td = date_label_tag.find_parent('td')
        if parent_td:
            date_tag = parent_td.find('b')
            if date_tag:
                return date_tag.get_text(strip=True)

    inscricao_label_tag = soup.find('font', string='NÚMERO DE INSCRIÇÃO')
    
    if inscricao_label_tag:
        # O valor da data está no próximo <b> após o <br> que segue a tag <font>
        # Navega para o elemento pai (<td>) e depois encontra o <b> com a data.
        parent_td = inscricao_label_tag.find_parent('td')
        if parent_td:
            date_tag = parent_td.find('b')
            if date_tag:
                return date_tag.get_text(strip=True)
    return None
