'''
A ideia é fazer download de publicações de diários oficiais, e minerar os pregões e licitações em ocorreram as fraudes/atos ilícitos
'''

# %%
import os
import time
import os
import requests


class DataAnotation:
    def __init__(self) -> None:

        pass
    

    def downloadDiarioOficial(self, ano, mes, dia, pagina, num_processo):

        base_url = fr"https://pesquisa.in.gov.br/imprensa/servlet/INPDFViewer?jornal=530&pagina={pagina}&data={dia}/{mes}/{ano}&captchafield=firstAccess"
        response = requests.get(base_url, stream=True)

        with open(os.path.join(os.getcwd(), 'diario_oficial', f"processo_{num_processo}.pdf"), "wb") as f:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)


# testando classe


D = DataAnotation()

D.downloadDiarioOficial("2022", "07", "28", "12", 2)
        


# %%
driver = D.driver

driver.find_element(by=By.TAG_NAME, value="body").get_attribute("innerHTML")

# %%
# %%

url = "https://pesquisa.in.gov.br/imprensa/servlet/INPDFViewer?jornal=530&pagina=12&data=28/07/2022&captchafield=firstAccess"
