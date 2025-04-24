# %%
from analise_inicial.analise_inicial_CEIS import initial_exploration as CEIS_exploration
'''

Exploração inicial

'''
from analise_inicial.analise_inicial_CEIS import initial_exploration as IE_CEIS
from analise_inicial.analise_inicial_CNEP import initial_exploration as IE_CNEP

IE_CEIS() # Exploração inicial dos dados da base CEIS
IE_CNEP() # Exploração inicialn dos dados da base CNEP
