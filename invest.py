import pandas as pd
import numpy as np
import time
import requests
#from fake_useragent import UserAgent
import psycopg2
from sqlalchemy import create_engine
import sqlalchemy
import datetime

url = 'http://www.fundamentus.com.br/detalhes.php?papel='

#ua = UserAgent(verify_ssl=False)

#header = {'User-Agent':str(ua.chrome)}#{"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
  #"X-Requested-With": "XMLHttpRequest"}

header = {
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36"
}

titulo = True



while True:


    consolidado_acoes_diario = pd.read_excel('Consolidado Ibovespa.xlsx')

    acoes_ibov = pd.read_excel('IBOV2.xlsx')

    consolidado_acoes = pd.DataFrame()

    for codigo_acao in acoes_ibov["Código"][:2]:
        print('Acessando informacoes da açao:', codigo_acao)
        
        r = requests.get(url+codigo_acao, headers= header)#, headers = header)
        print(r.text)
        

        acao = pd.read_html(r.text, decimal=',', thousands='.')

         

        acao[0] = acao[0].transpose()
        acao[1] = acao[1].transpose()
        acao[2] = acao[2].transpose()
        acao[3] = acao[3].transpose()
        acao[4] = acao[4].transpose()

   
        acao[0].iloc[2,4] = '?Vol $ méd 2m'
        acao[2].iloc[2,11] = '?Cres. Rec 5a'
        informacoes_1 = acao[0].iloc[:2, :]
        informacoes_2 = acao[0].iloc[2:, :]

        informacoes_3 = acao[1].iloc[:2, :]
        informacoes_4 = acao[1].iloc[2:, :]

        informacoes_2_1 = acao[2].iloc[:2,1:11]
        informacoes_2_2 = acao[2].iloc[2:4,1:12]
        informacoes_2_3 = acao[2].iloc[4:,1:12]




        ativo = acao[3].iloc[:2, 1]

        if acao[3].iloc[0, 2] == '?Disponibilidades':
            disponibilidades = acao[3].iloc[:2, 2]
        else:
            disponibilidades = pd.DataFrame(data={'oi': ['?Disponibilidades', 0]})
            disponibilidades = disponibilidades.fillna(0)
            disponibilidades = disponibilidades.iloc[0:2,0]

        if acao[3].iloc[0, 2] == '?Cart. de Crédito':
            cart_credito = acao[3].iloc[:2, 2]
        else:
            cart_credito = pd.DataFrame(data={'oi': ['?Cart. de Crédito',0]})
            cart_credito = cart_credito.fillna(0)
            cart_credito = cart_credito.iloc[0:2,0]
        try:    
            if acao[3].iloc[0, 3] == '?Ativo Circulante':
                ativo_circulante = acao[3].iloc[:2, 3]
        except Exception:   
                ativo_circulante = pd.DataFrame(data={'oi': ['?Ativo Circulante',0]})
                ativo_circulante = ativo_circulante.fillna(0)
                ativo_circulante = ativo_circulante.iloc[0:2,0]

        if acao[3].iloc[2, 1] == '?Dív. Bruta':
            div_bruta = acao[3].iloc[2:4, 1]
        else:
            div_bruta = pd.DataFrame(data={'oi': ['?Dív. Bruta', 0]})
            div_bruta = div_bruta.fillna(0)
            div_bruta = div_bruta.iloc[0:2,0]

        if acao[3].iloc[2, 2] == '?Dív. Líquida':
            div_liquida = acao[3].iloc[2:4, 2]
        else:
            div_liquida = pd.DataFrame(data={'oi': ['?Dív. Líquida', 0]})
            div_liquida = div_liquida.fillna(0)
            div_liquida = div_liquida.iloc[0:2,0]

        try:    
            if acao[3].iloc[2, 3] == '?Patrim. Líq':
                pl = acao[3].iloc[2:4, 3]
        except Exception:
                if acao[3].iloc[2, 2] == '?Patrim. Líq':
                    pl = acao[3].iloc[2:4, 2]
                else:
                    pl = pd.DataFrame(data={'oi': ['?Patrim. Líq',0]})
                    pl = pl.fillna(0)
                    pl = pl.iloc[0:2,0]

        if acao[3].iloc[2, 1] == '?Depósitos':
            depositos = acao[3].iloc[2:4, 1]
        else:
            depositos = pd.DataFrame(data={'oi': ['?Depósitos', 0]})
            depositos = depositos.fillna(0)
            depositos = depositos.iloc[0:2,0]    

        if acao[4].iloc[0, 2] == '?Result Int Financ':
            rf12 = acao[4].iloc[:2, 2]
            rl12 = pd.DataFrame(data={'oi': ['?Receita Líquida 12 meses', 0]})
            rl12 = rl12.fillna(0)
            rl12 = rl12.iloc[0:2,0]    
        elif acao[4].iloc[0, 2] == '?Receita Líquida':
            rl12 = acao[4].iloc[:2, 2]
            rf12 = pd.DataFrame(data={'oi': ['?Result Int Financ 12 meses', 0]})
            rf12 = rf12.fillna(0)
            rf12 = rf12.iloc[0:2,0]    

        
            
        if acao[4].iloc[0, 3] == '?Rec Serviços':
            rec12 = acao[4].iloc[:2, 3]
            ebit12 = pd.DataFrame(data={'oi': ['?EBIT 12 meses', 0]})
            ebit12 = ebit12.fillna(0)
            ebit12 = ebit12.iloc[0:2,0]    
        
        elif acao[4].iloc[2, 3] == '?EBIT':
            ebit12 = acao[4].iloc[:2, 3]
            rec12 = pd.DataFrame(data={'oi': ['?Rec Serviços 12 meses', 0]})
            rec12 = rec12.fillna(0)
            rec12 = rec12.iloc[0:2,0]
    
        
        if acao[4].iloc[0, 4] == '?Lucro Líquido':
            ll12 = acao[4].iloc[:2, 4]
        else:
            ll12 = pd.DataFrame(data={'oi': ['?Lucro Líquido 12 meses', 0]})
            ll12 = ll12.fillna(0)
            ll12 = ll12.iloc[0:2,0]
        
            
        if acao[4].iloc[0, 2] == '?Result Int Financ':
            rf3 = acao[4].iloc[2:, 2]
            rl3 = pd.DataFrame(data={'oi': ['?Receita Líquida 3 meses', 0]})
            rl3 = rl3.fillna(0)
            rl3 = rl3.iloc[0:2,0]    
        elif acao[4].iloc[0, 2] == '?Receita Líquida':
            rl3 = acao[4].iloc[2:, 2]
            rf3 = pd.DataFrame(data={'oi': ['?Result Int Financ 3 meses', 0]})
            rf3 = rf3.fillna(0)
            rf3 = rf3.iloc[0:2,0]    

        
            
        if acao[4].iloc[0, 3] == '?Rec Serviços':
            rec3 = acao[4].iloc[2:, 3]
            ebit3 = pd.DataFrame(data={'oi': ['?EBIT 3 meses', 0]})
            ebit3 = ebit3.fillna(0)
            ebit3 = ebit3.iloc[0:2,0]    
        
        elif acao[4].iloc[2, 3] == '?EBIT':
            ebit3 = acao[4].iloc[2:, 3]
            rec3 = pd.DataFrame(data={'oi': ['?Rec Serviços 3 meses', 0]})
            rec3 = rec3.fillna(0)
            rec3 = rec3.iloc[0:2,0]
   
        
        if acao[4].iloc[2, 4] == '?Lucro Líquido':
            ll3 = acao[4].iloc[2:, 4]
        else:
            ll3 = pd.DataFrame(data={'oi': ['?Lucro Líquido 3 meses', 0]})
            ll3 = ll3.fillna(0)
            ll3 = ll3.iloc[0:2,0]    
        
        
        
        rf12.iloc[0]= '?Result Int Financ 12 meses'
        rl12.iloc[0] = '?Receita Líquida 12 meses'
        rec12.iloc[0] = '?Rec Serviços 12 meses'
        ebit12.iloc[0] = '?EBIT 12 meses'
        ll12.iloc[0] = '?Lucro Líquido 12 meses'
        
        rf3.iloc[0] = '?Result Int Financ 3 meses'
        rl3.iloc[0] = '?Receita Líquida 3 meses'
        rec3.iloc[0] = '?Rec Serviços 3 meses'
        ebit3.iloc[0] = '?EBIT 3 meses'
        ll3.iloc[0] = '?Lucro Líquido 3 meses'
#



        informacoes_2 = informacoes_2.reset_index(drop=True)
        informacoes_4 = informacoes_4.reset_index(drop=True)
        informacoes_2_2 = informacoes_2_2.reset_index(drop=True)
        informacoes_2_3 = informacoes_2_3.reset_index(drop=True)
        div_bruta = div_bruta.reset_index(drop=True)
        div_liquida = div_liquida.reset_index(drop=True)
        depositos = depositos.reset_index(drop=True)
        pl = pl.reset_index(drop=True)
        rf12 = rf12.reset_index(drop=True)
        rl12 = rl12.reset_index(drop=True)
        rec12 = rec12.reset_index(drop=True)
        ebit12 = ebit12.reset_index(drop=True)
        ll12 = ll12.reset_index(drop=True)
        rf3 = rf3.reset_index(drop=True)
        rl3 = rl3.reset_index(drop=True)
        rec3 = rec3.reset_index(drop=True)
        ebit3 = ebit3.reset_index(drop=True)
        ll3 = ll3.reset_index(drop=True)

        

        acao = pd.concat([informacoes_1, informacoes_2, informacoes_3, informacoes_4, informacoes_2_1, informacoes_2_2, informacoes_2_3, ativo, ativo_circulante, disponibilidades, cart_credito, div_bruta, div_liquida, depositos, pl, rf12, rf3, rl12, rl3, rec12, rec3, ebit12, ebit3, ll12, ll3], axis = 1, join = 'inner')
        
        acao.columns = acao.iloc[0]
        acao = acao.drop(0)
        
        
        consolidado_acoes = consolidado_acoes.append(acao, sort=False)
        consolidado_acoes = consolidado_acoes.replace('-', '0')
        
    consolidado_acoes = consolidado_acoes.reset_index(drop=True)

    novo_cabecalho = [coluna.replace('?', '') for coluna in consolidado_acoes.columns]

    consolidado_acoes.columns = novo_cabecalho
    
    #coreçao datas
    consolidado_acoes['Data últ cot'] = pd.to_datetime(consolidado_acoes['Data últ cot'], errors='coerce', format='%d/%m/%Y')
    consolidado_acoes['Últ balanço processado'] = pd.to_datetime(consolidado_acoes['Últ balanço processado'], errors='coerce', format='%d/%m/%Y')
    #coreçao numeros
    consolidado_acoes['Vol $ méd 2m'] = pd.to_numeric(consolidado_acoes['Vol $ méd 2m'], errors='coerce')
    
    consolidado_acoes['Valor de mercado'] = pd.to_numeric(consolidado_acoes['Valor de mercado'], errors='coerce')
    consolidado_acoes['Valor da firma'] = pd.to_numeric(consolidado_acoes['Valor da firma'], errors='coerce')
    consolidado_acoes['Nro. Ações'] = pd.to_numeric(consolidado_acoes['Nro. Ações'], errors='coerce')
        #vol_med = consolidado_acoes['Vol $ méd (2m)'].mean()
    consolidado_acoes_diario = consolidado_acoes_diario.append(consolidado_acoes, sort=False)

    consolidado_acoes_diario.to_excel('Consolidado Ibovespa.xlsx', index=False)

    engine = create_engine('postgresql+psycopg2://yvnujpqhlgajpd:22a197cb5edf921fd317aa732fb59e5e29c48459f9e7be3d99400f08e0f4238d@ec2-35-175-155-248.compute-1.amazonaws.com:5432/dbf3b683gg5rhs')

    conn = engine.connect()

    consolidado_acoes_diario.to_sql(name = 'invest', con = conn, if_exists = 'append', index = False)

    conn.close()

    print(datetime.datetime.now())



    
    
    titulo = False
    time.sleep(207*207)
    