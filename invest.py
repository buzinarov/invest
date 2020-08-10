import pandas as pd
import numpy as np
import time
import requests
from selenium import webdriver
#from fake_useragent import UserAgent
import psycopg2
from sqlalchemy import create_engine
import sqlalchemy
import datetime
import os
from selenium.webdriver.chrome.options import Options


chrome_options = webdriver.ChromeOptions()

chrome_options.binary_location = os.environ.get("GOOGLE_CHROME_BIN")
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--no-sandbox")

driver = webdriver.Chrome(executable_path=os.environ.get("CHROMEDRIVER_PATH"), chrome_options=chrome_options)



#proxyDict = {
#    "http"  : os.environ.get('FIXIE_URL', ''),
#    "https" : os.environ.get('FIXIE_URL', '')
#}

#ua = UserAgent(verify_ssl=False, use_cache_server=False)

#header = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36'}

header = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36"}
  #      "referer" : "https://googleads.g.doubleclick.net/pagead/ads?client=ca-pub-3119085269630402&output=html&h=60&slotname=4422770468&adk=2541374544&adf=1327086493&w=468&lmt=1596597228&psa=1&guci=2.2.0.0.2.2.0.0&format=468x60&url=http%3A%2F%2Fwww.fundamentus.com.br%2Findex.php&flash=0&wgl=1&adsid=ChEI8I6k-QUQgZe-rN7o3uC7ARJMAMGjPQwo_H7OXJPtCOA20LPv68oYt6JnRDqba7h5ZiZ0RpqSdt8XPPEDHJ9012Sj_OAHU3azvx85l9O_Hu3x_bJwVTJ7FWlmKCX1Nw&dt=1596597228773&bpp=4&bdt=319&idt=196&shv=r20200803&cbv=r20190131&ptt=9&saldr=aa&abxe=1&cookie=ID%3D4e61b5c385e9103d%3AT%3D1594378670%3AS%3DALNI_MY1uarlDN0xldSekncmjp8nTJaGBw&prev_fmts=468x60&correlator=3390765410852&frm=20&pv=1&ga_vid=1344508319.1594378670&ga_sid=1596597229&ga_hid=380651181&ga_fc=1&iag=0&icsg=142671528&dssz=22&mdo=0&mso=0&u_tz=-180&u_his=11&u_java=0&u_h=1080&u_w=1920&u_ah=1040&u_aw=1920&u_cd=24&u_nplug=3&u_nmime=4&adx=15&ady=412&biw=795&bih=952&scr_x=0&scr_y=0&eid=42530557%2C42530559%2C42530587%2C21066920&oid=3&pvsid=1013780939292229&pem=408&ref=http%3A%2F%2Fwww.fundamentus.com.br%2Fdetalhes.php%3Fpapel%3DVALE3&rx=0&eae=0&fc=896&brdim=0%2C0%2C0%2C0%2C1920%2C0%2C1920%2C1040%2C812%2C969&vis=1&rsz=%7C%7CeE%7C&abl=CS&pfx=0&fu=9216&bc=23&jar=2020-8-5-3&ifi=2&uci=a!2&fsb=1&xpc=dngyDrkgEQ&p=http%3A//www.fundamentus.com.br&dtd=204"
#}
#{'User-Agent':str(ua.chrome)}#
titulo = True



while True:

    #session = requests.session()
    url = 'http://www.fundamentus.com.br/detalhes.php?papel='
    consolidado_acoes_diario = pd.read_excel('Consolidado Ibovespa.xlsx')

    acoes_ibov = pd.read_excel('IBOV2.xlsx')

    consolidado_acoes = pd.DataFrame()

    for codigo_acao in acoes_ibov["Código"]:
        print('Acessando informacoes da açao:', codigo_acao)
        
        #driver.get = ('http://www.fundamentus.com.br/detalhes.php?papel='+codigo_acao)

        driver.get(url+codigo_acao)#, headers = header)#, headers = header)
        
        r = driver.page_source

        acao = pd.read_html(r)#, decimal=',', thousands='.')

        

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
    driver.close()
    print(datetime.datetime.now())



    
    
    titulo = False
    time.sleep(207*207)
    