#este código acessa o site do SIAGAS e faz o download das informações 

import requests
from bs4 import BeautifulSoup
import csv
import os
import pandas as p
from threading import Thread

sucesso = 0
erro= 0
erroc= 0

from fake_useragent import UserAgent
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
ua=UserAgent()
hdr = {'User-Agent': ua.random,
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
      'Accept-Encoding': 'none',
      'Accept-Language': 'en-US,en;q=0.8',
      'Connection': 'keep-alive'}

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}


def pt(x,z):
    global erro
    global fim
    global sucesso
    global erroc
    
    if x != "erro":
        pr=tratar(x)
    else:
        erro +=1
    pr = 0
    if pr==0:
        st="Erro"
    else:
        st="Sucesso"
    os.system('clear')
    print("Poço: " + str(z) + " | Coleta: " + st + " |Falta: " + str(fim-z) + " | Erros: "+ str(erro)+ " | Sucessos: " + str(sucesso) + " | Erro Construtivo:" + str(erroc))




def baixarhtml(cod):
    site='http://siagasweb.cprm.gov.br/layout/detalhe.php?ponto='+ str(cod)
    # pega o poço 
    page = requests.get(site,params=None, headers=hdr, cookies=None, auth=None, timeout=None)
    if page.status_code == 200:
        return page.text
    else:
        return "erro"


def criarlista(lista):
    data=[]
    rows = lista.find_all('tr')
    for row in rows:
        cols = row.find_all('td')
        cols = [ele.text.strip() for ele in cols]
        data.append([ele for ele in cols if ele])
    return data

def sdt(lista,caminho):
    dt=p.DataFrame(lista)
    dt.to_csv(caminho, sep=',',index=False, header = False)
    
def Sgeral(ta,caminho):
    caminho1=caminho + "/geral.csv"
    data=criarlista(ta[1])
    sdt(data,caminho1)

def UFeS(ger,loc,caminho):
    t=ger.find_all(string="Situação:")
    situ=t[1].next_element.next_element.next_element.next_element.next_element.replace("\xa0", "")
    dt=ger.find(string="Data:")
    data=dt.next_element.next_element.next_element.next_element.next_element.replace("\xa0", "")
    vet =loc + [data,situ]
    sdt(vet,caminho+"/UFeS.csv")
    
def SCon(ta,caminho):
    if len(ta) > 8:
        data=criarlista(ta[1])
        sdt(data,caminho+'/con_perfuracao.csv')
        data=criarlista(ta[2])
        sdt(data,caminho+'/con_diametro.csv')
        data=criarlista(ta[3])
        sdt(data,caminho+'/con_revestimento.csv')
        data=criarlista(ta[4])
        sdt(data,caminho+'/con_filtro.csv')
        data=criarlista(ta[5])
        sdt(data,caminho+'/con_anular.csv')
        data=criarlista(ta[6])
        sdt(data,caminho+'/con_boca.csv')
        data=criarlista(ta[7])
        sdt(data,caminho+'/com_entrada.csv')
        data=criarlista(ta[8])
        sdt(data,caminho+'/con_profundidade.csv')
    else:
        global erroc 
        erroc +=1


def Sgeo(ta,caminho):
    data=criarlista(ta[1])
    sdt(data,caminho+'/geo_feicao.csv')
    data=criarlista(ta[2])
    sdt(data,caminho+'/geo_formacao.csv')
    data=criarlista(ta[3])
    sdt(data,caminho+'/geo_litologia.csv')

def Shid(ta,caminho):
    aq=criarlista(ta[1])
    aq=['Aquífero:',aq[0][0].replace('Aquífero:','')]
    data=criarlista(ta[2])
    data.append(aq)
    sdt(data,caminho+'/hid_aquifero.csv')
    data=criarlista(ta[3])
    sdt(data,caminho+'/hid_nivel.csv')

def Sbomb(ta,caminho):
    data=criarlista(ta[1])
    sdt(data,caminho+'/bombeamento.csv')

def Squimica(ta,caminho):
    caminho1=caminho + "/qui_Fixo.csv"
    caminho2=caminho + "/qui_Analise.csv"
    data=criarlista(ta[1])
    sdt(data,caminho1)
    data2=criarlista(ta[2])
    sdt(data2,caminho2)
    
    
def Salvar(loc):
    if not os.path.exists(loc):
       os.makedirs(loc)

def tratar(t):
    soup = BeautifulSoup(t, 'html.parser')
    if  soup.find('tr') != None:
        ponto_list = soup.find('tr')
        ponto_list_items = ponto_list.find_all('b')
        cod = ''
        uf =''
        mun = ''
        loc = ''
        for ponto in ponto_list_items:
            #Limpa o texto
            dados = ponto.contents[0]
            #codigo do poço
            if dados[4] == ":":
                for x in range(5, len(dados)):
                    if dados[x] != ' ' :
                        cod = cod + dados[x]
            #Estado
            if len(dados) > 5:
                if dados[2] == ":":
                    for x in range(3, len(dados)):
                        if dados[x] != ' ':
                            uf = uf + dados[x]
            #Municipio
            if len(dados) > 9:                
                if dados[9] == ":":
                    for x in range(10, len(dados)):
                        if dados[x] != ' ' or x > 12:
                            mun = mun + dados[x]
            #local
            if len(dados) > 10:
                if dados[10] == ":":
                    for x in range(11, len(dados)):
                        if dados[x] != ' ' or x > 14:
                            loc = loc + dados[x]  
        if uf != '' :            
            caminho="/Banco de Dados/"+uf+ "/" + cod
            Salvar(caminho)
            p=["dados",cod,uf,mun,loc]
            
            ger= soup.find(id="tabs-1")
            geral= ger.find_all("table")
            construtivo= soup.find(id="tabs-2").find_all("table")
            geologico=soup.find(id="tabs-3").find_all("table")
            hidrogeologico=soup.find(id="tabs-4").find_all("table")
            bombeamento = soup.find(id="tabs-5").find_all("table")
            quimica = soup.find(id="tabs-6").find_all("table")
            
            t0=Thread(target=UFeS,args=[ger,p,caminho])
            t0.start()
            t1=Thread(target=Sgeral,args=[geral,caminho])
            t1.start()
            t2=Thread(target=SCon,args=[construtivo,caminho])
            t2.start()
            t3=Thread(target=Sgeo,args=[geologico,caminho])
            t3.start()
            t4=Thread(target=Shid,args=[hidrogeologico,caminho])
            t4.start()
            t5=Thread(target=Sbomb,args=[bombeamento,caminho])
            t5.start()
            t6=Thread(target=Squimica,args=[quimica,caminho])
            t6.start()
            global sucesso
            sucesso +=1
            return 1
        else:
            global erro
            erro += 1
            return 0

inicio = 35
fim = 35
pocos = fim- inicio
falta = 0

for z in range (inicio , fim):
    x=baixarhtml(z)
    x=Thread(target=pt,args=[x,z])
    x.start()


