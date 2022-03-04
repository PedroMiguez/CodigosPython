import psycopg2
import pandas as pd
import os

conn = psycopg2.connect(
    host="localhost",
    database="Siagas",
    user="postgres",
    password="senha")

def Comando(x):
    try:
        global conn;
        cur = conn.cursor()
        cur.execute(x)
        cur.close()
        conn.commit()
        return("Sucesso")
    except (Exception, psycopg2.DatabaseError) as error:
        return(str(error))
 #   finally:
   #  if conn is not None:
   #     conn.close()       
    

def tabela(caminho):
    try:
        ce = pd.read_csv(caminho , header=None)
        return ce.values.tolist()
    except:
        return("Erro na inserção")

def Inserir(campos,valores):
    x= "INSERT INTO siagas_geral(" + campos + ") VALUES(" + valores+ ")"     
    return x

def codufloc(c):
    campos = "codigo"
    valores = c[0][0]
    campos += ",uf"
    valores += ",'"+ c[1][0] + "'"
    campos += ",mun"
    valores +=",'"+ c[2][0].replace("'","chr(39)")  + "'"
    if pd.isna(c[3][0]) != True:
        campos += ",localidade"
        valores += ",'"+ c[3][0].replace("'","chr(39)")  + "'"
    return [campos,valores]

def geral(c):
    campos=""
    valores=""
    if pd.isna(c[1][1]) != True:
        campos += ",nome"
        valores +=",'"+ c[1][1].replace("'","chr(39)")  + "'"
    if pd.isna(c[2][1]) != True:
        campos += ",instalacao"
        dt = c[2][1]
        ano= dt[6] + dt[7]+ dt[8]+dt[9]
        mm = dt[3]+ dt[4]
        dd =dt[0]+dt[1] 
        valores+= ",'"+ano+"-"+mm+"-"+dd + "'"
    if pd.isna(c[3][1]) != True:
        campos += ",proprietario"
        valores +=",'"+ c[3][1].replace("'","chr(39)") + "'"
    if pd.isna(c[4][1]) != True:
        campos += ",naturezaponto"
        valores +=",'"+ c[4][1].replace("'","chr(39)")  + "'"
    if pd.isna(c[5][1]) != True:
        campos += ",usoagua"
        valores +=",'"+ c[5][1].replace("'","chr(39)")  + "'"
    if pd.isna(c[6][1]) != True:
        campos += ",cota"
        valores +=","+ str(c[6][1])
    if pd.isna(c[9][1]) != True:
        campos += ",UTMS"
        valores += ","+str(c[9][1]) 
    if pd.isna(c[10][1]) != True:
        campos += ",UTMO"
        valores += ","+str(c[10][1])
    if pd.isna(c[11][1]) != True:
        campos += ",lat"
        valores += ","+str(c[11][1]) 
    if pd.isna(c[12][1]) != True:
        campos += ",long"
        valores +=","+ str(c[12][1]) 
    if pd.isna(c[13][1]) != True:
        campos += ",bacia"
        valores +=",'"+ c[13][1].replace("'","chr(39)")  + "'"
    if pd.isna(c[14][1]) != True:
        campos += ",subbacia"
        valores +=",'"+ c[14][1].replace("'","chr(39)")  + "'"
    return [campos,valores]
        

def Lergeral(caminho):
    g=tabela(caminho + "/codufloc.csv")
    if g != 'erro':
        g1=codufloc(g)
        g=tabela(caminho + "/geral.csv")
        if g != 'erro':
            g2=geral(g)
            campos=g1[0]+g2[0]
            valores=g1[1]+g2[1]
            return Inserir(campos,valores)
        else:
            return 'erro'
    else:
        return 'erro' 
    
    
pastas = os.listdir('D:\Base\Banco de Dados\PR')
erros=""
for pocos in pastas:
    x = 'D:/Base/Banco de Dados/PR/' + pocos
    y =Lergeral(x)
    z= Comando(y)
    print(z)
    if z !="Sucesso":
        cur = conn.cursor()
        cur.execute("ROLLBACK")
        erros+="|"+z
        
Fechar()



