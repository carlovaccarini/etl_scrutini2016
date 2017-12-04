
# coding: utf-8

"""
Created on Fri Dec  1 23:40:10 2017

@author: cvaccarini
"""

import pandas as pd
import sys, requests
import numpy as np

decimali_in_perc=4
filename = sys.argv[1]
#filename="ScrutiniFI.csv"
dataset = pd.read_csv(filename, delimiter=";", header=0)

#strip delle DESC
for i in ["DESCREGIONE", "DESCPROVINCIA","DESCCOMUNE"]:
    dataset[i] = dataset[i].apply(lambda x: x.strip())

#eliminazione della SICLIA
try:
    url = 'http://ckan.ancitel.it/api/action/datastore_search?resource_id=c381efe6-f73f-4e20-a825-547241eeb457'
    x=requests.get(url).json()
    regioni = list(set([x["result"]["records"][i]["Regione"].upper() for i in range(0,len(x["result"]["records"]))]))
    regioni.append("VALLE D'AOSTA") #nel dataset usato mancava la valle d'aosta
    dataset_regioni=[i for i in dataset["DESCREGIONE"].drop_duplicates()]
    regioni_in_piu=[i for i in dataset_regioni if not i in regioni]
    regioni_in_meno=[i for i in regioni if not i in dataset_regioni]
    ind=dataset[dataset["DESCREGIONE"].isin(regioni_in_piu)]["DESCREGIONE"].index
    for i in ind:
        dataset.iloc[i,0]="SICILIA"
except IndexError:
    pass

#raccolta dello sporco (dei valori sporchi)
sporco=list()
[[sporco.append(j) for j in dataset[i] if type(j) == float or not j.isdigit()] for i in ["VOTANTI", "ELETTORI_M", "ELETTORI"]]

#cerco gli indici ai quali trovo gli elementi sporchi --> dataset[dataset[i].isin(sporco)]
indici_sporco=list()
for i in ["VOTANTI", "ELETTORI_M", "ELETTORI"]:
    [indici_sporco.append(x) for x in list(dataset[dataset[i].isin(sporco)].index)]
indici_sporco=list(set(indici_sporco))

new_dataset = dataset.copy()

#cerco gli indici ai quali trovo gli elementi nulli --> dataset[dataset[i].isnull()]
indici_null=list()
for i in ["VOTANTI", "ELETTORI_M", "ELETTORI"]:
    [indici_null.append(x) for x in list(new_dataset[new_dataset[i].isnull()].index)]
indici_null=list(set(indici_null))


#elimino gli sporchi i cui indici sono in indici
print("eliminati gli sporchi:\t" + str(set(indici_sporco)))
for i in set(indici_sporco):
    new_dataset = new_dataset.drop([i])

#bisogna isolare i record stringa poi possiamo passare tutto a float
def convert_series(x):
    try:
        if(x!=np.NaN and x.isdigit()):
            return int(x)
        else:
            return x
    except AttributeError:
            print(x)
            
for i in ["VOTANTI", "ELETTORI_M", "ELETTORI"]:
    new_dataset[i]=new_dataset[i].apply(lambda x: convert_series(x))

for i in ["DESCREGIONE", "DESCPROVINCIA"]:
    new_dataset[i] = new_dataset[i].apply(lambda x: x.strip())

#droppo le righe con valori nulli (così come ho fatto per quelle con valori sporchi)
new_dataset=new_dataset.dropna()


x=len(new_dataset[new_dataset["VOTANTI"]==new_dataset["NUMVOTISI"]+new_dataset["NUMVOTINO"]+new_dataset["NUMVOTIBIANCHI"]+new_dataset["NUMVOTICONTESTATI"]+new_dataset["NUMVOTINONVALIDI"]])
print("non verificano la regola di validazione:\t" + str(x) + " record")

new_dataset[new_dataset["VOTANTI"]!=new_dataset["NUMVOTISI"]+new_dataset["NUMVOTINO"]+new_dataset["NUMVOTIBIANCHI"]+new_dataset["NUMVOTICONTESTATI"]+new_dataset["NUMVOTINONVALIDI"]]
#guarda caso è il nostro broglio elettorale di prima...

indici_broglio = list(new_dataset[new_dataset["VOTANTI"]!=new_dataset["NUMVOTISI"]+new_dataset["NUMVOTINO"]+new_dataset["NUMVOTIBIANCHI"]+new_dataset["NUMVOTICONTESTATI"]+new_dataset["NUMVOTINONVALIDI"]].index)

#elimino i brogli
for i in indici_broglio:
    new_dataset=new_dataset.drop([i])


#cerco gli indici ai quali trovo gli zeri che ragionevolmente sono insensati
indici_zero=list()
for i in ["VOTANTI", "VOTANTI_M", "NUMVOTISI", "NUMVOTINO"]:
    [indici_zero.append(x) for x in list(new_dataset[new_dataset[i]==0].index)]
indici_zero=list(set(indici_zero))
print("eliminate le righe:\t" + str(set(indici_zero)) + " per valori con 0")
for i in set(indici_zero):
    new_dataset = new_dataset.drop([i])

#Provo a ricostruire i dati droppati per avere un altro branch di analisi</h1>
#FASE DI RECUPERO
rec_dataset = dataset.copy()

indici_da_ripulire=list(set(indici_sporco+indici_zero))

#si lavorerà su questo subset
sub=rec_dataset.iloc[indici_da_ripulire, :]

#preparo l'analisi per provincia
group_provincia = new_dataset.groupby("DESCPROVINCIA").sum()
group_provincia["%_VOTANTI"]=group_provincia["VOTANTI"]/group_provincia["ELETTORI"]
group_provincia["%_VOTANTI_M"]=group_provincia["VOTANTI_M"]/group_provincia["VOTANTI"]
group_provincia["%_ELETTORI_M"]=group_provincia["ELETTORI_M"]/group_provincia["ELETTORI"]

perc_ricostruzione=((len(dataset)-len(new_dataset)) /len(dataset))
print(str( round(perc_ricostruzione , decimali_in_perc)) + "% del dataset iniziale da recuperare")

def rec_value_from_DESCPROVINCIA(provincia, colonna, dato_di_recupero):
    x=group_provincia.loc[[provincia],:]
    if colonna=="ELETTORI":
        #dato_di_recupero è il numero di votanti
        rec_value=int(dato_di_recupero/x["%_VOTANTI"])
    elif colonna=="ELETTORI_M":
        #dato_di_recupero è il numero di elettori
        rec_value=int(dato_di_recupero*x["%_ELETTORI_M"])
    elif colonna=="VOTANTI":
        #dato_di_recupero è il numero di elettori
        rec_value=int(dato_di_recupero*x["%_VOTANTI"])
    elif colonna=="VOTANTI_M":
        #dato_di_recupero è il numero di votanti
        rec_value=int(dato_di_recupero*x["%_VOTANTI_M"])
    else:
        rec_value=0
    return rec_value


#serve per pulire i campi sporchi togliendo i char
def whole_list_to_int(lista):
    if(len([i for i in lista if type(i)==int or type(i)==float])!=len(lista)):
        #nella lista ho degli item che sono stringa
        new_lista=list()
        #potrei usare le list comprhension ma poi diveterebbe poco comprehension...
        for i in lista:
            if (type(i)==str):
                x=i
                for j in i:
                    #print(ord(j))
                    if (ord(j)<48 or ord(j)>57):
                        x=x.replace(j,"")
                if(len(x)>0):
                    i=int(x)
                else:
                    i=int(0)
            new_lista.append(i)
        return new_lista

#questa funzione isola i casi in cui si può e non si può ricostruire il dato e lo ricostruisce oppure salta la riga
def remake(r, prov):
    new_r=r
    #adesso ho tutti digits
    for i in [2,3,4,5,0,1,6,7,8]:#range(0, len(old_r)):
        flag=True
        if(i in range(2,6)):
            #voti, voti_m, votisi, votino
            print("VOTI " + str(r[i]))
            #se qui ho tutti 0 allora non posso ricalcolare il dato
            if(r[i]==0):
                if (r[2]+r[3]+r[4]+r[5]==0):
                    print("non posso recuperare il dato, conviene SALTARE il record")
                    """
                    #pensandoci bene riavere solo i votanti non è una buona soluzione, meglio droppare
                    if(r[0]>0):
                        print("\t però ho gli elettori e posso stimare i votanti con i risultati in provincia")
                    else:
                    """
                    flag=False
                    break
                elif(i==2):
                    if(sum([r[k] for k in range(4,9)])>0):
                        #qui assumo che le invalidazioni ci siano (se ci sono i risultati ci dovrebbero essere anche loro)
                        print("posso ricostruire i voti utilizzando i risultati")
                        new_r[2]=sum([r[k] for k in range(4,9)])
                        flag=True
                    #else:
                        #ricostruire con i dati medi di altri comuni
                elif(i==3 and r[2]>0):
                    new_r[i]=rec_value_from_DESCPROVINCIA(prov, "VOTANTI_M", r[0])
        elif(i in range(0,2)):
            #elettori, elettori_m
            print("ELETTORATO " + str(r[i]))
            if(r[i]==0):
                if(sum([r[k] for k in range(0,4)])==0):
                    print("non posso fare nulla: non ho elettori, elettori_m e votanti")
                else:
                    print("posso recuperare dall'andamento in provincia")
                    if(i==0 and r[2]>0):
                        #rec gli ELETTORI dalla % dei votanti della prov
                        new_r[i]=rec_value_from_DESCPROVINCIA(prov, "ELETTORI", r[2])
                    elif(i==1 and r[0]>0):
                        new_r[i]=rec_value_from_DESCPROVINCIA(prov, "ELETTORI_M", r[0])
                    else:
                        print("\t non posso recuperare perchè non ho i ")
                    
        else:
            #voti bianchi, nonvalidi, contestati
            print("INVALIDAZIONI DI VOTO " + str(r[i]))
    
    #ritorno la stringa vuota se nella fase di recupero non sono riuscito a recuperare sensatamente tutto il dato
    if(flag):
        return new_r
    else:
        return []


only_epurated=False #variabile che serve per far partire la scrittura del nuovo file con i valori epurati

if round(perc_ricostruzione,decimali_in_perc)>=0.0001:
    print("RECUPERO IL DATO")
    sub=sub.fillna(0) #so che il NaN è diverso da 0, ma qui i campi sono 
    df_ricalcolato=pd.DataFrame(columns=sub.columns)
    for old_r in sub.values:
        print(old_r[3:])
        r=whole_list_to_int(old_r[3:])
        x=remake(r, old_r[1])
        if (len(x)>0):
            print("recupero riuscito...\n" + str(list(old_r[0:3])+x))
            df_row=pd.DataFrame([list(old_r[0:3])+x], columns=sub.columns)
            df_ricalcolato=df_ricalcolato.append(df_row, ignore_index=True)
    if len(df_ricalcolato)==0:
        print("recupero non riuscito...")
        only_epurated=True
else:
    print("Percentuale da recuperare inferiore al 0.0001%")
    only_epurated=True

#raggruppo i dati, calcolo, formatto l'output, 
def write_csv(df, epurated=True):
    group_regione_rec = df.groupby("DESCREGIONE").sum()
    group_regione_rec["REGIONE"]=group_regione_rec.index
    group_regione_rec["REGIONE"]=group_regione_rec["REGIONE"].apply(lambda x: x.strip())
    group_regione_rec["ELETTORI_F"]=group_regione_rec["ELETTORI"]-group_regione_rec["ELETTORI_M"]
    group_regione_rec["%_VOTANTI"]=round(group_regione_rec["VOTANTI"]/group_regione_rec["ELETTORI"],decimali_in_perc)
    group_regione_rec["%_VOTI_SI"]=round(group_regione_rec["NUMVOTISI"]/group_regione_rec["VOTANTI"],decimali_in_perc)
    group_regione_rec["%_VOTI_NO"]=round(group_regione_rec["NUMVOTINO"]/group_regione_rec["VOTANTI"],decimali_in_perc)
    group_regione_rec["%_SCHEDE_BIANCHE"]=round(group_regione_rec["NUMVOTIBIANCHI"]/group_regione_rec["VOTANTI"],decimali_in_perc)
    group_regione_rec["%_SCHEDE_NON_VALIDE"]=round(group_regione_rec["NUMVOTINONVALIDI"]/group_regione_rec["VOTANTI"],decimali_in_perc)
    group_regione_rec["%_SCHEDE_CONTESTATE"]=round(group_regione_rec["NUMVOTICONTESTATI"]/group_regione_rec["VOTANTI"],decimali_in_perc)
    output=group_regione_rec.loc[:,["REGIONE", "ELETTORI_M", "ELETTORI_F", "ELETTORI", "%_VOTANTI", "%_VOTI_SI", "%_VOTI_NO", "%_SCHEDE BIANCHE", "%_SCHEDE_NON_VALIDE", "%_SCHEDE_CONTESTATE"]]
    if epurated:
        output_file= filename.split(".csv")[0]  + "-aggregated.csv"
    else:
        output_file= "withRecovery_" +filename.split(".csv")[0]  + "-aggregated.csv"
    output.to_csv(path_or_buf=output_file , index=False, header=["Regione", "Elettori Maschi", "Elettori Femmine", "Elettori Totali", "Percentuali votanti", "Percentuali voti si", "Percentuali voti no", "Percentuale schede bianche", "Percentuale schede non valide", "Percentuale schede contestate"])
    return output



write_csv(new_dataset)
print("Scritto il file " + filename.split(".csv")[0]  + "-aggregated.csv")
if not only_epurated:
    #metto apposto i tipi del ricalcolato, saranno sicuramente tutti int
    for i in ["ELETTORI","ELETTORI_M","VOTANTI","VOTANTI_M","NUMVOTISI","NUMVOTINO","NUMVOTIBIANCHI","NUMVOTINONVALIDI","NUMVOTICONTESTATI"]:
        df_ricalcolato[i]=df_ricalcolato[i].apply(lambda x: int(x))
    print("\nè stato scritto anche il file " + "withRecovery_" +filename.split(".csv")[0]  + "-aggregated.csv con i valori recuperati" )
    write_csv(new_dataset.append(df_ricalcolato), epurated=only_epurated)
