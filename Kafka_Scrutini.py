# -*- coding: utf-8 -*-
"""
Created on Tue Nov 28 23:23:03 2017

@author: carlov
"""
import pandas as pd
from confluent_kafka import Producer, Consumer, KafkaError, TopicPartition, OFFSET_BEGINNING
import requests, sys, csv


filename = sys.argv[1]
#filename="ScrutiniFI.csv"
delimiter=","
decimali_in_perc=4
dataset = pd.read_csv(filename, delimiter=";", header=0)
#fare la ragionata sui nomi di comuni regioni e province
for i in ["DESCPROVINCIA", "DESCREGIONE","DESCCOMUNE"]:
    dataset[i]=dataset[i].apply(lambda x: x.strip())
setup_kafka = dataset.loc[:,["DESCPROVINCIA", "DESCREGIONE"]].drop_duplicates()
list_topic = dataset["DESCREGIONE"].unique()
setup_kafka.groupby("DESCREGIONE").count()

#il famoso pezzo della SICLIA
#questa volta vado a chiamare una libreria esterna e incrocio le liste per evidenziare mancanti o superflui
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

p = Producer({'bootstrap.servers': 'localhost:9092' })


"""
TOPIC--->REGIONE
PARTITION--->PROVINCIA (siccome è un int allora prendo l'indice della lista)
msg.value--->record
"""
#dove starà tutta la struttra delle partiotions
unique_reg_prov=dataset.loc[:,["DESCREGIONE", "DESCPROVINCIA"]].drop_duplicates()
def set_partitions_struct(df_reg_prov):
    x=dict()
    for i in df_reg_prov["DESCREGIONE"].drop_duplicates():
        x[i]=list()
    for i in df_reg_prov["DESCPROVINCIA"]:
        x[df_reg_prov[df_reg_prov["DESCPROVINCIA"]==i]["DESCREGIONE"].iloc[0]].append(i)
    return x

partitions_struct=set_partitions_struct(unique_reg_prov)

def get_partition(topic, prov):
    return partitions_struct[topic].index(prov)

#producer che spara su i record con encode utf-8 (già attivo)
for ind in list(dataset.index):
    topic=dataset.iloc[ind, 0]
    prov=dataset.iloc[ind, 1]
    msg=""
    for i in dataset.iloc[ind,:]:
        msg=msg+str(i)+delimiter
    msg=msg[:(len(msg)-1)]+"\n"
    print(topic + "\t" + prov + "\t" + msg)
    print("-"*10)
    p.produce(topic.replace(" ", "_").replace("'", ""), msg.encode('utf-8'), partition=get_partition(topic, prov))
    p.flush()

#imposto i consumer
def set_TopicPartitions_assignments(struttura):
    x=dict()
    for i in struttura.keys():
            regione_pulita=i.replace(" ","_").replace("'", "")
            x[regione_pulita]=[TopicPartition(regione_pulita, j, OFFSET_BEGINNING) for j in range(0,len(struttura[i]))]
    return x

consumer_assignments=set_TopicPartitions_assignments(partitions_struct)

#v=dict.fromkeys([ x.replace(" ","_").replace("'", "") for x in consumer_assignments.keys()]
#serve la dict comprehension perchè con fromkeys si andava a puntare su tutti gli elementi!
kafka_out_struct={"ELETTORI": 0, "ELETTORI_M":0, "ELETTORI_F":0, "VOTANTI":0, "VOTANTI_M":0, "NUMVOTISI":0,"NUMVOTINO":0, "NUMVOTIBIANCHI":0, "NUMVOTINONVALIDI":0, "NUMVOTICONTESTATI":0}
kafka_out={key: dict(kafka_out_struct) for key in consumer_assignments.keys()}

def write_on_kafka_out(msg):
    try:
        flag=True
        buffer=msg.value().decode("utf-8").split(delimiter)[3:]
        [int(i) for i in buffer]
    except (ValueError,TypeError):
        flag=False
    finally:
        if flag:
            #scrivo sul kafka_topic
            print("---->" +msg.topic())
            kafka_out[msg.topic()]["ELETTORI"]+=int(buffer[0])
            kafka_out[msg.topic()]["ELETTORI_M"]+=int(buffer[1])
            kafka_out[msg.topic()]["ELETTORI_F"]+=int(buffer[0])-int(buffer[1])
            kafka_out[msg.topic()]["VOTANTI"]+=int(buffer[2])
            kafka_out[msg.topic()]["VOTANTI_M"]+=int(buffer[3])
            kafka_out[msg.topic()]["NUMVOTISI"]+=int(buffer[4])
            kafka_out[msg.topic()]["NUMVOTINO"]+=int(buffer[5])
            kafka_out[msg.topic()]["NUMVOTIBIANCHI"]+=int(buffer[6])
            kafka_out[msg.topic()]["NUMVOTINONVALIDI"]+=int(buffer[7])
            kafka_out[msg.topic()]["NUMVOTICONTESTATI"]+=int(buffer[8])

#consumer --> poll from beginning 
def run_consumer(tp):
    c = Consumer({'bootstrap.servers': 'localhost:9092', 'group.id': 'CarloGroup',
                  'default.topic.config': {'auto.offset.reset': 'smallest'}})
    c.assign([tp])
    run=True
    while run:
        msg=c.poll(timeout=1000)
        if not msg.value().decode("utf-8"):
            print("EOF " + msg.topic() + " " + str(msg.partition()))
            run=False
        elif msg.error():
            print("ERRORE:" + msg.error())        
            run=False
        print(msg.value().decode("utf-8"))
        try:        
            write_on_kafka_out(msg)
        except IndexError:
            print("IndexError")
            pass
    c.close()

#DESCREGIONE;DESCPROVINCIA;DESCCOMUNE;ELETTORI;ELETTORI_M;VOTANTI;VOTANTI_M;NUMVOTISI;NUMVOTINO;NUMVOTIBIANCHI;NUMVOTINONVALIDI;NUMVOTICONTESTATI
#attivo i consumer uno alla volta su tutte le partition necessarie
for regione in consumer_assignments.keys():
    for tp in consumer_assignments[regione]:
        print(tp)
        run_consumer(tp)


#facciamo i calcoli finali e formattiamo l'output
output=dict()
for r in kafka_out.keys():
    output[r]=dict()
    output[r]["REGIONE"]=r
    output[r]["ELETTORI"]=kafka_out[r]["ELETTORI"]
    output[r]["ELETTORI_M"]=kafka_out[r]["ELETTORI_M"]
    output[r]["ELETTORI_F"]=kafka_out[r]["ELETTORI_F"]
    output[r]["%_VOTANTI"]=round(kafka_out[r]["VOTANTI"]/kafka_out[r]["ELETTORI"], decimali_in_perc)
    output[r]["%_VOTI_SI"]=round(kafka_out[r]["NUMVOTISI"]/kafka_out[r]["VOTANTI"], decimali_in_perc)
    output[r]["%_VOTI_NO"]=round(kafka_out[r]["NUMVOTINO"]/kafka_out[r]["VOTANTI"], decimali_in_perc)
    output[r]["%_SCHEDE_BIANCHE"]=round(kafka_out[r]["NUMVOTIBIANCHI"]/kafka_out[r]["VOTANTI"], decimali_in_perc)
    output[r]["%_SCHEDE_NON_VALIDE"]=round(kafka_out[r]["NUMVOTINONVALIDI"]/kafka_out[r]["VOTANTI"], decimali_in_perc)
    output[r]["%_SCHEDE_CONTESTATE"]=round(kafka_out[r]["NUMVOTICONTESTATI"]/kafka_out[r]["VOTANTI"], decimali_in_perc)
    
#scrivo il file
with open("kafka-"+ filename.split(".csv")[0]  + "-aggregated.csv", 'w', newline='') as csvfile:
    #fields = ["Regione", "Elettori Maschi", "Elettori Femmine", "Elettori Totali", "Percentuali votanti", "Percentuali voti si", "Percentuali voti no", "Percentuale schede bianche", "Percentuale schede non valide", "Percentuale schede contestate"]
    fields= output[list(output.keys())[0]].keys()
    writer = csv.DictWriter(csvfile, fieldnames=fields)
    writer.writeheader()
    for i in output.keys():
        writer.writerow(output[i])
  
