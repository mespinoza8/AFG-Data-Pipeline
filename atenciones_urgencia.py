import pandas as pd
import os

path=os.listdir("datos")


def carga_atenciones_urgencia():

    for file in path:
        if "urg" in file:
            df=pd.read_parquet(os.path.join("datos", file))

    diagnosticos=['Bronquitis/bronquiolitis aguda (J20-J21)',
              'Crisis obstructiva bronquial (J40-J46)',
              'Covid-19, Virus no identificado U07.2',
              'Otra causa respiratoria (J22, J30-J39, J47, J60-J98)',
              'Influenza (J09-J11)',
              'Covid-19, Virus identificado U07.1',
              'Neumonía (J12-J18)']

    cols=['RegionGlosa','ComunaGlosa','ServicioSaludGlosa','TipoUrgencia',
      'NivelComplejidad','Anio','Causa','SemanaEstadistica','NumTotal',
      'NumMenor1Anio','Num1a4Anios','Num5a14Anios','Num15a64Anios',
      'Num65oMas']
    
    df=df.query("RegionCodigo=='13' & Anio>=2019 & Causa.isin(@diagnosticos)")[cols]

    return df

