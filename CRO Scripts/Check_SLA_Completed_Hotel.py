## IMPORTAMOS LIBRERIAS ##
import pandas as pd
import numpy as np
import pandas_gbq
from google.cloud import bigquery
import os
import win32com.client
import warnings
warnings.filterwarnings('ignore')
from oauth2client.service_account import ServiceAccountCredentials
from datetime import date, timedelta, datetime
import time
## CREAMOS VARIABLES ##
today = datetime.now().strftime('%d-%m-%Y')

## CREAMOS PRIMER DF PARA DETECTAR SI TENEMOS HOTELES CON CRO_No_CRO = 'NO CRO'. 

print("Creando primer DF")
df = pandas_gbq.read_gbq("""
        WITH
-- Agregamos los mails reales por fecha y hotel
data_p AS (
  SELECT
    Date,
    -- Hotel,
    CASE
      WHEN Hotel = 'TBC NH Collection Bodega Kopke' THEN 'PT13.KOPKE'
      WHEN Hotel = 'NH Collection Mexico City Reforma' THEN 'MXDF.MECIT'
      WHEN Hotel = 'nhow Lima Miraflores' THEN 'PELI.WLIMA'
      WHEN Hotel = 'NH Mexico City Reforma Centro' THEN 'MXDF.MECIT'
      WHEN Hotel = 'TBC (La Perla Aguascalientes, MX)' THEN 'MXXX.XXXXX'
      WHEN Hotel = 'NH Collection Ibiza Marina' THEN 'ESIB.IBIZA'
      ELSE Hotel_Id
    END AS Hotel_Id,
    SUM(Mails) AS Mails
  FROM `nh-cro-forecast.Ad_Hoc.taskforce_mail_hotel_v2`
  WHERE Date >= '2025-01-01'
  GROUP BY Date,Hotel_Id
),

-- Agregamos el revenue real por fecha y hotel
data_r AS (
  SELECT
    Date,
    Hotel_Name AS Hotel,
    Hotel_ID,
    SUM(TOTAL_REV) AS Total_Rev,
    SUM(ROOMS) AS Rooms,
    SUM(ROOM_REV) AS Room_Rev,
    SUM(NIGHTS) AS Nights,
  FROM `nh-cro-forecast.Ad_Hoc.Revenue_mails_per_hotel`
  WHERE Date >= '2025-01-01'
  GROUP BY Date, Hotel_Name, Hotel_ID
),
-- Unificamos los datos reales de mails y revenue

data_real AS (
  SELECT
    COALESCE(p.Date, r.Date) AS Date,
    -- COALESCE(p.Hotel, r.Hotel) AS Hotel,
    COALESCE(p.Hotel_Id, r.Hotel_Id) AS Hotel_Id,
    COALESCE(r.Total_Rev, 0) AS Total_Rev,
    COALESCE(p.Mails, 0) AS Mails,
    COALESCE(r.Rooms, 0) AS Rooms,
    COALESCE(r.Room_Rev, 0) AS Room_Rev,
    COALESCE(r.Nights, 0) AS Nights,
  FROM data_p p
  FULL OUTER JOIN data_r r
    ON p.Date = r.Date AND p.Hotel_Id = r.Hotel_ID
),
-- Corregimos los nombres de hotel para obtener una versión uniforme y eliminamos duplicados
hoteles AS (
  SELECT
    Hotel,
    MIN(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          REGEXP_REPLACE(
                            REGEXP_REPLACE(
                              REGEXP_REPLACE(Hotel, r'’','´'),
                            r'â€™', "´"),
                          r'â',"´"),
                        r"Ã­","í"),
                      r"Ãº","ú"),
                    r"Ã¡","á"),
                  r"Ã¼","ü"),
                r"Ã³","ó"),
              r"Ã¶","ö"),
            r"Ã©","é"),
          r"Â",""),
        r"Ã±","ñ"),
      r"Ã£","ã")
    ) AS Hotel_Corregido,
    MIN(
      CASE
        WHEN Hotel = 'TBC NH Collection Bodega Kopke' THEN 'PT13.KOPKE'
        WHEN Hotel = 'NH Collection Mexico City Reforma' THEN 'MXDF.MECIT'
        WHEN Hotel = 'nhow Lima Miraflores' THEN 'PELI.WLIMA'
        WHEN Hotel = 'NH Mexico City Reforma Centro' THEN 'MXDF.MECIT'
        WHEN Hotel = 'TBC (La Perla Aguascalientes, MX)' THEN 'MXXX.XXXXX'
        ELSE Hotel_Id
      END
    ) AS Hotel_Id
  FROM `nh-cro-forecast.Ad_Hoc.taskforce_mail_hotel`
  WHERE Date >= '2025-01-01'
  GROUP BY Hotel
),
-- Nueva CTE para obtener los Hotel_Id únicos (sin duplicados)
hoteles_unicos AS (
  SELECT
    Hotel_Id,
    MIN(Hotel_Corregido) AS Hotel_Corregido
  FROM hoteles
  GROUP BY Hotel_Id
),
-- Unimos la información real con la corrección de nombres y asignamos el país 
data_final AS (
  SELECT
    d.Date,
    hu.Hotel_Corregido,
    d.Hotel_Id,
    d.Total_Rev,
    d.Mails,
    d.rooms,
    d.Room_Rev,
    d.Nights,
    CASE
      WHEN hu.Hotel_Corregido IN ('NH Ventas','NH Collection Aránzazu','NH Califa','NH Collection Gran Hotel Calderón','NH Lagasca','NH Collection Eurobuilding','NH Ribera del Manzanares','NH Deusto','NH Nacional','NH Collection Plaza Mayor','NH Collection Pódium','NH Herencia Rioja','NH Chamberí','NH Las Tablas','NH Collection Constanza','NH Palacio del Duero','NH Barajas Airport','NH Collection Palacio de Oquendo','NH Collection Palacio de Castellanos','NH Mindoro','NH Collection Gran Hotel','NH Collection Santiago','NH Atocha','NH Collection Abascal','NH Plaza de Armas','NH Principado','NH Pirineos','NH Paseo de la Habana','NH Les Corts','NH Collection Colón','NH Balboa','NH Entenza','NH Las Ciencias','NH Collection Victoria','NH Collection Suecia','NH Bálago','NH Collection Palacio de Tepa','NH Playa las Canteras','NH Diagonal Center','NH Iruña Park','NH Collection Finisterre','NH Príncipe de Vergara','NH Eixample','NH Collection Gran Vía','NH Zurbano','NH Collection Palomé','NH Las Artes','NH Center','NH Collection Paseo del Prado','NH Ciudad de la Imagen') 
      THEN 'SPAIN'

      WHEN hu.Hotel_Corregido IN ('NH Collection Firenze Palazzo Gaddi','Tivoli Palazzo Gaddi Firence Hotel','NH Collection President','NH Machiavelli','NH Collection Murano Villa','NH Fiera','NH Collection Piazza Carlina','NH Collection Marina','NH Milano Buenos Aires','NH Collection Vittorio Veneto','NH Collection Santo Stefano','NH Villa Carpegna','NH Collection Porta Rossa','NH Collection Porta Nuova','NH Parco Degli Aragonesi Catania','NH Pontevecchio','NH Laguna Palace','NH Collection Giustiniano','NH Collection Fori Imperiali','NH Collection Murano','NH Villa San Mauro','NH Lingotto Congress','NH Concordia','NH Anglo American','NH Collection Palazzo Barocci','NH Collection Palazzo Cinquecento') 
      THEN 'ITALY'

      WHEN hu.Hotel_Corregido IN ('TBC NH Collection Bodega Kopke','Tivoli Avenida Liberdade','Anantara Vilamoura','Tivoli The Residences at Victoria Golf Club','Avani Avenida Liberdade','NH Campo Grande','NH Collection Liberdade')
      THEN 'PORTUGAL'

      WHEN hu.Hotel_Corregido IN ('NH MAASTRICHT','NH Museum Quarter','NH Schiller','NH Conference Centre Koningshof','NH City Centre','NH Schiphol Airport','NH Caransa','NH Collection Barbizon Palace','NH Conference Centre Leeuwenhorst','NH Jan Tabak','NH Sparrenhorst')
      THEN 'NETHERLANDS'

      WHEN hu.Hotel_Corregido IN ('NH Mexico City Reforma Centro','NH Valle Dorado','NH Collection Aeropuerto T2 Mexico','NH Collection Santa Fe','NH Collection Centro Histórico','TBC (La Perla Aguascalientes, MX)')
      THEN 'MEXICO'

      WHEN hu.Hotel_Corregido IN ('NH Duesseldorf City Nord','NH Collection Berlin Mitte','NH München Ost','NH Horner Rennbahn','NH Collection Berlin Friedrichstrasse')
      THEN 'GERMANY'

      WHEN hu.Hotel_Corregido IN ('NH Collection Royal Hacienda','NH Collection Royal Medellín','NH Collection Royal Terra 100','NH Royal Cali','NH Collection Royal Andino','NH Collection Royal WTC Bogotá','NH Collection Royal Teleport','NH Collection Royal Smartsuites','NH Royal Urban 26','NH Royal Urban Cartagena','NH Royal Pavillon','NH Royal Urban 93')
      THEN 'COLOMBIA'

      WHEN hu.Hotel_Corregido IN ('NH Urbano','NH Collection Buenos Aires Crillón','NH 9 de Julio','NH Collection Lancaster','NH Florida','NH Latino','NH Collection Jousten','NH Edelweiss','NH Tango','NH Panorama','City Hotel NH')
      THEN 'ARGENTINA'

      WHEN hu.Hotel_Corregido IN ('NH Carrefour de l´Europe','NH Collection Grand Sablon','NH Grand Place Arenberg','NH Carrefour de l’Europe','NH Brussels Carrefour de l´Europe','NH Gent Belfort','NH Stéphanie')
      THEN 'BELGIUM'

      WHEN hu.Hotel_Corregido IN ('NH Iquique','NH Collection Casacostanera') 
      THEN 'CHILE'

      WHEN hu.Hotel_Corregido IN ('NH Opera Faubourg',"NH Gare de l´Est") THEN 'FRANCE'
      WHEN hu.Hotel_Corregido = 'NH Luxembourg' THEN 'LUXEMBOURG'
      WHEN hu.Hotel_Corregido = 'NH Gate One' THEN 'SLOVAKIA'
      WHEN hu.Hotel_Corregido = 'NH Collection Royal Quito' THEN 'ECUADOR'
      WHEN hu.Hotel_Corregido = 'nhow Lima Miraflores' THEN 'PERU'
      WHEN hu.Hotel_Corregido = 'NH Vienna Airport' THEN 'AUSTRIA'
      WHEN m.Hotel_Country = 'The Netherlands' THEN 'NETHERLANDS'
      ELSE UPPER(m.Hotel_Country)
    END AS Country_MD_Hotel
  FROM data_real d
  LEFT JOIN hoteles_unicos hu ON d.Hotel_Id = hu.Hotel_Id
  LEFT JOIN `nh-spit-resevations.bbdd_maestros.MD_Hotel_Format` m ON d.Hotel_ID = m.Hotel_Id
),

YTD_agg AS (
  SELECT
    Date,
    UPPER(Country_MD_Hotel) AS Country,
    -- Hotel_Id,
    SUM(Total_Rev) AS Total_Rev,
    SUM(Mails) AS Mails,
    SUM(Rooms) AS Rooms,
    SUM(Room_Rev) AS Room_Rev,
    SUM(Nights) AS Nights
  FROM data_final
  GROUP BY Date, UPPER(Country_MD_Hotel)
),

BG_agg AS (
  SELECT
    CAST(date AS DATE) AS date,
    UPPER(Country) AS Country,
    SUM(CAST(Total_Revenue AS FLOAT64)) AS Total_Rev_BG,
    SUM(CAST(Productive_Mails AS FLOAT64)) AS Mails_BG,
    SUM(CAST(Rooms AS FLOAT64)) AS Rooms_BG,
    SUM(CAST(Room_Revenue AS FLOAT64)) AS Room_Revenue_BG,
    SUM(CAST(Nights AS NUMERIC)) AS Nights_BG
  FROM `nh-cro-forecast.KPI_metrics.Budget_mail_2025_V_2`
  GROUP BY CAST(date AS DATE), UPPER(Country)
),
todo as(
-- Unimos ambas fuentes por fecha y país
SELECT
  COALESCE(BG.date, Y.Date) AS Date,
  COALESCE(BG.Country, Y.Country) AS Country,
  CASE 
    WHEN COALESCE(BG.Country, Y.Country) = "PORTUGAL" THEN "CRO Lisbon"
    WHEN COALESCE(BG.Country, Y.Country) IN ("ANDORRA", "AUSTRIA", "BELGIUM", "CZECH REPUBLIC", "DENMARK", "FINLAND", "FRANCE", "GERMANY", "HUNGARY", "IRELAND", "ITALY", "LUXEMBOURG", "POLAND", "ROMANIA", "SLOVAKIA", "SOUTHAFRICA","SPAIN", "SWITZERLAND", "NETHERLANDS", "TUNISIA", "UNITED KINGDOM", "USA") THEN "CRO"
    WHEN COALESCE(BG.Country, Y.Country) IN ("CUBA","DOMINICAN REPUBLIC","MEXICO") THEN "CRO Mexico"
    WHEN COALESCE(BG.Country, Y.Country) IN ("COLOMBIA","ECUADOR","HAITI","PERU","ARGENTINA","CHILE","URUGUAY","BRASIL") THEN "CRO Colombia"
    ELSE "NO CRO"
  END AS CRO_No_CRO,
  BG.Total_Rev_BG,
  BG.Mails_BG,
  BG.Rooms_BG,
  BG.Room_Revenue_BG,
  BG.Nights_BG,
  Y.Total_Rev,
  Y.Mails,
  Y.Rooms,
  Y.Room_Rev,
  Y.Nights
FROM BG_agg BG
FULL OUTER JOIN YTD_agg Y ON BG.date = Y.Date AND BG.Country = Y.Country
)
SELECT * 
from todo 
WHERE CRO_No_CRO = 'NO CRO'
"""
,project_id="nh-cro-forecast"
)

print("Creado DF")
time.sleep(2)
print(df.head())
time.sleep(20)


### SI EL DF ES MAYOR A 0 SIGNIFICA QUE TENEMOS ALGUN TIPO DE ERROR ###
print('Empezamos segunda parte del proceso')
if len(df) > 0:
    print("Hay Errores")
    time.sleep(2)
    ## CREAMOS OTRO DF, EL DEFINITIVO QUE MANDAREMOS PARA SABER QUE EL HOTEL DONDE ESTA EL FALLO ##
    print("Descargamos el segundo DF que mandaremos para ver los fallos")
    time.sleep(5)
    df_2 = pandas_gbq.read_gbq("""WITH
    -- Agregamos los mails reales por fecha y hotel
    data_p AS (
      SELECT
        Date,
        -- Hotel,
        CASE
          WHEN Hotel = 'TBC NH Collection Bodega Kopke' THEN 'PT13.KOPKE'
          WHEN Hotel = 'NH Collection Mexico City Reforma' THEN 'MXDF.MECIT'
          WHEN Hotel = 'nhow Lima Miraflores' THEN 'PELI.WLIMA'
          WHEN Hotel = 'NH Mexico City Reforma Centro' THEN 'MXDF.MECIT'
          WHEN Hotel = 'TBC (La Perla Aguascalientes, MX)' THEN 'MXXX.XXXXX'
          WHEN Hotel = 'NH Collection Ibiza Marina' THEN 'ESIB.IBIZA'
          ELSE Hotel_Id
        END AS Hotel_Id,
        SUM(Mails) AS Mails
      FROM `nh-cro-forecast.Ad_Hoc.taskforce_mail_hotel_v2`
      GROUP BY Date,Hotel_Id
    ),

    -- Agregamos el revenue real por fecha y hotel
    data_r AS (
      SELECT
        Date,
        Hotel_Name AS Hotel,
        Hotel_ID,
        SUM(TOTAL_REV) AS Total_Rev,
        SUM(ROOMS) AS Rooms,
        SUM(ROOM_REV) AS Room_Rev,
        SUM(NIGHTS) AS Nights,
      FROM `nh-cro-forecast.Ad_Hoc.Revenue_mails_per_hotel`
      WHERE Date >= '2025-01-01'
      GROUP BY Date, Hotel_Name, Hotel_ID
    ),
    -- Unificamos los datos reales de mails y revenue

    data_real AS (
      SELECT
        COALESCE(p.Date, r.Date) AS Date,
        -- COALESCE(p.Hotel, r.Hotel) AS Hotel,
        COALESCE(p.Hotel_Id, r.Hotel_Id) AS Hotel_Id,
        COALESCE(r.Total_Rev, 0) AS Total_Rev,
        COALESCE(p.Mails, 0) AS Mails,
        COALESCE(r.Rooms, 0) AS Rooms,
        COALESCE(r.Room_Rev, 0) AS Room_Rev,
        COALESCE(r.Nights, 0) AS Nights,
      FROM data_p p
      FULL OUTER JOIN data_r r
        ON p.Date = r.Date AND p.Hotel_Id = r.Hotel_ID
    ),
    -- Corregimos los nombres de hotel 
    hoteles AS (
      SELECT
        Hotel,
        MIN(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          REGEXP_REPLACE(
                            REGEXP_REPLACE(
                              REGEXP_REPLACE(
                                REGEXP_REPLACE(
                                  REGEXP_REPLACE(Hotel, r'’','´'),
                                r'â€™', "´"),
                              r'â',"´"),
                            r"Ã­","í"),
                          r"Ãº","ú"),
                        r"Ã¡","á"),
                      r"Ã¼","ü"),
                    r"Ã³","ó"),
                  r"Ã¶","ö"),
                r"Ã©","é"),
              r"Â",""),
            r"Ã±","ñ"),
          r"Ã£","ã")
        ) AS Hotel_Corregido,
        MIN(
          CASE
            WHEN Hotel = 'TBC NH Collection Bodega Kopke' THEN 'PT13.KOPKE'
            WHEN Hotel = 'NH Collection Mexico City Reforma' THEN 'MXDF.MECIT'
            WHEN Hotel = 'nhow Lima Miraflores' THEN 'PELI.WLIMA'
            WHEN Hotel = 'NH Mexico City Reforma Centro' THEN 'MXDF.MECIT'
            WHEN Hotel = 'TBC (La Perla Aguascalientes, MX)' THEN 'MXXX.XXXXX'
            ELSE Hotel_Id
          END
        ) AS Hotel_Id
      FROM `nh-cro-forecast.Ad_Hoc.taskforce_mail_hotel`
      WHERE Date >= '2025-01-01'
      GROUP BY Hotel
    ),
    -- obtenemos los Hotel_Id únicos
    hoteles_unicos AS (
      SELECT
        Hotel_Id,
        MIN(Hotel_Corregido) AS Hotel_Corregido
      FROM hoteles
      GROUP BY Hotel_Id
    ),
    -- Unimos la información real con la corrección de nombres y asignamos el país 
    data_final AS (
      SELECT
        d.Date,
        hu.Hotel_Corregido,
        d.Hotel_Id,
        d.Total_Rev,
        d.Mails,
        d.rooms,
        d.Room_Rev,
        d.Nights,
        CASE
          WHEN hu.Hotel_Corregido IN ('NH Ventas','NH Collection Aránzazu','NH Califa','NH Collection Gran Hotel Calderón','NH Lagasca','NH Collection Eurobuilding','NH Ribera del Manzanares','NH Deusto','NH Nacional','NH Collection Plaza Mayor','NH Collection Pódium','NH Herencia Rioja','NH Chamberí','NH Las Tablas','NH Collection Constanza','NH Palacio del Duero','NH Barajas Airport','NH Collection Palacio de Oquendo','NH Collection Palacio de Castellanos','NH Mindoro','NH Collection Gran Hotel','NH Collection Santiago','NH Atocha','NH Collection Abascal','NH Plaza de Armas','NH Principado','NH Pirineos','NH Paseo de la Habana','NH Les Corts','NH Collection Colón','NH Balboa','NH Entenza','NH Las Ciencias','NH Collection Victoria','NH Collection Suecia','NH Bálago','NH Collection Palacio de Tepa','NH Playa las Canteras','NH Diagonal Center','NH Iruña Park','NH Collection Finisterre','NH Príncipe de Vergara','NH Eixample','NH Collection Gran Vía','NH Zurbano','NH Collection Palomé','NH Las Artes','NH Center','NH Collection Paseo del Prado','NH Ciudad de la Imagen') 
          THEN 'SPAIN'

          WHEN hu.Hotel_Corregido IN ('NH Collection Firenze Palazzo Gaddi','Tivoli Palazzo Gaddi Firence Hotel','NH Collection President','NH Machiavelli','NH Collection Murano Villa','NH Fiera','NH Collection Piazza Carlina','NH Collection Marina','NH Milano Buenos Aires','NH Collection Vittorio Veneto','NH Collection Santo Stefano','NH Villa Carpegna','NH Collection Porta Rossa','NH Collection Porta Nuova','NH Parco Degli Aragonesi Catania','NH Pontevecchio','NH Laguna Palace','NH Collection Giustiniano','NH Collection Fori Imperiali','NH Collection Murano','NH Villa San Mauro','NH Lingotto Congress','NH Concordia','NH Anglo American','NH Collection Palazzo Barocci','NH Collection Palazzo Cinquecento') 
          THEN 'ITALY'

          WHEN hu.Hotel_Corregido IN ('TBC NH Collection Bodega Kopke','Tivoli Avenida Liberdade','Anantara Vilamoura','Tivoli The Residences at Victoria Golf Club','Avani Avenida Liberdade','NH Campo Grande','NH Collection Liberdade')
          THEN 'PORTUGAL'

          WHEN hu.Hotel_Corregido IN ('NH MAASTRICHT','NH Museum Quarter','NH Schiller','NH Conference Centre Koningshof','NH City Centre','NH Schiphol Airport','NH Caransa','NH Collection Barbizon Palace','NH Conference Centre Leeuwenhorst','NH Jan Tabak','NH Sparrenhorst')
          THEN 'NETHERLANDS'

          WHEN hu.Hotel_Corregido IN ('NH Mexico City Reforma Centro','NH Valle Dorado','NH Collection Aeropuerto T2 Mexico','NH Collection Santa Fe','NH Collection Centro Histórico','TBC (La Perla Aguascalientes, MX)')
          THEN 'MEXICO'

          WHEN hu.Hotel_Corregido IN ('NH Duesseldorf City Nord','NH Collection Berlin Mitte','NH München Ost','NH Horner Rennbahn','NH Collection Berlin Friedrichstrasse')
          THEN 'GERMANY'

          WHEN hu.Hotel_Corregido IN ('NH Collection Royal Hacienda','NH Collection Royal Medellín','NH Collection Royal Terra 100','NH Royal Cali','NH Collection Royal Andino','NH Collection Royal WTC Bogotá','NH Collection Royal Teleport','NH Collection Royal Smartsuites','NH Royal Urban 26','NH Royal Urban Cartagena','NH Royal Pavillon','NH Royal Urban 93')
          THEN 'COLOMBIA'

          WHEN hu.Hotel_Corregido IN ('NH Urbano','NH Collection Buenos Aires Crillón','NH 9 de Julio','NH Collection Lancaster','NH Florida','NH Latino','NH Collection Jousten','NH Edelweiss','NH Tango','NH Panorama','City Hotel NH')
          THEN 'ARGENTINA'

          WHEN hu.Hotel_Corregido IN ('NH Carrefour de l´Europe','NH Collection Grand Sablon','NH Grand Place Arenberg','NH Carrefour de l’Europe','NH Brussels Carrefour de l´Europe','NH Gent Belfort','NH Stéphanie')
          THEN 'BELGIUM'

          WHEN hu.Hotel_Corregido IN ('NH Iquique','NH Collection Casacostanera') 
          THEN 'CHILE'

          WHEN hu.Hotel_Corregido IN ('NH Opera Faubourg',"NH Gare de l´Est") THEN 'FRANCE'
          WHEN hu.Hotel_Corregido = 'NH Luxembourg' THEN 'LUXEMBOURG'
          WHEN hu.Hotel_Corregido = 'NH Gate One' THEN 'SLOVAKIA'
          WHEN hu.Hotel_Corregido = 'NH Collection Royal Quito' THEN 'ECUADOR'
          WHEN hu.Hotel_Corregido = 'nhow Lima Miraflores' THEN 'PERU'
          WHEN hu.Hotel_Corregido = 'NH Vienna Airport' THEN 'AUSTRIA'
          WHEN m.Hotel_Country = 'The Netherlands' THEN 'NETHERLANDS'
          WHEN m.Hotel_Country = 'United Kingdom' THEN 'UK'
          ELSE UPPER(m.Hotel_Country)
        END AS Country_MD_Hotel
      FROM data_real d
      LEFT JOIN hoteles_unicos hu ON d.Hotel_Id = hu.Hotel_Id
      LEFT JOIN `nh-spit-resevations.bbdd_maestros.MD_Hotel_Format` m ON d.Hotel_ID = m.Hotel_Id
    )
    select *
    from data_final
    where 0 = 0
    AND Country_MD_Hotel is null
    and Date >= '2025-04-01'
    order by 1 desc
    """
    ,project_id="nh-cro-forecast"
    )
### ENVIAMOS MAIL ###
    print("Enviamos mail")
    time.sleep(3)
    try: 
        outlook_mail = win32com.client.Dispatch("Outlook.Application")
        mail = outlook_mail.CreateItem(0x0)
        mail.Subject = '---!ERROR EN SLA_COMPLETED_HOTEL!--- REVISAR --- - '+ today
        # attachment = mail.Attachments.Add(output_path)
        mail.To = 'ej.garcia@minor-hotels.com'
        mail.HTMLBody = "Se ha encontrado algún error en el SLA_Completed_Hotel "+ df_2.to_html(index=False)
        print('Mail enviado')
        mail.Send()
    except:
        print('Mail No enviado')

else:
    print('Todo correcto')
    time.sleep(10)