#-*-coding:utf-8-*-
from pyspark.sql.functions import *

from base import spark
from utils import uuidsha


proto_columns = ['docu_dk', 'docu_nr_mp', 'docu_orgi_orga_dk_responsavel'] 

columns = [
    col('docu_dk').alias('alrt_docu_dk'), 
    col('docu_nr_mp').alias('alrt_docu_nr_mp'), 
    col('dt_tac').alias('alrt_date_referencia'),  
    col('docu_orgi_orga_dk_responsavel').alias('alrt_orgi_orga_dk'),
    col('elapsed').alias('alrt_dias_referencia'),
    col('alrt_key'),
]

key_columns = [
    col('docu_dk'),
    col('dt_tac'),
]

def alerta_ctac(options):
    """
    Alerta de TACs que não foram comunicadas ao CSMP. Um andamento de TAC
    ativa o alerta. Um andamento de ciência ao CSMP em data maior ou igual ao
    TAC desativa o alerta. Um andamento de ofício com destinatário ao CSMP
    (vide regex) também desativa o alerta.
    """
    ANDAMENTOS_OFICIO = (7436, 6581, 6497, 6614, 6615, 6616, 6617, 6618, 6619, 6126, 6989)
    ANDAMENTOS_TAC = (1007, 6304, 7858, 6326, 6655, 6670, 4114)
    ANDAMENTOS_CIENCIA = (6647, 6643, 6683, 6684, 6685, 6573)

    ANDAMENTOS_TAC_CIENCIA = ANDAMENTOS_TAC + ANDAMENTOS_CIENCIA
    ANDAMENTOS_OFICIO_CIENCIA = ANDAMENTOS_OFICIO + ANDAMENTOS_CIENCIA

    REGEX_CSMP = (
        "(CSMP|CONSELHO SUPERIOR|CONSELHO SUPERIOR DO MINIST[ÉE]RIO P[ÚU]BLICO)"
    )

    resultado = spark.sql("""
        SELECT docu_dk, docu_nr_mp, docu_orgi_orga_dk_responsavel, dt_tac, datediff(current_timestamp(), dt_tac) as elapsed
        FROM
        (
            SELECT docu_dk, docu_nr_mp, docu_orgi_orga_dk_responsavel, MAX(dt_tac) AS dt_tac, MAX(dt_oficio_csmp) as dt_oficio
            FROM 
            (
                SELECT docu_dk, docu_nr_mp, docu_orgi_orga_dk_responsavel,
                CASE WHEN stao_tppr_dk IN {ANDAMENTOS_OFICIO_CIENCIA} THEN pcao_dt_andamento ELSE NULL END as dt_oficio_csmp,
                CASE WHEN stao_tppr_dk IN {ANDAMENTOS_TAC} THEN pcao_dt_andamento ELSE NULL END as dt_tac
                FROM documento
                JOIN (
                    SELECT * FROM
                    vista
                    JOIN {schema_exadata}.mcpr_andamento ON pcao_vist_dk = vist_dk
                    JOIN {schema_exadata}.mcpr_sub_andamento ON stao_pcao_dk = pcao_dk
                    WHERE pcao_dt_cancelamento IS NULL
                    AND pcao_dt_andamento >= to_timestamp('{date_tac_begin}', 'yyyy-MM-dd')
                    AND (
                        stao_tppr_dk in {ANDAMENTOS_TAC_CIENCIA} OR
                        (stao_tppr_dk in {ANDAMENTOS_OFICIO} AND upper(stao_destinatario) REGEXP '{REGEX_CSMP}')

                    )
                ) T ON T.vist_docu_dk = docu_dk
            ) A
            GROUP BY docu_dk, docu_nr_mp, docu_orgi_orga_dk_responsavel
            HAVING MAX(dt_tac) IS NOT NULL
        ) B
        WHERE dt_oficio IS NULL OR dt_tac > dt_oficio
    """.format(
            schema_exadata=options['schema_exadata'],
            date_tac_begin=options['date_tac_begin'],
            ANDAMENTOS_TAC=ANDAMENTOS_TAC,
            ANDAMENTOS_OFICIO=ANDAMENTOS_OFICIO,
            ANDAMENTOS_TAC_CIENCIA=ANDAMENTOS_TAC_CIENCIA,
            ANDAMENTOS_OFICIO_CIENCIA=ANDAMENTOS_OFICIO_CIENCIA,
            REGEX_CSMP=REGEX_CSMP
        )
    )

    resultado = resultado.withColumn('alrt_key', uuidsha(*key_columns))

    return resultado.select(columns)
