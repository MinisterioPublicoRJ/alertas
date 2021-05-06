#-*-coding:utf-8-*-
import os
import base
import argparse
from jobs import AlertaSession

if __name__ == "__main__":
    os.environ['PYTHON_EGG_CACHE'] = "/tmp"

    parser = argparse.ArgumentParser(description="Execute process")
    parser.add_argument('-e','--schemaExadata', metavar='schemaExadata', type=str, help='')
    parser.add_argument('-a','--schemaExadataAux', metavar='schemaExadataAux', type=str, help='')
    parser.add_argument('-g','--schemaOpenGeo', metavar='schemaOpenGeo', type=str, help='')
    parser.add_argument('-i','--impalaHost', metavar='impalaHost', type=str, help='')
    parser.add_argument('-o','--impalaPort', metavar='impalaPort', type=str, help='')
    parser.add_argument('-pl', '--prescricaoLimiar', metavar='prescricaoLimiar', type=int, default=90, help='')
    parser.add_argument('-al', '--schemaAlertas', metavar='schemaAlertas', type=str, help='')
    parser.add_argument('-ac', '--schemaAlertasCompras', metavar='schemaAlertasCompras', type=str, help='')
    parser.add_argument('-dtb', '--dateTACBegin', metavar='dateTACBegin', type=str, default='2021-05-01', help='')
    args = parser.parse_args()

    options = {
                    'schema_exadata': args.schemaExadata,
                    'schema_exadata_aux': args.schemaExadataAux,
                    'schema_opengeo': args.schemaOpenGeo,
                    'impala_host' : args.impalaHost,
                    'impala_port' : args.impalaPort,
                    'prescricao_limiar': args.prescricaoLimiar,
                    'schema_alertas': args.schemaAlertas,
                    'schema_alertas_compras': args.schemaAlertasCompras,
                    'date_tac_begin': args.dateTACBegin,
                }
    session = AlertaSession(options)
    session.generateAlertas()
