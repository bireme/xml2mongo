import argparse
import datetime
import os
import logging

import xmltodict
import xml.etree.ElementTree as ET

from tqdm import tqdm
from dataclasses import dataclass

from mongodb.mongoExport import MongoExport


@dataclass
class X2M_Parameters:
    xmlDir: str
    database: str
    collection: str
    host: int
    port: str
    user: str
    password: str
    xmlFilter: str
    xmlFileEncod: str
    logFile: str
    recursive: bool
    clear: bool
    bulkWrite: bool


class Xml2Mongo:

    def export_files(self: X2M_Parameters):
        try:
            export_mongo: MongoExport = MongoExport(self.database, self.collection, self.clear,
                                                    self.host, self.port, self.user, self.password)
            os.chdir(self.xmlDir)
            path_xml = os.listdir()
            xml_encod: str = self.xmlFileEncod or "utf-8"

            print(f"\n{datetime.datetime.now().strftime('Export start time %H:%M')}\nTotal files for export:"
                  f" {len(path_xml)}\n")

            for xml in (xml for xml in path_xml if xml.endswith(".xml")):
                with open(xml, 'r') as xml_file:
                    tree = ET.parse(xml_file)
                    root = tree.getroot()
                    print(f"Name File: {xml}")
                    for row in tqdm(root.findall('PubmedArticle')):
                        xml_str = ET.tostring(row, encoding=xml_encod)
                        doc = xmltodict.parse(xml_str, attr_prefix='', cdata_key='text')
                        export_mongo.insert(doc)
                xml_file.close()

        except:
            logging.exception("Error Export")
        else:
            print(f"\n***Export completed!***\n{datetime.datetime.now().strftime('Export end time %H:%M')}")


class main:

    parse = argparse.ArgumentParser(description='Argumentos requeridos: -xmlDir, -database, -collection.')

    parse.add_argument('-xmlDir', action='store', dest='xmlDir',
                       required=True, help='diretório dos arquivos XML'),

    parse.add_argument('-database', action='store', dest='database',
                       required=True, help='Nome do banco de dados MongoDB')

    parse.add_argument('-collection', action='store', dest='collection',
                       required=True, help='Nome da coleção de banco de dados MongoDB')

    parse.add_argument('-host', action='store', dest='host',
                       required=False, help='Nome do servidor MongoDB. O valor padrão é localhost')

    parse.add_argument('-port', type=int, action='store', dest='port',
                       required=False, help='Número da porta do servidor MongoDB. O valor padrão é 27017')

    parse.add_argument('-user', action='store', dest='user',
                       required=False, help='Nome de usuário do MongoDB')

    parse.add_argument('-password', action='store', dest='password',
                       required=False, help='Senha do usuário MongoDB')

    parse.add_argument('-xmlFilter', action='store', dest='xmlFilter',
                       required=False,
                       help='se presente, usa a expressão regular para filtrar os nomes de arquivo xml desejados')

    parse.add_argument('-xmlFileEncod', action='store', dest='xmlFileEncod',
                       required=False, help='se presente, indique a codificação do arquivo xml. O padrão é utf-8')

    parse.add_argument('-logFile', action='store', dest='logFile',
                       required=False,
                       help='se presente, indique o nome de um arquivo de log com os nomes dos arquivos '
                            'XML que não foram importados por causa de bugs')

    parse.add_argument('--recursive', action='store_true', dest='recursive',
                       required=False, help='se presente, procure por documentos xml em subdiretórios')

    parse.add_argument('--clear', action='store_true', dest='clear',
                       required=False,
                       help='se presente, limpe todos os documentos da coleção antes de importar novos')

    parse.add_argument('--bulkWrite', action='store_true', dest='bulkWrite',
                       required=False, help='se estiver presente irá escrever muitos documentos no MongoDb a cada '
                                            'iteração (requer mais RAM disponível')

    args = parse.parse_args()

    params: X2M_Parameters = X2M_Parameters(args.xmlDir, args.database, args.collection,
                                            args.host, args.port, args.user, args.password,
                                            args.xmlFilter, args.xmlFileEncod,
                                            args.logFile, args.recursive, args.clear,
                                            args.bulkWrite)

    Xml2Mongo.export_files(params)
