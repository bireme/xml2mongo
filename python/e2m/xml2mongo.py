import argparse
import datetime
import os

import xmltodict
import xml.etree.ElementTree as ET

from tqdm import tqdm
from dataclasses import dataclass

from mongodb.mongoExport import MongoExport


@dataclass
class X2M_Parameters:
    tagMain: str
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

        os.chdir(self.xmlDir)
        path_xml = os.listdir()

        xml_encod: str = self.xmlFileEncod or "utf-8"
        export_mongo: MongoExport = MongoExport(self.database, self.collection, self.clear,
                                                self.host, self.port, self.user, self.password)

        print(f"\n--{datetime.datetime.now().strftime('Export start time %H:%M')}--")
        print(f"Total files for export:"f" {len(path_xml)}\n")

        for xml in (xml for xml in path_xml if Xml2Mongo.str2elem(xml)):
            try:
                print(f"Name File: {xml}")
                for row in tqdm(Xml2Mongo.str2elem(xml).findall(self.tagMain)):
                    xml_str = ET.tostring(row, encoding=xml_encod)
                    doc = xmltodict.parse(xml_str, attr_prefix='', cdata_key='text')
                    export_mongo.insert(doc)
            except Exception as e:
                print(e)

        print(f"\n--{datetime.datetime.now().strftime('Export end time %H:%M')}--\n")

    @staticmethod
    def str2elem(xml):
        try:
            with open(xml, 'r') as xml_file:
                tree = ET.parse(xml_file)
                xml_re = tree.getroot()
                xml_file.close()
                return xml_re

        except Exception:
            print(f"Name File: {xml}\nFILE CONVERSION ERROR")


class main:
    parse = argparse.ArgumentParser(description='Argumentos requeridos: -xmlDir, -database, -collection.')

    parse.add_argument('--tagMain', action='store', dest='tagMain',
                       required=True, help='Retorna a lista contendo todos os elementos correspondentes na ordem do '
                                           'documento.'),

    parse.add_argument('-xmlDir', action='store', dest='xmlDir',
                       required=True, help='Diret??rio dos arquivos XML'),

    parse.add_argument('-database', action='store', dest='database',
                       required=True, help='Nome do banco de dados MongoDB')

    parse.add_argument('-collection', action='store', dest='collection',
                       required=True, help='Nome da cole????o de banco de dados MongoDB')

    parse.add_argument('-host', action='store', dest='host',
                       required=False, help='Nome do servidor MongoDB. O valor padr??o ?? localhost')

    parse.add_argument('-port', type=int, action='store', dest='port',
                       required=False, help='N??mero da porta do servidor MongoDB. O valor padr??o ?? 27017')

    parse.add_argument('-user', action='store', dest='user',
                       required=False, help='Nome de usu??rio do MongoDB')

    parse.add_argument('-password', action='store', dest='password',
                       required=False, help='Senha do usu??rio MongoDB')

    parse.add_argument('-xmlFilter', action='store', dest='xmlFilter',
                       required=False,
                       help='Se presente, usa a express??o regular para filtrar os nomes de arquivo xml desejados')

    parse.add_argument('-xmlFileEncod', action='store', dest='xmlFileEncod',
                       required=False, help='Se presente, indique a codifica????o do arquivo xml. O padr??o ?? utf-8')

    parse.add_argument('-logFile', action='store', dest='logFile',
                       required=False,
                       help='Se presente, indique o nome de um arquivo de log com os nomes dos arquivos '
                            'XML que n??o foram importados por causa de bugs')

    parse.add_argument('--recursive', action='store_true', dest='recursive',
                       required=False, help='Se presente, procure por documentos xml em subdiret??rios')

    parse.add_argument('--clear', action='store_true', dest='clear',
                       required=False,
                       help='Se presente, limpe todos os documentos da cole????o antes de importar novos')

    parse.add_argument('--bulkWrite', action='store_true', dest='bulkWrite',
                       required=False, help='Se estiver presente ir?? escrever muitos documentos no MongoDb a cada '
                                            'itera????o (requer mais RAM dispon??vel')

    args = parse.parse_args()

    params: X2M_Parameters = X2M_Parameters(args.tagMain, args.xmlDir, args.database, args.collection,
                                            args.host, args.port, args.user, args.password,
                                            args.xmlFilter, args.xmlFileEncod,
                                            args.logFile, args.recursive, args.clear,
                                            args.bulkWrite)

    Xml2Mongo.export_files(params)
