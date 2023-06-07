import boto3
import json


def lambda_handler(event, context):
    # Configurações do S3
    print("Evento recebido:" + str(event))
    source_bucket = "insira-bucket-entrada"
    source_key = event['Records'][0]['s3']['object']['key']
    destination_bucket_compra = 'insira-bucket-saida'
    destination_bucket_cancelamento = 'insira-bucket-saida-cancelamento'

    # Inicializar o cliente do S3
    s3_client = boto3.client('s3')

    try:
        # Ler o arquivo JSON de origem do S3
        response = s3_client.get_object(Bucket=source_bucket, Key=source_key)
        json_content = response['Body'].read().decode('utf-8')
        data = json.loads(json_content)

        # Gravar o novo arquivo JSON no S3
        if get_value(data, 'cliente'):
            # Transformar os nomes dos campos
            transformed_data = transform_json(data)

            # Converter os dados transformados de volta para JSON
            transformed_json = json.dumps(transformed_data)
            destination_key = "Formatado_" + str(data['nfeProc']['protNFe']['infProt']['chNFe']) + ".json"
            s3_client.put_object(Body=transformed_json, Bucket=destination_bucket_compra, Key=destination_key)
        else:
            transform_cancelled = json.dumps(data)
            destination_key = "Formatado_" + str(data['procEventoNFe']['retEvento']['infEvento']['chNFe']) + ".json"
            s3_client.put_object(Body=transform_cancelled, Bucket=destination_bucket_cancelamento, Key=destination_key)

        return {
            'statusCode': 200,
            'body': 'Transformação do JSON concluída com sucesso!'
        }
    except Exception as e:
        print(f"Erro durante a transformação do JSON: {str(e)}")
        return {
            'statusCode': 500,
            'body': f"Erro durante a transformação do JSON: {str(e)}"
        }


def transform_json(data):
    # Lógica para transformar os nomes dos campos do JSON
    infNFe = data['nfeProc']['NFe']['infNFe']
    idNfe = data['nfeProc']['protNFe']['infProt']

    transformed_data = {
        'cliente': get_value(data, 'cliente'),
        'idNfe': get_value(idNfe, 'chNFe'),
        'dhEmi': get_value(infNFe, 'ide', 'dhEmi'),
        'dhSaiEnt': get_value(infNFe, 'ide', 'dhSaiEnt'),
        'natOp': get_value(infNFe, 'ide', 'natOp'),
        'emit': {
            'CNPJ': get_value(infNFe, 'emit', 'CNPJ'),
            'xNome': get_value(infNFe, 'emit', 'xNome'),
            'xFant': get_value(infNFe, 'emit', 'xFant')
        },
        'dest': {
            'CPF': get_value(infNFe, 'dest', 'CPF'),
            'xNome': get_value(infNFe, 'dest', 'xNome'),
            'email': get_value(infNFe, 'dest', 'email')
        },
        'det': {
            'prod': transform_prod(get_value(infNFe, 'det', 'prod')),
            'imposto': {
                'vTotTrib': get_value(infNFe, 'det', 'imposto', 'vTotTrib'),
                'ICMS': {
                    'ICMS40': {
                        'orig': get_value(infNFe, 'det', 'imposto', 'ICMS', 'ICMS40', 'orig'),
                        'CST': get_value(infNFe, 'det', 'imposto', 'ICMS', 'ICMS40', 'CST'),
                    }
                },
                'IPI': {
                    'cEnq': get_value(infNFe, 'det', 'imposto', 'IPI', 'cEnq'),
                    'IPINT': {
                        'CST': get_value(infNFe, 'det', 'imposto', 'IPI', 'IPINT', 'CST')
                    }
                },
                'PIS': {
                    'PISAliq': {
                        'CST': get_value(infNFe, 'det', 'imposto', 'PIS', 'PISAliq', 'CST'),
                        'vBC': get_value(infNFe, 'det', 'imposto', 'PIS', 'PISAliq', 'vBC'),
                        'pPIS': get_value(infNFe, 'det', 'imposto', 'PIS', 'PISAliq', 'pPIS'),
                        'vPIS': get_value(infNFe, 'det', 'imposto', 'PIS', 'PISAliq', 'vPIS'),
                    }
                },
                'COFINS': {
                    'COFINSAliq': {
                        'CST': get_value(infNFe, 'det', 'imposto', 'COFINS', 'COFINSAliq', 'CST'),
                        'vBC': get_value(infNFe, 'det', 'imposto', 'COFINS', 'COFINSAliq', 'vBC'),
                        'pCOFINS': get_value(infNFe, 'det', 'imposto', 'COFINS', 'COFINSAliq', 'pCOFINS'),
                        'vCOFINS': get_value(infNFe, 'det', 'imposto', 'COFINS', 'COFINSAliq', 'vCOFINS'),
                    }
                },
                'ICMSUFDest': {
                    'vBCUFDest': get_value(infNFe, 'det', 'imposto', 'ICMSUFDest', 'vBCUFDest'),
                    'vBCFCPUFDest': get_value(infNFe, 'det', 'imposto', 'ICMSUFDest', 'vBCFCPUFDest'),
                    'pFCPUFDest': get_value(infNFe, 'det', 'imposto', 'ICMSUFDest', 'pFCPUFDest'),
                    'pICMSUFDest': get_value(infNFe, 'det', 'imposto', 'ICMSUFDest', 'pICMSUFDest'),
                    'pICMSInter': get_value(infNFe, 'det', 'imposto', 'ICMSUFDest', 'pICMSInter'),
                    'pICMSInterPart': get_value(infNFe, 'det', 'imposto', 'ICMSUFDest', 'pICMSInterPart'),
                    'vFCPUFDest': get_value(infNFe, 'det', 'imposto', 'ICMSUFDest', 'vFCPUFDest'),
                    'vICMSUFDest': get_value(infNFe, 'det', 'imposto', 'ICMSUFDest', 'vICMSUFDest'),
                    'vICMSUFRemet': get_value(infNFe, 'det', 'imposto', 'ICMSUFDest', 'vICMSUFRemet'),
                }
            }
        },
        'total': {
            'ICMSTot': {
                'vBC': get_value(infNFe, 'total', 'ICMSTot', 'vBC'),
                'vICMS': get_value(infNFe, 'total', 'ICMSTot', 'vICMS'),
                'vICMSDeson': get_value(infNFe, 'total', 'ICMSTot', 'vICMSDeson'),
                'vFCP': get_value(infNFe, 'total', 'ICMSTot', 'vFCP'),
                'vBCST': get_value(infNFe, 'total', 'ICMSTot', 'vBCST'),
                'vST': get_value(infNFe, 'total', 'ICMSTot', 'vST'),
                'vFCPST': get_value(infNFe, 'total', 'ICMSTot', 'vFCPST'),
                'vFCPSTRet': get_value(infNFe, 'total', 'ICMSTot', 'vFCPSTRet'),
                'vProd': get_value(infNFe, 'total', 'ICMSTot', 'vProd'),
                'vFrete': get_value(infNFe, 'total', 'ICMSTot', 'vFrete'),
                'vSeg': get_value(infNFe, 'total', 'ICMSTot', 'vSeg'),
                'vDesc': get_value(infNFe, 'total', 'ICMSTot', 'vDesc'),
                'vII': get_value(infNFe, 'total', 'ICMSTot', 'vII'),
                'vIPI': get_value(infNFe, 'total', 'ICMSTot', 'vIPI'),
                'vIPIDevol': get_value(infNFe, 'total', 'ICMSTot', 'vIPIDevol'),
                'vPIS': get_value(infNFe, 'total', 'ICMSTot', 'vPIS'),
                'vCOFINS': get_value(infNFe, 'total', 'ICMSTot', 'vCOFINS'),
                'vOutro': get_value(infNFe, 'total', 'ICMSTot', 'vOutro'),
                'vNF': get_value(infNFe, 'total', 'ICMSTot', 'vNF'),
                'vTotTrib': get_value(infNFe, 'total', 'ICMSTot', 'vTotTrib'),
            }
        }
    }

    return transformed_data


def transform_prod(prod_list):
    transformmed_prod = []

    for prod in prod_list:
        transformmed_prod.append({
            'cProd': get_value(prod, 'cProd'),
            'cEAN': get_value(prod, 'cEAN'),
            'xProd': get_value(prod, 'xProd'),
            'NCM': get_value(prod, 'NCM'),
            'CEST': get_value(prod, 'CEST'),
            'CFOP': get_value(prod, 'CFOP'),
            'uCom': get_value(prod, 'uCom'),
            'qCom': get_value(prod, 'qCom'),
            'vUnCom': get_value(prod, 'vUnCom'),
            'vProd': get_value(prod, 'vProd'),
            'cEANTrib': get_value(prod, 'cEANTrib'),
            'uTrib': get_value(prod, 'uTrib'),
            'qTrib': get_value(prod, 'qTrib'),
            'vUnTrib': get_value(prod, 'vUnTrib'),
            'indTot': get_value(prod, 'indTot')
        })

    return transformmed_prod


def get_value(json_data, *path):
    """
    A função get_value recebe um objeto JSON (json_data) e uma sequência de argumentos (*path)
    que representam uma sequência de chaves para acessar um valor específico dentro desse JSON.
    :param json_data:
    :param path:
    :return:
    """
    current_key = ''
    try:
        result = json_data
        for key in path:
            current_key = key
            result = result[key]
        return result

    except (KeyError, TypeError):
        print(f"Warning ao tentar localizar chave do json {current_key}")
        return None
