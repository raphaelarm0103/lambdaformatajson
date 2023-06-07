import unittest
from unittest.mock import MagicMock, patch
from lambda_function import lambda_handler


class TestLambdaHandler(unittest.TestCase):
    def test_lambda_handler(self):
        event = {
            'Records': [
                {
                    's3': {
                        'object': {
                            'key': 'example.json'
                        }
                    }
                }
            ]
        }
        context = MagicMock()

        mock_s3_client = MagicMock()
        mock_s3_client.get_object.return_value = {
            'Body': MagicMock(return_value=bytes('{"nfeProc": {"protNFe": {"infProt": {"chNFe": "12345"}}}}', 'utf-8'))
        }
        mock_s3_client.put_object.return_value = {}

        with patch('boto3.client', return_value=mock_s3_client):
            response = lambda_handler(event, context)

        self.assertEqual(response['statusCode'], 200)
        self.assertEqual(response['body'], 'Transformação do JSON concluída com sucesso!')
        mock_s3_client.get_object.assert_called_with(Bucket='insira-bucket-entrada', Key='example.json')
        mock_s3_client.put_object.assert_called_with(
            Body='{"cliente": null, "idNfe": "12345", "dhEmi": null, "dhSaiEnt": null, "natOp": null, "emit": {"CNPJ": '
                 'null, "xNome": null, "xFant": null}, "dest": {"CPF": null, "xNome": null, "email": null}, "det": '
                 '{"prod": [], "imposto": {"vTotTrib": null, "ICMS": {"ICMS40": {"orig": null, "CST": null}}}, "IPI": '
                 '{"cEnq": null, "IPINT": {"CST": null}}}, "total": {"ICMSTot": {"vBC": null, "vICMS": null, '
                 '"vICMSDeson": null, "vFCP": null, "vBCST": null, "vST": null, "vFCPST": null, "vFCPSTRet": null, '
                 '"vProd": null, "vFrete": null, "vSeg": null, "vDesc": null, "vII": null, "vIPI": null, "vIPIDevol": '
                 'null, "vPIS": null, "vCOFINS": null, "vOutro": null, "vNF": null, "vTotTrib": null}}}}',
            Bucket='insira-bucket-saida', Key='Formatado_12345.json')


if __name__ == '__main__':
    unittest.main()