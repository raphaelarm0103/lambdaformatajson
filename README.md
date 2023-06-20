# lambdaformatajson

Esse é um exemplo de uma função Lambda do AWS (Amazon Web Services) escrita em Python, que utiliza a biblioteca Boto3 para interagir com o serviço S3 (Simple Storage Service). Vou explicar o funcionamento do código linha por linha:

As bibliotecas necessárias, boto3 (para interagir com o AWS SDK) e json (para trabalhar com JSON), são importadas.

A função lambda_handler é definida como a função principal do Lambda. Essa função será chamada quando o Lambda for invocado.

A função lambda_handler recebe dois parâmetros: event e context. O parâmetro event contém informações sobre o evento que acionou o Lambda, como dados de entrada. O parâmetro context fornece informações de contexto e funcionalidades adicionais.

O evento recebido é impresso na saída usando a função print.

São definidas algumas variáveis, como source_bucket (nome do bucket de entrada), source_key (chave do objeto do S3 recebido no evento), destination_bucket_compra (nome do bucket de destino para os dados formatados de compra) e destination_bucket_cancelamento (nome do bucket de destino para os dados formatados de cancelamento).

O cliente do S3 é inicializado usando a função boto3.client.

É feita uma tentativa de ler o arquivo JSON de origem do S3 usando a função s3_client.get_object. O conteúdo JSON é convertido em uma string usando a função read().decode('utf-8').

A string JSON é convertida de volta para um objeto Python usando a função json.loads, e o resultado é armazenado na variável data.

Se o valor da chave 'cliente' existir em data, é realizada uma transformação nos dados para um novo formato. Caso contrário, é realizado um processamento diferente para cancelamentos.

Os dados transformados são convertidos de volta para uma string JSON usando a função json.dumps. Um nome de arquivo de destino é definido com base nas informações do objeto original.

O novo arquivo JSON é salvo no S3 usando a função s3_client.put_object, especificando o corpo do objeto, o bucket de destino e a chave do objeto.

A função retorna um dicionário contendo um status code 200 e uma mensagem de sucesso.

Se ocorrer uma exceção durante o processamento, a mensagem de erro é impressa e a função retorna um dicionário com um status code 500 e a mensagem de erro.

A função transform_json é definida para realizar a transformação nos dados JSON. Ela recebe o objeto data como entrada e retorna um novo objeto com os dados transformados.

A função transform_prod é definida para transformar a lista de produtos dentro dos dados. Ela recebe a lista de produtos prod_list como entrada e retorna uma nova lista de produtos transformados.

A função get_value é definida para obter um valor específico de um objeto JSON com base em uma sequência de chaves. Ela recebe o objeto JSON json_data e uma sequência de chaves *path. A função percorre as chaves no objeto JSON e retorna o valor correspondente. Se a chave não existir ou ocorrer um erro, a função retorna None e imprime um aviso.
