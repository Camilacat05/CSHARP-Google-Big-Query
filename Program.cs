using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using System;

namespace Curso_big_query
{
    class Program
    {
        static void Main(string[] args)
        {

            CriarTabela();
        }

        public static void CriarTabela() {

            String conjuntoDados = "suco_vendas";
            String nomeTabela = "clientes_CSHARP";
            String projetoID = "gifted-antonym-391620";

            BigQueryClient client = BigQueryClient.Create(projetoID);
            BigQueryDataset dataset = client.GetDataset(conjuntoDados);
            var schema = new TableSchemaBuilder
            {
                {"CODIGO", BigQueryDbType.Int64},
                {"DESCRITOR", BigQueryDbType.String},
                {"DATA", BigQueryDbType.Date}
            }.Build();

            dataset.CreateTable(nomeTabela, schema);
            Console.WriteLine("TABELA CRIADA COM SUCESSO");
            Console.ReadLine();

        }
        public static void EnviarCSVBigQuery()
        {

            String conjuntoDados = "suco_vendas";
            String nomeTabela = "clientes_CSHARP";
            String projetoID = "gifted-antonym-391620";

            BigQueryClient cliente = BigQueryClient.Create(projetoID);
            var gcsURI = "gs://curso-big-query-storage/curso-alura.csv";
            BigQueryDataset dataset = cliente.GetDataset(conjuntoDados);
            TableSchema schema = new TableSchemaBuilder
            {
                { "CODIGO", BigQueryDbType.Int64 },
                { "DESCRITOR", BigQueryDbType.String},
                { "DATA", BigQueryDbType.Date},
                { "CASADO", BigQueryDbType.Bool}
            }.Build();

            var destinationTableRef = dataset.GetTableReference(nomeTabela); //aqui é o destino de onde vai ser colocado os dados
            var jobOptions = new CreateLoadJobOptions() //tá chamando e carregando o job de criação de tabela no google BigQuery
            {
                SourceFormat = FileFormat.Csv,//define o formato de consumo
                FieldDelimiter = ",", //delimita a separação da linhas e valores
                SkipLeadingRows = 1 // define que vai ignorar a primeira linha do arquivo csv
            };
            var loadJob = cliente.CreateLoadJob(gcsURI, destinationTableRef, schema, jobOptions); //criando o job com as configurações estabelecidas
            loadJob.PollUntilCompleted(); // nesse ponto apenas pontua que o job terminou de executar
            BigQueryTable table = cliente.GetTable(destinationTableRef); //acessando a tabela criada
            Console.WriteLine($"Carregada {table.Resource.NumRows} linhas na tabela {table.FullyQualifiedId}");
            Console.ReadLine();
        }
        public static void ApagarTabelaBigQuery() {
            
            String conjuntoDados = "suco_vendas";
            String nomeTabela = "clientes_CSHARP";
            String projetoID = "gifted-antonym-391620";

            BigQueryClient client = BigQueryClient.Create(projetoID);
            client.DeleteTable(conjuntoDados, nomeTabela);
            Console.WriteLine("TABELA DELETADA COM SUCESSO");
            Console.ReadLine();


        }


        static void programaByteBank003()
        {
            String conjuntoDados = "BYTEBANK_CSHARP";
            String projetoID = "curso-big-query-0965";
            BigQueryClient cliente = BigQueryClient.Create(projetoID);
            BigQueryDataset dataset = cliente.GetDataset(conjuntoDados);

            String nomeTabelaAgencia = "AGENCIAS";
            TableSchema schemaAgencia = new TableSchemaBuilder
              {
                  { "NUMERO_AGENCIA", BigQueryDbType.String },
                  { "NOME_AGENCIA", BigQueryDbType.String}
              }.Build();
            var destinationTableRefAgencia = dataset.GetTableReference(nomeTabelaAgencia);
            var gcsURIAgencia = "gs://curso-big-query-0965/Exercicios/Agencias.csv";

            String nomeTabelaCliente = "CLIENTES";
            TableSchema schemaCliente = new TableSchemaBuilder
              {
                  { "CPF", BigQueryDbType.String },
                  { "NOME_CLIENTE", BigQueryDbType.String}
              }.Build();
            var destinationTableRefCliente = dataset.GetTableReference(nomeTabelaCliente);
            var gcsURICliente = "gs://curso-big-query-0965/Exercicios/Clientes.csv";

            String nomeTabelaContaCorrente = "CONTAS_CORRENTE";
            TableSchema schemaContaCorrente = new TableSchemaBuilder
              {
                  { "NUMERO_CONTA", BigQueryDbType.String },
                  { "NUMERO_AGENCIA", BigQueryDbType.String },
                  { "CPF", BigQueryDbType.String },
                  { "TIPO_CONTA", BigQueryDbType.Int64 }
              }.Build();
            var destinationTableRefContaCorrente = dataset.GetTableReference(nomeTabelaContaCorrente);
            var gcsURIContaCorrente = "gs://curso-big-query-0965/Exercicios/ContasCorrente.csv";

            var jobOptions = new CreateLoadJobOptions()
            {
                SourceFormat = FileFormat.Csv,
                FieldDelimiter = ",",
                SkipLeadingRows = 1
            };

            var loadJobAgencia = cliente.CreateLoadJob(gcsURIAgencia, destinationTableRefAgencia, schemaAgencia, jobOptions);
            loadJobAgencia.PollUntilCompleted();
            var loadJobCliente = cliente.CreateLoadJob(gcsURICliente, destinationTableRefCliente, schemaCliente, jobOptions);
            loadJobCliente.PollUntilCompleted();
            var loadJobContaCorrente = cliente.CreateLoadJob(gcsURIContaCorrente, destinationTableRefContaCorrente, schemaContaCorrente, jobOptions);
            loadJobContaCorrente.PollUntilCompleted();

            Console.WriteLine("Tabelas carregadas com sucesso");
            Console.ReadLine();
        }
    }

    
}



