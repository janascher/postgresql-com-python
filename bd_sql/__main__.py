"""
    import configparser: Importa o módulo configparser, que fornece classes para trabalhar com arquivos de configuração em estilo INI (abreviação de "Initialization").

    import subprocess: Importa o módulo subprocess, que permite a criação de novos processos, a conexão com seus pipes de entrada/saída/erro e a obtenção de seus códigos de retorno.

    import time: Importa o módulo time, que fornece várias funções relacionadas ao tempo, como medição de tempo e pausas.

    import psycopg2: Importa o módulo psycopg2, que é um adaptador de banco de dados PostgreSQL para Python.

    from configs import config: Importa a função config do módulo configs. Isso provavelmente é uma função personalizada definida em um arquivo chamado configs.py.
"""
import configparser
import subprocess
import time

import psycopg2
from configs import config


class User:
    def __init__(self, id=None, name=None, email=None):
        """
            Inicializa a classe User.
        """
        self.id = id
        self.name = name
        self.email = email

    def connect(self):
        """ 
            Conecta ao servidor de banco de dados PostgreSQL.
        """
        conn = None
        try:
            params = config()
            # print("Conectando ao banco de dados PostgreSQL...")
            conn = psycopg2.connect(**params)
            # print("Conexão bem-sucedida!")
            return conn
            # conn.close()
            #print("Conexão com o banco de dados fechada.")
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def create_table(self):
        """
            Cria a Tabela Users no banco de dados PostgreSQL. Também cria um índice no campo "name".
        """
        conn = self.connect()
        cur = conn.cursor()

        try:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY NOT NULL,
                    name VARCHAR(100) NOT NULL,
                    email VARCHAR(100) NOT NULL
                );
            """)

            # Criação do índice no campo "name"
            cur.execute("""
                CREATE INDEX idx_users_name ON users (name);
            """)

            conn.commit()
            print("Tabela criada com sucesso!")
            # cur.close()
            # conn.close()
        except (Exception, psycopg2.DatabaseError) as error:
            conn.rollback()
            print(f"ERRO: {error}")
        finally:
            conn.close()

    def insert_from_csv(self, csv_file):
        """
            Insere registros em massa no banco de dados PostgreSQL a partir de um arquivo CSV. 
            Usa o módulo subprocess para executar um comando psql para realizar a inserção.
        """
        conn = self.connect()

        # Cria uma instância do objeto configparser
        config = configparser.ConfigParser()

        # Lê o arquivo database.ini
        config.read('database.ini') 

        # Obtém o nome do banco de dados da seção correta no arquivo "database.ini"
        dbname = config.get('bdsql', 'database')
        dbuser = config.get('bdsql', 'user')

        try:            
            subprocess.run(
                f'sudo -u {dbuser} psql -d {dbname} -c "\\copy users(id, name, email) FROM \'{csv_file}\' DELIMITER \',\' CSV HEADER"',
                shell=True,
            )
            print("Registros inseridos com sucesso!")
        except Exception as error:
            print(f"ERRO: {error}")
        finally:
         if conn is not None:
            conn.close()

    def perform_query(self, query, param):
        """
            Executa uma consulta SQL.
            Mede o tempo de execução da consulta e retorna o resultado e o tempo decorrido.
        """
        conn = self.connect()
        cur = conn.cursor()

        start_time = time.time()
        cur.execute(query, (param,))
        result = cur.fetchall()
        end_time = time.time()

        elapsed_time = end_time - start_time

        conn.close()

        return result, elapsed_time
    
    def query_by_name(self, name):
        """
            Executa uma consulta para obter registros da tabela "users", cujo nome corresponda a um padrão fornecido como parâmetro.
        """
        query = "SELECT * FROM users WHERE name LIKE %s"
        return self.perform_query(query, name)

    def query_exact_name(self, name):
        """
            Executa uma consulta para obter registros da tabela "users", cujo nome seja exatamente igual ao valor fornecido como parâmetro.
        """
        query = "SELECT * FROM users WHERE name = %s"
        return self.perform_query(query, name)

    def query_by_email(self, email):
        """
            Executa uma consulta para obter registros da tabela "users", cujo email seja exatamente igual ao valor fornecido como parâmetro.
        """
        query = "SELECT * FROM users WHERE email = %s"
        return self.perform_query(query, email)

    def populate_table(self, num_rows):
        """
            Preenche a tabela "users" com um número específico de registros gerados aleatoriamente. 
            Remove todos os registros existentes antes de inserir novos dados.
        """
        conn = self.connect()
        cur = conn.cursor()

        try:
            cur.execute("DELETE FROM users;")  # Limpar a tabela antes de inserir novos dados

            # Gerar dados aleatórios para inserção
            data = []
            for i in range(num_rows):
                data.append((f"User {i}", f"user{i}@example.com"))

            # Inserir os dados na tabela
            cur.executemany("INSERT INTO users (name, email) VALUES (%s, %s);", data)

            conn.commit()
            print(f"Inseridos {num_rows} registros na tabela.")
        except (Exception, psycopg2.DatabaseError) as error:
            conn.rollback()
            print(f"ERRO: {error}")
        finally:
            conn.close()

    def query_performance_test(self):
        """
            Realiza um teste de desempenho das consultas em diferentes tamanhos de tabela.
            Gera uma tabela com um número específico de registros e executa consultas para medir o tempo de execução.
            O teste é realizado para tamanhos de tabela predefinidos e para diferentes tipos de consulta.
        """
        num_rows_list = [1000, 10000, 100000, 1000000]
        queries = [
            ("A", "User 500"),
            ("B", "User 500"),
            ("C", "user500@example.com")
        ]

        for num_rows in num_rows_list:
            self.populate_table(num_rows)

            print(f"Resultados para {num_rows} registros:")
            print("Consulta\t\tTempo de Execução (s)")
            print("---------------------------------------")
            for query_type, param in queries:
                elapsed_time = None, None
                if query_type == "A":
                    _, elapsed_time = self.query_by_name(param) # "_" é uma convenção utilizada em Python para indicar uma variável que não será utilizada posteriormente.
                elif query_type == "B":
                    _, elapsed_time = self.query_exact_name(param)
                elif query_type == "C":
                    _, elapsed_time = self.query_by_email(param)

                print(f"{query_type}\t\t{elapsed_time:.4f} s")
            print("\n")

user = User()

"""
    Estabelece a conexão com o banco de dados
"""
# user.connect()


"""
    Cria uma tabela user
"""
# user.create_table()

"""
    Insere os dados do arquivo CSV no PostgresSQL
"""
# csv_file = 'bd_sql/data/raw/user_data.csv'
# User.insert_from_csv(csv_file)

"""
    Realiza o teste de desempenho das consultas
"""
user.query_performance_test()

"""
Saída do teste de desempenho das consultas:

Inseridos 1000 registros na tabela.
Resultados para 1000 registros:
Consulta                Tempo de Execução (s)
---------------------------------------
A               0.0008 s
B               0.0009 s
C               0.1041 s


Inseridos 10000 registros na tabela.
Resultados para 10000 registros:
Consulta                Tempo de Execução (s)
---------------------------------------
A               0.0061 s
B               0.0010 s
C               0.0280 s


Inseridos 100000 registros na tabela.
Resultados para 100000 registros:
Consulta                Tempo de Execução (s)
---------------------------------------
A               0.0027 s
B               0.0015 s
C               0.0515 s


Inseridos 1000000 registros na tabela.
Resultados para 1000000 registros:
Consulta                Tempo de Execução (s)
---------------------------------------
A               0.0050 s
B               0.0028 s
C               0.1948 s
"""