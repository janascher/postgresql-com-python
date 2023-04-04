import psycopg2
from config import config


def connect():
    """ 
        Conecta ao servidor de banco de dados PostgreSQL em Python.
    """
    conn = None
    try:
        # lê parâmetros de conexão
        params = config()
        # conecta ao servidor PostgreSQL
        print("Conectando ao banco de dados PostgreSQL...")
        conn = psycopg2.connect(**params)
        print("Conexão bem-sucedida!")

        # Cria o cursor
        cur = conn.cursor()

        # Executa o comando SQL para Criar Tabela
        cur.execute(
            """
            CREATE TABLE Employee (
                ID INT PRIMARY KEY NOT NULL, 
                NAME TEXT NOT NULL, 
                EMAIL TEXT NOT NULL
                )
        """)

        # Confirma a operação
        conn.commit()
        print("Tabela criada com sucesso!")
        # fecha a comunicação com o PostgreSQL
        # conn.close()
        #print("Conexão com o banco de dados fechada.")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


if __name__ == "__main__":
    connect()
